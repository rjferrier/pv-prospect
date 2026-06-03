"""Evaluation metrics for the PV and weather models."""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from pv_prospect.model.domain import (
    EvalReport,
    PerSiteMetrics,
    SplitMetrics,
    WeatherEvalReport,
    WeatherTargetMetrics,
)


def eval_in_f_space(y_true: np.ndarray, y_pred: np.ndarray) -> SplitMetrics:
    """Compute regression metrics comparing predicted and actual capacity factor."""
    mse = float(mean_squared_error(y_true, y_pred))
    return SplitMetrics(
        r2=float(r2_score(y_true, y_pred)),
        rmse=float(np.sqrt(mse)),
        mae=float(mean_absolute_error(y_true, y_pred)),
        mse=mse,
    )


def clamped_power_pred(
    cf_pred: np.ndarray,
    panel_capacity: np.ndarray,
    inverter_capacity: np.ndarray,
) -> np.ndarray:
    """Return end-user power prediction: ``min(cf_pred * panel, inverter)``.

    The model predicts capacity factor; the product delivers power. The
    inverter clamp is what makes the two equivalent on heavily over-paneled
    sites (where summer daily-mean power sits at the inverter cap).
    """
    return np.minimum(cf_pred * panel_capacity, inverter_capacity)


def eval_in_clamped_power_space(
    cf_pred: np.ndarray,
    power_true: np.ndarray,
    panel_capacity: np.ndarray,
    inverter_capacity: np.ndarray,
) -> SplitMetrics:
    """Compute metrics after converting capacity-factor predictions to power.

    This is the honest end-user error: what the product actually delivers
    versus what was measured.
    """
    power_pred = clamped_power_pred(cf_pred, panel_capacity, inverter_capacity)
    return eval_in_f_space(power_true, power_pred)


def eval_per_site(
    system_ids: np.ndarray,
    y_true: np.ndarray,
    y_pred: np.ndarray,
) -> tuple[PerSiteMetrics, ...]:
    """Return per-site metrics sorted by system_id."""
    rows = []
    for sid in sorted(np.unique(system_ids)):
        mask = system_ids == sid
        if mask.sum() == 0:
            continue
        rows.append(
            PerSiteMetrics(
                system_id=int(sid),
                n=int(mask.sum()),
                r2=float(r2_score(y_true[mask], y_pred[mask])),
                rmse=float(np.sqrt(mean_squared_error(y_true[mask], y_pred[mask]))),
                mae=float(mean_absolute_error(y_true[mask], y_pred[mask])),
            )
        )
    return tuple(rows)


def build_eval_report(
    train_target: np.ndarray,
    test_target: np.ndarray,
    train_pred_cf: np.ndarray,
    test_pred_cf: np.ndarray,
    train_power: np.ndarray,
    test_power: np.ndarray,
    train_panel_capacity: np.ndarray,
    test_panel_capacity: np.ndarray,
    train_inverter_capacity: np.ndarray,
    test_inverter_capacity: np.ndarray,
    train_system_ids: np.ndarray,
    test_system_ids: np.ndarray,
    cutoff: str,
) -> EvalReport:
    """Assemble a full EvalReport from raw prediction arrays."""
    return EvalReport(
        train_f_space=eval_in_f_space(train_target, train_pred_cf),
        test_f_space=eval_in_f_space(test_target, test_pred_cf),
        train_power_space=eval_in_clamped_power_space(
            train_pred_cf, train_power, train_panel_capacity, train_inverter_capacity
        ),
        test_power_space=eval_in_clamped_power_space(
            test_pred_cf, test_power, test_panel_capacity, test_inverter_capacity
        ),
        test_per_site_f_space=eval_per_site(test_system_ids, test_target, test_pred_cf),
        test_per_site_power_space=eval_per_site(
            test_system_ids,
            test_power,
            clamped_power_pred(
                test_pred_cf, test_panel_capacity, test_inverter_capacity
            ),
        ),
        cutoff=cutoff,
    )


# ---------------------------------------------------------------------------
# Weather model evaluation
# ---------------------------------------------------------------------------

_EARTH_RADIUS_KM = 6371.0
_BLOCK_DEG = 0.16


def assign_coarse_blocks(
    df: pd.DataFrame, block_deg: float = _BLOCK_DEG
) -> pd.DataFrame:
    """Add ``block_row`` and ``block_col`` integer identifiers to each row.

    Each block is a ``block_deg`` × ``block_deg`` tile in (lat, lon) space.
    Floor-division on raw coordinates is grid-origin-agnostic and correct
    even when the input data is snapped to a provider's own internal grid
    (e.g. OpenMeteo) rather than to the uk-geo lattice.
    """
    out = df.copy()
    out['block_row'] = np.floor(df['latitude'] / block_deg).astype(int)
    out['block_col'] = np.floor(df['longitude'] / block_deg).astype(int)
    return out


def block_climatology(df: pd.DataFrame, value_columns: list[str]) -> pd.DataFrame:
    """Aggregate rows to ``(block_row, block_col, month_of_year)`` mean values.

    Also computes centroid lat/lon for each block so IDW distances can be
    calculated without re-reading the raw rows.
    """
    out = df.copy()
    out['month_of_year'] = out['time'].dt.month
    grp = out.groupby(['block_row', 'block_col', 'month_of_year'])
    agg = grp[value_columns].mean()
    centroids = grp[['latitude', 'longitude']].mean()
    centroids.columns = ['centroid_lat', 'centroid_lon']
    return pd.concat([agg, centroids], axis=1).reset_index()


def _haversine_km(
    lat1: np.ndarray,
    lon1: np.ndarray,
    lat2: np.ndarray,
    lon2: np.ndarray,
) -> np.ndarray:
    """Vectorised haversine distance in km (broadcasting supported)."""
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    return 2.0 * _EARTH_RADIUS_KM * np.arcsin(np.clip(np.sqrt(a), 0.0, 1.0))


def idw_predictions(
    test_clim: pd.DataFrame,
    train_clim: pd.DataFrame,
    targets: list[str],
    power: float = 2.0,
) -> pd.DataFrame:
    """Inverse-distance-weighted predictions for each test block, per month.

    For each (test block, month) pair, weights training block climatologies
    by 1/distance² (IDW) and produces a weighted average prediction. This is
    the natural "dumb" spatial baseline: if the model can't beat it, the
    learned features add no value beyond distance-weighted interpolation of
    known sites.

    ``test_clim`` and ``train_clim`` must both have columns:
    ``block_row``, ``block_col``, ``month_of_year``, ``centroid_lat``,
    ``centroid_lon``, and all columns in ``targets``.
    """
    result_rows = []
    for month in sorted(test_clim['month_of_year'].unique()):
        test_month = test_clim[test_clim['month_of_year'] == month].reset_index(
            drop=True
        )
        train_month = train_clim[train_clim['month_of_year'] == month].reset_index(
            drop=True
        )
        if train_month.empty:
            continue

        test_lat = test_month['centroid_lat'].values[:, None]
        test_lon = test_month['centroid_lon'].values[:, None]
        train_lat = train_month['centroid_lat'].values[None, :]
        train_lon = train_month['centroid_lon'].values[None, :]

        dist = _haversine_km(test_lat, test_lon, train_lat, train_lon)
        dist = np.maximum(dist, 1e-6)
        weights = 1.0 / dist**power
        weights = weights / weights.sum(axis=1, keepdims=True)

        row = {
            'block_row': test_month['block_row'].values,
            'block_col': test_month['block_col'].values,
            'month_of_year': month,
        }
        for t in targets:
            row[t] = (weights * train_month[t].values[None, :]).sum(axis=1)
        result_rows.append(pd.DataFrame(row))

    return pd.concat(result_rows, ignore_index=True) if result_rows else pd.DataFrame()


def evaluate_block_climatology(
    pred_clim: pd.DataFrame,
    obs_clim: pd.DataFrame,
    targets: list[str],
) -> tuple[WeatherTargetMetrics, ...]:
    """Compare predicted and observed block climatologies.

    Inner-joins on ``(block_row, block_col, month_of_year)`` so only blocks
    present in both frames are scored. Returns per-target metrics.
    """
    merged = pred_clim.merge(
        obs_clim,
        on=['block_row', 'block_col', 'month_of_year'],
        suffixes=('_pred', '_obs'),
    )
    metrics = []
    for t in targets:
        y_pred = merged[f'{t}_pred'].values
        y_obs = merged[f'{t}_obs'].values
        mse = float(mean_squared_error(y_obs, y_pred))
        metrics.append(
            WeatherTargetMetrics(
                target=t,
                r2=float(r2_score(y_obs, y_pred)),
                rmse=float(np.sqrt(mse)),
                mae=float(mean_absolute_error(y_obs, y_pred)),
                bias=float(np.mean(y_pred - y_obs)),
            )
        )
    return tuple(metrics)


def build_weather_eval_report(
    train_targets_raw: np.ndarray,
    test_targets_raw: np.ndarray,
    train_pred_raw: np.ndarray,
    test_pred_raw: np.ndarray,
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    target_columns: list[str],
    cutoff: str,
) -> WeatherEvalReport:
    """Assemble a WeatherEvalReport from raw prediction arrays.

    Computes temporal hold-out metrics (per-row, per-target) plus
    block-climatology metrics on the test set, with IDW as the baseline.
    """
    temporal_test = tuple(
        WeatherTargetMetrics(
            target=t,
            r2=float(r2_score(test_targets_raw[:, i], test_pred_raw[:, i])),
            rmse=float(
                np.sqrt(mean_squared_error(test_targets_raw[:, i], test_pred_raw[:, i]))
            ),
            mae=float(mean_absolute_error(test_targets_raw[:, i], test_pred_raw[:, i])),
            bias=float(np.mean(test_pred_raw[:, i] - test_targets_raw[:, i])),
        )
        for i, t in enumerate(target_columns)
    )

    train_with_pred = assign_coarse_blocks(train_df.copy())
    test_with_pred = assign_coarse_blocks(test_df.copy())
    for i, t in enumerate(target_columns):
        test_with_pred[f'{t}_pred'] = test_pred_raw[:, i]

    train_clim = block_climatology(train_with_pred, target_columns)
    obs_clim = block_climatology(test_with_pred, target_columns)
    pred_clim_df = test_with_pred.copy()
    pred_clim_df[target_columns] = test_with_pred[
        [f'{t}_pred' for t in target_columns]
    ].rename(columns={f'{t}_pred': t for t in target_columns})
    model_clim = block_climatology(pred_clim_df, target_columns)

    idw_clim = idw_predictions(obs_clim, train_clim, target_columns)

    block_clim_model = evaluate_block_climatology(model_clim, obs_clim, target_columns)
    block_clim_idw = evaluate_block_climatology(idw_clim, obs_clim, target_columns)

    return WeatherEvalReport(
        temporal_test=temporal_test,
        block_clim_model=block_clim_model,
        block_clim_idw=block_clim_idw,
        cutoff=cutoff,
    )
