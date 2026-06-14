"""Pure validation helpers and orchestrator for /validate routes.

Feeds the loaded 90-day window's real weather (baked temperature +
plane_of_array_irradiance) through the deployed PV model and compares to
real power, per day.  Never runs the weather model or calculate_POA — this
isolates PV-model error from weather-model error and sidesteps the
weather-path vintage bias.

predicted_cf is the model's raw capacity-factor output (pre-clamp).
predicted_kwh is post-inverter-clamp delivered power in kWh/day.
On clipped days predicted_cf * panels_capacity * 24/1000 > predicted_kwh;
the gap is headroom lost to the inverter — informative, not a bug.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd
from pv_prospect.app.chain import run_pv_model
from pv_prospect.common.domain.pv_site import PVSite
from pv_prospect.model import eval_in_f_space
from pv_prospect.model.domain import ModelArtifact
from pv_prospect.model.features import (
    AGE_COLUMN,
    BINARY_FEATURES,
    CONTINUOUS_FEATURES,
    compute_age_years,
    is_clipped,
)

_MAPE_DEFAULT_FLOOR_KWH = 0.5


@dataclass
class SeriesPoint:
    date: date
    predicted_kwh: float
    actual_kwh: float
    predicted_cf: float
    actual_cf: float
    clipped: bool


@dataclass
class ErrorMetrics:
    mape: float | None
    power_space_r2: float | None


@dataclass
class SiteValidation:
    series: list[SeriesPoint]
    error: ErrorMetrics


def window_age_fill(
    windows: dict[int, pd.DataFrame],
    install_dates: dict[int, date | None],
) -> float:
    """Return the median age_years across all window rows with a known install date.

    Used as fill_with for date-less sites so they receive the same kind of
    global-median imputation that training used across the full corpus.
    Returns 0.0 if no site in the window has a known installation date.
    """
    known_ages: list[pd.Series] = []
    for system_id, df in windows.items():
        install_date = install_dates.get(system_id)
        if install_date is None:
            continue
        install_series = pd.Series(
            [pd.Timestamp(install_date)] * len(df), index=df.index
        )
        age_years, _ = compute_age_years(df['time'], install_series)
        known_ages.append(age_years)
    if not known_ages:
        return 0.0
    all_ages = pd.concat(known_ages, ignore_index=True)
    return float(all_ages.median())


def assemble_features(
    window_df: pd.DataFrame,
    pv_site: PVSite,
    age_fill: float,
) -> pd.DataFrame:
    """Build the feature DataFrame from a per-site window frame.

    Output contains CONTINUOUS_FEATURES + BINARY_FEATURES columns, the
    structural ``age_years`` column (routed to the degradation factor), plus aux
    columns power, power_max, and inverter_capacity needed for clipping and
    actuals computation in validate_site.
    """
    df = window_df.copy()
    df['day_of_year'] = df['time'].dt.dayofyear

    if pv_site.installation_date is not None:
        install_series = pd.Series(
            [pd.Timestamp(pv_site.installation_date)] * len(df), index=df.index
        )
    else:
        install_series = pd.Series([pd.NaT] * len(df), index=df.index)

    df['age_years'], df['age_known'] = compute_age_years(
        df['time'], install_series, fill_with=age_fill
    )
    df['inverter_capacity'] = pv_site.inverter_system.capacity
    return df


def compute_metrics(
    predicted_kwh: pd.Series,
    actual_kwh: pd.Series,
    clipped: pd.Series,
    floor: float = _MAPE_DEFAULT_FLOOR_KWH,
) -> ErrorMetrics:
    """Compute power-space R² and MAPE, excluding clipped rows from both.

    MAPE is additionally restricted to days where actual_kwh >= floor to
    avoid exploding on near-zero-output days.  Returns None for each metric
    when there are insufficient rows (degenerate window).
    """
    unclipped = ~clipped
    pred_kept = predicted_kwh[unclipped].values
    actual_kept = actual_kwh[unclipped].values

    if len(actual_kept) >= 2:
        power_space_r2: float | None = eval_in_f_space(actual_kept, pred_kept).r2
    else:
        power_space_r2 = None

    above_floor = unclipped & (actual_kwh >= floor)
    actual_above = actual_kwh[above_floor].values
    pred_above = predicted_kwh[above_floor].values

    if len(actual_above) > 0:
        mape: float | None = float(
            np.mean(np.abs((actual_above - pred_above) / actual_above))
        )
    else:
        mape = None

    return ErrorMetrics(mape=mape, power_space_r2=power_space_r2)


def validate_site(
    window_df: pd.DataFrame,
    pv_site: PVSite,
    pv_artifact: ModelArtifact,
    age_fill: float,
    floor: float = _MAPE_DEFAULT_FLOOR_KWH,
) -> SiteValidation:
    """Orchestrate PV model inference over a validation window for one site.

    Not unit-tested (impure: calls the model forward pass); covered by the
    Task-4 smoke test.
    """
    feature_df = assemble_features(window_df, pv_site, age_fill)
    panels_cap = np.full(len(feature_df), float(pv_site.panel_system.capacity))
    inverter_cap = np.full(len(feature_df), float(pv_site.inverter_system.capacity))

    pv_result = run_pv_model(
        pv_artifact,
        feature_df[CONTINUOUS_FEATURES + BINARY_FEATURES + [AGE_COLUMN]],
        panels_cap,
        inverter_cap,
    )

    idx = feature_df.index
    predicted_kwh = pd.Series(pv_result.clamped_power_w * 24 / 1000, index=idx)
    actual_kwh = feature_df['power'] * 24 / 1000
    predicted_cf = pd.Series(pv_result.capacity_factor, index=idx)
    actual_cf = feature_df['power'] / pv_site.panel_system.capacity
    clipped = is_clipped(feature_df)

    series = [
        SeriesPoint(
            date=feature_df['time'].iloc[i].date(),
            predicted_kwh=float(predicted_kwh.iloc[i]),
            actual_kwh=float(actual_kwh.iloc[i]),
            predicted_cf=float(predicted_cf.iloc[i]),
            actual_cf=float(actual_cf.iloc[i]),
            clipped=bool(clipped.iloc[i]),
        )
        for i in range(len(feature_df))
    ]

    error = compute_metrics(predicted_kwh, actual_kwh, clipped, floor)
    return SiteValidation(series=series, error=error)
