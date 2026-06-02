"""Evaluation metrics in capacity-factor space and clamped-power space."""

from __future__ import annotations

import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from pv_prospect.model.domain import EvalReport, PerSiteMetrics, SplitMetrics


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
