"""Tests for loso_site_metrics."""

import numpy as np
import pytest
from pv_prospect.model.evaluation import loso_site_metrics


def test_level_ratio_is_mean_actual_over_mean_predicted_cf() -> None:
    """level_ratio is computed in CF space: mean(actual) / mean(predicted)."""
    actual_cf = np.array([0.2, 0.4])  # mean 0.3
    pred_cf = np.array([0.1, 0.3])  # mean 0.2
    panel = np.array([5000.0, 5000.0])
    inverter = np.array([5000.0, 5000.0])
    metrics = loso_site_metrics(
        system_id=42,
        actual_cf=actual_cf,
        pred_cf=pred_cf,
        power_true=actual_cf * panel,
        panel_capacity=panel,
        inverter_capacity=inverter,
    )
    assert metrics.system_id == 42
    assert metrics.n == 2
    assert metrics.level_ratio == pytest.approx(1.5)


def test_power_r2_is_perfect_when_clamped_power_matches() -> None:
    """power_r2 scores clamped-power predictions against power_true."""
    pred_cf = np.array([0.1, 0.2])
    panel = np.array([6000.0, 6000.0])
    inverter = np.array([5000.0, 5000.0])
    metrics = loso_site_metrics(
        system_id=1,
        actual_cf=pred_cf,
        pred_cf=pred_cf,
        power_true=np.array([600.0, 1200.0]),
        panel_capacity=panel,
        inverter_capacity=inverter,
    )
    assert metrics.power_r2 == pytest.approx(1.0)


def test_power_mape_excludes_nonpositive_actual_rows() -> None:
    """Winter near-zero (here exactly zero) actuals are dropped from MAPE."""
    pred_cf = np.array([0.05, 0.09])
    panel = np.array([10000.0, 10000.0])
    inverter = np.array([1e9, 1e9])  # no clamp → power_pred = [500, 900]
    metrics = loso_site_metrics(
        system_id=1,
        actual_cf=pred_cf,
        pred_cf=pred_cf,
        power_true=np.array([0.0, 1000.0]),
        panel_capacity=panel,
        inverter_capacity=inverter,
    )
    # only the second row counts: |1000 - 900| / 1000 = 0.1
    assert metrics.power_mape == pytest.approx(0.1)
