"""Tests for eval_in_clamped_power_space."""

import numpy as np
import pytest
from pv_prospect.model.evaluation import clamped_power_pred, eval_in_clamped_power_space


def test_clamping_caps_power_at_inverter_capacity() -> None:
    """Power predictions above inverter capacity are clamped."""
    cf_pred = np.array([0.5])
    panel = np.array([6000.0])
    inverter = np.array([4000.0])
    result = clamped_power_pred(cf_pred, panel, inverter)
    # 0.5 * 6000 = 3000 → 3000 < 4000 → not clamped
    assert result[0] == pytest.approx(3000.0)


def test_clamping_below_inverter_capacity_is_not_clamped() -> None:
    """Predictions that exceed the inverter capacity are clamped to inverter."""
    cf_pred = np.array([1.0])
    panel = np.array([6000.0])
    inverter = np.array([4000.0])
    result = clamped_power_pred(cf_pred, panel, inverter)
    # 1.0 * 6000 = 6000 → clamped to 4000
    assert result[0] == pytest.approx(4000.0)


def test_eval_in_clamped_power_space_compares_against_power_true() -> None:
    """Metrics compare clamped predictions against raw power targets."""
    cf_pred = np.array([0.1, 0.2])
    power_true = np.array([600.0, 1200.0])
    panel = np.array([6000.0, 6000.0])
    inverter = np.array([5000.0, 5000.0])
    metrics = eval_in_clamped_power_space(cf_pred, power_true, panel, inverter)
    # 0.1 * 6000 = 600, 0.2 * 6000 = 1200 — perfect predictions
    assert metrics.r2 == pytest.approx(1.0)
    assert metrics.rmse == pytest.approx(0.0, abs=1e-8)
