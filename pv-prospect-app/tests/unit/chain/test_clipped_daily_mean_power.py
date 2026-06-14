"""Tests for clipped_daily_mean_power in chain.py."""

from __future__ import annotations

import numpy as np
from pv_prospect.app.chain import clipped_daily_mean_power


def _flat_shape(n: int = 24) -> np.ndarray:
    """Uniform shape (mean == 1), no intraday variation."""
    return np.ones(n)


def _peaked_shape(n: int = 24) -> np.ndarray:
    """Shape with zeros for first half, twos for second (mean == 1)."""
    shape = np.zeros(n)
    shape[n // 2 :] = 2.0
    return shape


def test_backward_compat_flat_shape_unconstrained_inverter() -> None:
    """With a flat shape and inverter >= panel, result == cf * panel."""
    cf = np.array([0.10, 0.15, 0.20])
    panel = 5000.0
    inverter = 6000.0  # above any cf * panel value
    result = clipped_daily_mean_power(cf, panel, inverter, _flat_shape())
    expected = cf * panel
    np.testing.assert_allclose(result, expected, rtol=1e-9)


def test_flat_shape_reduces_to_daily_mean_clamp() -> None:
    """Flat shape makes per-hour clip identical to daily-mean clip."""
    cf = np.array([0.10, 0.25, 0.40])
    panel = 5000.0
    inverter = 1500.0  # below cf[2]*panel, above cf[0]*panel
    result = clipped_daily_mean_power(cf, panel, inverter, _flat_shape())
    expected = np.minimum(cf * panel, inverter)
    np.testing.assert_allclose(result, expected, rtol=1e-9)


def test_peaked_shape_clips_below_daily_mean_power() -> None:
    """Peaked shape causes clipping even when daily-mean DC is below inverter."""
    cf = np.array([0.20])
    panel = 5000.0
    # dc peak = 0.20 * 5000 * 2 = 2000 W; daily mean (no clip) = 1000 W
    inverter = 1500.0  # above daily mean but below peak
    result = clipped_daily_mean_power(cf, panel, inverter, _peaked_shape())
    # daytime hours: min(2000, 1500)=1500; night hours: 0 → mean = 1500/2 = 750
    expected = np.array([750.0])
    np.testing.assert_allclose(result, expected, rtol=1e-9)


def test_result_strictly_decreases_as_inverter_shrinks() -> None:
    """Yield is non-increasing as inverter capacity decreases."""
    cf = np.array([0.15, 0.20, 0.25])
    panel = 7800.0
    shape = _peaked_shape()
    inverter_values = [panel, 0.9 * panel, 0.7 * panel, 0.5 * panel, 0.3 * panel]
    results = [
        clipped_daily_mean_power(cf, panel, inv, shape).sum()
        for inv in inverter_values
    ]
    for i in range(len(results) - 1):
        assert results[i] >= results[i + 1]


def test_result_unchanged_when_inverter_exceeds_peak_dc() -> None:
    """No clipping when inverter >= the peak instantaneous DC for all days."""
    cf = np.array([0.10, 0.15])
    panel = 4000.0
    shape = _peaked_shape()
    peak_dc = (cf * panel * 2.0).max()  # factor 2 from the shape
    inverter = peak_dc * 1.01
    result = clipped_daily_mean_power(cf, panel, inverter, shape)
    expected = cf * panel  # mean of shape == 1, so mean(dc_w) = cf * panel
    np.testing.assert_allclose(result, expected, rtol=1e-9)


def test_zero_cf_gives_zero_output() -> None:
    cf = np.zeros(5)
    result = clipped_daily_mean_power(cf, 5000.0, 4000.0, _peaked_shape())
    np.testing.assert_allclose(result, np.zeros(5), atol=1e-10)


def test_output_shape_matches_input_days() -> None:
    cf = np.linspace(0.05, 0.25, 31)
    result = clipped_daily_mean_power(cf, 6000.0, 4000.0, np.ones(24))
    assert result.shape == (31,)


def test_hand_worked_example() -> None:
    """Explicit arithmetic for a 4-hour day shape and 1-day forecast."""
    # shape: 3 zero-hours, 1 peak-hour of 24, mean == 1
    shape = np.zeros(4)
    shape[3] = 4.0  # mean(shape) == 1
    cf = np.array([0.25])
    panel = 1000.0
    inverter = 800.0
    # dc at peak hour = 0.25 * 1000 * 4 = 1000 W; clipped to 800 W
    # daily mean ac = (0 + 0 + 0 + 800) / 4 = 200 W
    result = clipped_daily_mean_power(cf, panel, inverter, shape)
    np.testing.assert_allclose(result, [200.0], rtol=1e-9)

    # sanity: without clipping, mean ac = 0.25 * 1000 * mean(shape) = 250 W
    result_no_clip = clipped_daily_mean_power(cf, panel, 9999.0, shape)
    np.testing.assert_allclose(result_no_clip, [250.0], rtol=1e-9)
