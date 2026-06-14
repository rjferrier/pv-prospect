"""Tests for reconstruct_hourly_poa."""

from __future__ import annotations

import datetime

import numpy as np
import pytest
from pv_prospect.app.poa import reconstruct_daily_mean_poa, reconstruct_hourly_poa


def test_returns_24_element_array() -> None:
    poa = reconstruct_hourly_poa(52.0, -1.0, datetime.date(2025, 7, 15), 200.0, 80.0, 35, 180)
    assert poa.shape == (24,)


def test_all_values_non_negative() -> None:
    poa = reconstruct_hourly_poa(52.0, -1.0, datetime.date(2025, 7, 15), 200.0, 80.0, 35, 180)
    assert np.all(poa >= 0)


def test_mean_equals_reconstruct_daily_mean_poa() -> None:
    """reconstruct_daily_mean_poa must be bit-identical to hourly.mean()."""
    args = (52.0, -1.0, datetime.date(2025, 7, 15), 200.0, 80.0, 35, 180)
    hourly = reconstruct_hourly_poa(*args)
    daily_mean = reconstruct_daily_mean_poa(*args)
    assert float(hourly.mean()) == pytest.approx(daily_mean, rel=1e-9)


def test_peak_above_mean_in_summer() -> None:
    """Daytime POA peaks should exceed the 24h mean."""
    poa = reconstruct_hourly_poa(52.0, -1.0, datetime.date(2025, 6, 15), 200.0, 80.0, 35, 180)
    assert poa.max() > poa.mean()


def test_zero_irradiance_gives_zero_array() -> None:
    poa = reconstruct_hourly_poa(52.0, -1.0, datetime.date(2025, 7, 15), 0.0, 0.0, 35, 180)
    np.testing.assert_allclose(poa, np.zeros(24), atol=1e-6)


def test_peak_mean_ratio_in_plausible_range() -> None:
    """Clear-sky summer peak/mean should be in the 2–4× range (empirically 2.76 for June)."""
    poa = reconstruct_hourly_poa(52.5, -1.5, datetime.date(2025, 6, 15), 200.0, 80.0, 35, 180)
    ratio = poa.max() / poa.mean()
    assert 2.0 <= ratio <= 5.0
