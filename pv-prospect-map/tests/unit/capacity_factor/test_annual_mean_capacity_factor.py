"""Tests for annual_mean_capacity_factor."""

from __future__ import annotations

import numpy as np
from pv_prospect.map.capacity_factor import MONTH_WEIGHTS, annual_mean_capacity_factor


def test_constant_series_returns_constant() -> None:
    monthly = np.full((12, 4), 0.15)
    result = annual_mean_capacity_factor(monthly)
    assert np.allclose(result, 0.15)


def test_day_weighted_average() -> None:
    # One cell; each month equals its index, weighted by days-in-month.
    monthly = np.arange(12, dtype=float).reshape(12, 1)
    expected = np.average(np.arange(12), weights=np.array(MONTH_WEIGHTS))
    result = annual_mean_capacity_factor(monthly)
    assert np.isclose(result[0], expected)


def test_per_cell_independence() -> None:
    monthly = np.zeros((12, 2))
    monthly[:, 0] = 0.1
    monthly[:, 1] = 0.2
    result = annual_mean_capacity_factor(monthly)
    assert np.isclose(result[0], 0.1)
    assert np.isclose(result[1], 0.2)
