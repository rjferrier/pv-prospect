"""Tests for build_grid_axes."""

from __future__ import annotations

import numpy as np
from pv_prospect.map.grid import build_grid_axes


def test_axes_start_at_minimum() -> None:
    lats, lons = build_grid_axes(50.0, 52.0, -2.0, 0.0, 0.5)
    assert lats[0] == 50.0
    assert lons[0] == -2.0


def test_resolution_spacing() -> None:
    lats, lons = build_grid_axes(50.0, 52.0, -2.0, 0.0, 0.5)
    assert np.allclose(np.diff(lats), 0.5)
    assert np.allclose(np.diff(lons), 0.5)


def test_endpoints_inclusive() -> None:
    lats, _ = build_grid_axes(50.0, 52.0, -2.0, 0.0, 0.5)
    assert lats[-1] == 52.0  # within a half-step of the max → included


def test_grid_dimensions() -> None:
    lats, lons = build_grid_axes(50.0, 52.0, -2.0, 1.0, 1.0)
    assert len(lats) == 3  # 50, 51, 52
    assert len(lons) == 4  # -2, -1, 0, 1
