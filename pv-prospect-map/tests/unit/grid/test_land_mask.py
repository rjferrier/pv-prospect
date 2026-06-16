"""Tests for land_mask."""

from __future__ import annotations

import numpy as np
from pv_prospect.map.grid import build_grid_axes, land_mask
from shapely.geometry import box


def test_mask_shape_matches_grid() -> None:
    lats, lons = build_grid_axes(0.0, 2.0, 0.0, 3.0, 1.0)
    mask = land_mask(lats, lons, box(0, 0, 5, 5))
    assert mask.shape == (len(lats), len(lons))


def test_points_inside_polygon_are_true() -> None:
    # Polygon covers [1,3] x [1,3]; grid cell (lat=2, lon=2) is inside.
    lats, lons = build_grid_axes(0.0, 4.0, 0.0, 4.0, 1.0)
    mask = land_mask(lats, lons, box(1, 1, 3, 3))
    lat_i = int(np.where(lats == 2.0)[0][0])
    lon_i = int(np.where(lons == 2.0)[0][0])
    assert mask[lat_i, lon_i]


def test_points_outside_polygon_are_false() -> None:
    lats, lons = build_grid_axes(0.0, 4.0, 0.0, 4.0, 1.0)
    mask = land_mask(lats, lons, box(1, 1, 3, 3))
    lat_i = int(np.where(lats == 0.0)[0][0])
    lon_i = int(np.where(lons == 0.0)[0][0])
    assert not mask[lat_i, lon_i]
