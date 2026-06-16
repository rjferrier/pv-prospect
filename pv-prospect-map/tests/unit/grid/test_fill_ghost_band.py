"""Tests for fill_ghost_band."""

from __future__ import annotations

import numpy as np
from pv_prospect.map.grid import fill_ghost_band


def _grid_with(mask: np.ndarray, value: float) -> np.ndarray:
    grid = np.full(mask.shape, np.nan)
    grid[mask] = value
    return grid


def test_land_cells_are_unchanged() -> None:
    mask = np.zeros((7, 7), dtype=bool)
    mask[3, 3] = True
    grid = _grid_with(mask, 5.0)
    result = fill_ghost_band(grid, mask, halo=2)
    assert result[3, 3] == 5.0
    assert result.shape == grid.shape


def test_ghost_cell_takes_nearest_land_value() -> None:
    # Single land cell at (3, 3): every ghost cell must inherit exactly its value.
    mask = np.zeros((7, 7), dtype=bool)
    mask[3, 3] = True
    grid = _grid_with(mask, 5.0)
    result = fill_ghost_band(grid, mask, halo=2)
    assert result[3, 5] == 5.0  # L1 = 2, inside the band
    assert result[1, 3] == 5.0  # L1 = 2, inside the band
    assert result[2, 2] == 5.0  # diagonal neighbour (L1 = 2)


def test_cells_beyond_halo_stay_nan() -> None:
    mask = np.zeros((7, 7), dtype=bool)
    mask[3, 3] = True
    grid = _grid_with(mask, 5.0)
    result = fill_ghost_band(grid, mask, halo=2)
    assert np.isnan(result[3, 6])  # L1 = 3 > halo
    assert np.isnan(result[0, 0])  # far corner


def test_ghost_cell_resolves_to_the_closer_land() -> None:
    mask = np.zeros((1, 7), dtype=bool)
    mask[0, 1] = True
    mask[0, 5] = True
    grid = np.full((1, 7), np.nan)
    grid[0, 1] = 10.0
    grid[0, 5] = 20.0
    result = fill_ghost_band(grid, mask, halo=3)
    assert result[0, 2] == 10.0  # nearer the left land
    assert result[0, 4] == 20.0  # nearer the right land
