"""UK PV capacity-factor map generation (offline tool)."""

from pv_prospect.map.capacity_factor import (
    annual_mean_capacity_factor,
    monthly_capacity_factors,
)
from pv_prospect.map.grid import build_grid_axes, land_mask, load_uk_geometry
from pv_prospect.map.render import render_contour_map
from pv_prospect.map.store import ModelStore, load_model_store

__version__ = '0.1.0'

__all__ = [
    'ModelStore',
    'annual_mean_capacity_factor',
    'build_grid_axes',
    'land_mask',
    'load_model_store',
    'load_uk_geometry',
    'monthly_capacity_factors',
    'render_contour_map',
]
