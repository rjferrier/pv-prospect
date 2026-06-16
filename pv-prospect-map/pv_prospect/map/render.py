"""Filled-contour rendering of a capacity-factor grid."""

from __future__ import annotations

import math
from pathlib import Path

import matplotlib
import numpy as np
from matplotlib.path import Path as MplPath
from shapely.geometry.base import BaseGeometry

from pv_prospect.map.grid import fill_ghost_band

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402  (backend must be set first)

# Cells (beyond land) onto which the field is extrapolated so contour fills reach
# the coast before being clipped to it. 3 (>= 2) covers land's diagonal neighbours.
GHOST_HALO = 3


def _geometry_to_clip_path(geometry: BaseGeometry) -> MplPath:
    """Compound matplotlib Path over every ring of the (multi)polygon geometry."""
    rings = []
    for polygon in getattr(geometry, 'geoms', [geometry]):
        rings.append(MplPath(np.asarray(polygon.exterior.coords)))
        for interior in polygon.interiors:
            rings.append(MplPath(np.asarray(interior.coords)))
    return MplPath.make_compound_path(*rings)


def render_contour_map(
    lats: np.ndarray,
    lons: np.ndarray,
    cf_percent_grid: np.ndarray,
    geometry: BaseGeometry,
    out_png: Path,
    *,
    title: str,
) -> None:
    """Render the ``(n_lat, n_lon)`` capacity-factor grid (NaN outside land).

    ``cf_percent_grid`` is capacity factor in percent. To stop the filled
    contours falling short of the coastline (a quad fills only when all four
    corners are finite), the field is first extrapolated onto a band of ghost
    cells just outside land; the contours are then clipped to the exact UK
    polygon so the overhang is trimmed away. The boundary is drawn over the
    contours; aspect is corrected for longitude compression at the grid's mean
    latitude.
    """
    lon_grid, lat_grid = np.meshgrid(lons, lats)
    finite = cf_percent_grid[np.isfinite(cf_percent_grid)]
    levels = np.linspace(np.floor(finite.min()), np.ceil(finite.max()), 13)

    land = np.isfinite(cf_percent_grid)
    field = fill_ghost_band(cf_percent_grid, land, GHOST_HALO)

    fig, ax = plt.subplots(figsize=(7, 9))
    filled = ax.contourf(lon_grid, lat_grid, field, levels=levels, cmap='viridis')
    lines = ax.contour(
        lon_grid,
        lat_grid,
        field,
        levels=levels,
        colors='black',
        linewidths=0.3,
        alpha=0.35,
    )

    clip_path = _geometry_to_clip_path(geometry)
    filled.set_clip_path(clip_path, ax.transData)
    lines.set_clip_path(clip_path, ax.transData)

    polygons = getattr(geometry, 'geoms', [geometry])
    for polygon in polygons:
        x, y = polygon.exterior.xy
        ax.plot(x, y, color='black', linewidth=0.5)

    colorbar = fig.colorbar(filled, ax=ax, fraction=0.046, pad=0.04)
    colorbar.set_label('Annual-mean capacity factor (%)')

    mean_lat = float(np.mean(lats))
    ax.set_aspect(1.0 / math.cos(math.radians(mean_lat)))
    ax.set_xlim(float(lons.min()), float(lons.max()))
    ax.set_ylim(float(lats.min()), float(lats.max()))
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_title(title)

    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150, bbox_inches='tight')
    plt.close(fig)
