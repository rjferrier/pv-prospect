"""Filled-contour rendering of a capacity-factor grid."""

from __future__ import annotations

import json
import math
from pathlib import Path

import matplotlib
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.path import Path as MplPath
from shapely.geometry.base import BaseGeometry

from pv_prospect.map.grid import fill_ghost_band

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402  (backend must be set first)

# Cells (beyond land) onto which the field is extrapolated so contour fills reach
# the coast before being clipped to it. 3 (>= 2) covers land's diagonal neighbours.
GHOST_HALO = 3

# Low-to-high capacity-factor ramp matching the PV Prospect website palette
# (royal blue -> teal -> amber -> sun). The stops mirror the site's legend
# gradient so the rendered map reads as the same family as the front end.
CAPACITY_FACTOR_CMAP = LinearSegmentedColormap.from_list(
    'pv_prospect',
    [
        (0.00, '#2b6fb0'),  # blue   — lower
        (0.45, '#5092a3'),  # teal
        (0.74, '#e7a23f'),  # amber
        (1.00, '#f59e0b'),  # sun    — higher
    ],
)


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
) -> None:
    """Render the ``(n_lat, n_lon)`` capacity-factor grid (NaN outside land).

    Produces a **bare, transparent** web overlay: the UK silhouette filled with
    the brand ramp plus contour lines, with no axes, title, or colour bar and a
    transparent background, so it drops cleanly onto the website backdrop. The
    site supplies its own gradient legend, so none is baked in; the legend's
    numeric range is written to a ``<out_png stem>_meta.json`` sidecar (the
    min/max contour-level percentages the colour ramp spans).

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
    filled = ax.contourf(
        lon_grid, lat_grid, field, levels=levels, cmap=CAPACITY_FACTOR_CMAP
    )
    lines = ax.contour(
        lon_grid,
        lat_grid,
        field,
        levels=levels,
        colors='white',
        linewidths=0.4,
        alpha=0.5,
    )

    clip_path = _geometry_to_clip_path(geometry)
    filled.set_clip_path(clip_path, ax.transData)
    lines.set_clip_path(clip_path, ax.transData)

    # UK coastline, drawn over the fill in the brand navy (matches the site).
    polygons = getattr(geometry, 'geoms', [geometry])
    for polygon in polygons:
        x, y = polygon.exterior.xy
        ax.plot(x, y, color='#16395a', linewidth=0.7, alpha=0.6)

    mean_lat = float(np.mean(lats))
    ax.set_aspect(1.0 / math.cos(math.radians(mean_lat)))
    ax.set_xlim(float(lons.min()), float(lons.max()))
    ax.set_ylim(float(lats.min()), float(lats.max()))
    ax.axis('off')

    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150, bbox_inches='tight', pad_inches=0, transparent=True)
    plt.close(fig)

    # Sidecar metadata: the contour-level bounds the colour ramp spans, so the
    # website can label its gradient legend with the real range (the baked-in
    # colour bar having been dropped). These are the ends of the fill normalisation
    # (floor(min)..ceil(max) in percent), i.e. what the bar's two ends represent.
    meta_path = out_png.with_name(f'{out_png.stem}_meta.json')
    meta_path.write_text(
        json.dumps(
            {
                'min_capacity_factor_percent': round(float(levels[0]), 2),
                'max_capacity_factor_percent': round(float(levels[-1]), 2),
            },
            indent=2,
        )
        + '\n'
    )
