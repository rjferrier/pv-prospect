"""UK Cartesian grid + landmask.

Builds a regular lat/lon grid over the UK and masks it to the landmass using the
Natural Earth 10m ``United Kingdom`` polygon — the same geometry source as
``pv-prospect/uk-geo/uk_geo.py`` (that standalone grid-generation tool is not a
package dependency of the monorepo, so the minimal load is replicated here).
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from urllib.request import urlopen

import numpy as np
import shapefile  # pyshp
import shapely
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

# UK bounding box (GB mainland + Northern Ireland) — matches the active
# UK_BOUNDING_BOXES in uk-geo/uk_geo.py (Shetland/Orkney/Hebrides excluded).
UK_LAT_MIN, UK_LAT_MAX = 49.9, 58.7
UK_LON_MIN, UK_LON_MAX = -8.2, 1.8

# Natural Earth source + cache directory, shared with uk-geo/uk_geo.py.
NATURAL_EARTH_10M_URL = (
    'https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_0_countries.zip'
)
GEO_CACHE_DIR = Path.home() / '.cache' / 'pv-prospect-geo'


def build_grid_axes(
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    resolution: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Return ``(lats, lons)`` 1-D axes on a regular grid at ``resolution`` degrees.

    Both endpoints are inclusive (within a half-step) so the box is fully covered.
    """
    lats = np.arange(lat_min, lat_max + resolution / 2, resolution)
    lons = np.arange(lon_min, lon_max + resolution / 2, resolution)
    return lats, lons


def _ensure_natural_earth_shapefile() -> Path:
    cache_path = GEO_CACHE_DIR / 'ne_10m_admin_0_countries'
    shp_file = cache_path / 'ne_10m_admin_0_countries.shp'
    if shp_file.exists():
        return shp_file
    print('Downloading Natural Earth 10m countries shapefile...')
    cache_path.mkdir(parents=True, exist_ok=True)
    with urlopen(NATURAL_EARTH_10M_URL) as response:
        data = response.read()
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        zf.extractall(cache_path)
    return shp_file


def load_uk_geometry() -> BaseGeometry:
    """Union of the Natural Earth 10m polygons whose ``ADMIN`` is United Kingdom."""
    shp_file = _ensure_natural_earth_shapefile()
    reader = shapefile.Reader(str(shp_file))
    field_names = [field[0] for field in reader.fields[1:]]
    admin_idx = field_names.index('ADMIN')
    geometries = [
        shape(shape_record.shape.__geo_interface__)
        for shape_record in reader.shapeRecords()
        if shape_record.record[admin_idx] == 'United Kingdom'
    ]
    return unary_union(geometries)


def land_mask(lats: np.ndarray, lons: np.ndarray, geometry: BaseGeometry) -> np.ndarray:
    """Boolean ``(n_lat, n_lon)`` mask: True where the grid cell is on UK land."""
    lon_grid, lat_grid = np.meshgrid(lons, lats)
    points = shapely.points(lon_grid.ravel(), lat_grid.ravel())
    return np.asarray(shapely.contains(geometry, points)).reshape(lon_grid.shape)


def _dilate(mask: np.ndarray, halo: int) -> np.ndarray:
    """Grow a boolean mask by ``halo`` cells using 4-connectivity (no edge wrap).

    Each pass adds the orthogonal neighbours of the current True cells, so after
    ``halo`` passes a cell is included if its L1 (taxicab) distance to an original
    True cell is at most ``halo``. ``halo >= 2`` therefore also reaches the
    diagonal neighbours (L1 = 2) of the original mask.
    """
    dilated = mask.copy()
    for _ in range(halo):
        grown = dilated.copy()
        grown[:-1, :] |= dilated[1:, :]
        grown[1:, :] |= dilated[:-1, :]
        grown[:, :-1] |= dilated[:, 1:]
        grown[:, 1:] |= dilated[:, :-1]
        dilated = grown
    return dilated


def fill_ghost_band(grid: np.ndarray, mask: np.ndarray, halo: int) -> np.ndarray:
    """Extrapolate ``grid`` onto a band of ghost cells just outside ``mask``.

    ``grid`` holds finite values on the ``mask`` (land) cells and is ignored
    elsewhere. A ghost band is the cells within ``halo`` of land but not land
    themselves; each is assigned its nearest land cell's value (nearest in grid
    index space). Cells beyond the band are left NaN.

    The band exists only so that the quad cells straddling the coastline have
    finite values at all four corners — a filled contour fills a quad only when
    every corner is finite. Because the map is later clipped to the exact land
    polygon, the ghost values never show at their own (off-land) locations; they
    only colour the fill along the coast, so nearest-neighbour is sufficient.
    """
    band = _dilate(mask, halo) & ~mask
    out = np.where(mask, grid, np.nan)
    land_index = np.argwhere(mask)
    land_value = grid[mask]
    for ghost_lat, ghost_lon in np.argwhere(band):
        squared_distance = (land_index[:, 0] - ghost_lat) ** 2 + (
            land_index[:, 1] - ghost_lon
        ) ** 2
        out[ghost_lat, ghost_lon] = land_value[int(squared_distance.argmin())]
    return out
