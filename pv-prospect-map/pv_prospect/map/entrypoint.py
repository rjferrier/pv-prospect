"""CLI: build the UK annual-mean PV capacity-factor map.

Usage (from this package, with its venv installed)::

    poetry run capacity-factor-map \
        --store-dir <local-store> \
        --out-dir out/cf-map --resolution 0.2

``<local-store>`` is a local model store with a ``promoted/{pv,weather}`` layout
whose PV artifact matches the current feature spec — see the README for which
store to use and how to assemble one from the repo's artifacts.

Writes ``capacity_factor.csv`` (the per-cell data) and ``capacity_factor_map.png``
(the contour render) under ``--out-dir``. Elevation lookups are cached in
``<out-dir>/elevation_cache.json`` so re-runs need no network.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from pv_prospect.map.capacity_factor import (
    DEFAULT_AZIMUTH,
    DEFAULT_TILT,
    annual_mean_capacity_factor,
    monthly_capacity_factors,
)
from pv_prospect.map.elevation import ElevationProvider
from pv_prospect.map.grid import (
    UK_LAT_MAX,
    UK_LAT_MIN,
    UK_LON_MAX,
    UK_LON_MIN,
    build_grid_axes,
    land_mask,
    load_uk_geometry,
)
from pv_prospect.map.render import render_contour_map
from pv_prospect.map.store import load_model_store


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--store-dir',
        required=True,
        type=Path,
        help='Local model store dir (expects <dir>/promoted/{pv,weather})',
    )
    parser.add_argument('--out-dir', type=Path, default=Path('out/cf-map'))
    parser.add_argument(
        '--resolution',
        type=float,
        default=0.2,
        help='Grid resolution in degrees (default 0.2)',
    )
    parser.add_argument('--tilt', type=float, default=DEFAULT_TILT)
    parser.add_argument('--azimuth', type=int, default=DEFAULT_AZIMUTH)
    parser.add_argument('--age-years', type=float, default=0.0)
    args = parser.parse_args()

    model_store = load_model_store(args.store_dir)
    print('Loaded model store')

    lats, lons = build_grid_axes(
        UK_LAT_MIN, UK_LAT_MAX, UK_LON_MIN, UK_LON_MAX, args.resolution
    )
    geometry = load_uk_geometry()
    mask = land_mask(lats, lons, geometry)
    lat_idx, lon_idx = np.where(mask)
    print(f'Grid {len(lats)}x{len(lons)}; {int(mask.sum())} UK land cells')

    cell_lats = lats[lat_idx]
    cell_lons = lons[lon_idx]
    elevations = ElevationProvider(
        args.out_dir / 'elevation_cache.json'
    ).elevations_for(list(zip(cell_lats, cell_lons, strict=False)))

    cells = pd.DataFrame(
        {'latitude': cell_lats, 'longitude': cell_lons, 'elevation': elevations}
    )
    monthly_cf = monthly_capacity_factors(
        cells, model_store, args.tilt, args.azimuth, args.age_years
    )
    cf = annual_mean_capacity_factor(monthly_cf)

    # Scatter the per-cell CF back into a 2-D grid (NaN outside the landmask).
    cf_grid = np.full(mask.shape, np.nan)
    cf_grid[lat_idx, lon_idx] = cf
    cf_percent_grid = cf_grid * 100.0

    args.out_dir.mkdir(parents=True, exist_ok=True)
    data_csv = args.out_dir / 'capacity_factor.csv'
    pd.DataFrame(
        {'latitude': cell_lats, 'longitude': cell_lons, 'capacity_factor': cf}
    ).to_csv(data_csv, index=False)

    out_png = args.out_dir / 'capacity_factor_map.png'
    render_contour_map(lats, lons, cf_percent_grid, geometry, out_png)

    print(
        f'\nCapacity factor: min={100 * cf.min():.1f}%  '
        f'mean={100 * cf.mean():.1f}%  max={100 * cf.max():.1f}%'
    )
    print(f'Wrote {data_csv}')
    print(f'Wrote {out_png}')
    print(f'Wrote {out_png.with_name(f"{out_png.stem}_meta.json")}')


if __name__ == '__main__':
    main()
