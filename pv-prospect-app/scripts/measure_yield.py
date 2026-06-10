"""Yield-space ground truth: does ``/predict`` match sites' ACTUAL generation?

This is step 1 of the corrected ``weather-pv-vintage-alignment`` brief. The
documented "~30% systematic underestimate" was only ever a POA-space MAPE
asserted to propagate; it was never measured as predicted-vs-actual annual kWh.
This script measures it directly, end-to-end (weather model -> POA -> PV model
-> energy), so we know (a) whether a real yield bias exists and (b) its size --
before investing in any re-transform/retrain.

IMPORTANT -- compare against TRUE generation, not the corpus ``power x 24``.
The prepared-PV corpus / validation window expose ``actual_kwh = power x 24``,
where ``power`` is the *daytime-weighted* daily mean (PVOutput night rows are
dropped, so it is inflated ~+26%; see FINDINGS.md). Using that as "actual"
would understate the API's true error. Here "actual" is PVOutput's own daily
energy total (the cumulative ``energy`` column's end-of-day value) -- real kWh.

Why this is the right yardstick: with a perfect model + perfect weather the
chain provably recovers true daily energy (the daytime-weighting cancels in the
``energy = power x 24`` integration -- demonstrated in energy_cancel.py). So any
systematic predicted/actual gap here is MODEL error (weather-model irradiance is
the leading suspect; PV-MLP nonlinearity and the temperature convention are
secondary), not the aggregation convention and not vintage.

PREREQUISITES (cannot run without these -- not in the instance repo by default):
  * Trained artifacts in the model store. Set ``STORE_DIR`` (local dir or
    ``gs://pv-prospect-versioned-model``); ``load_store`` reads PV + weather.
  * Per-site ACTUAL generation covering the window. Either a directory of RAW
    PVOutput CSVs named ``<system_id>.csv`` (``date,time,energy,...``; true kWh
    comes from the cumulative ``energy`` column -- cleaned/prepared have it
    stripped), OR a precomputed CSV ``system_id,actual_annual_kwh``.
  * Run in the app's env:  ``cd pv-prospect-app && poetry run python
    scripts/measure_yield.py --help``

Faithful to the production chain (imports the real ``predict_yield``); the only
approximations are flagged inline (multi-panel area-weighting; single-year
actuals are weather-noisy, so trust the cross-site aggregate, not individual
sites).
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from pv_prospect.app.chain import predict_yield
from pv_prospect.app.elevation import get_grid_cell_elevation
from pv_prospect.app.store import filesystem_for, load_store

if TYPE_CHECKING:
    from pv_prospect.etl.storage import FileSystem


def _panel_geometries(row: dict) -> list[tuple[int, int, float]]:
    """(azimuth, tilt, area_fraction) per panel group from a pv_sites.csv row."""
    geoms = []
    for i in (1, 2, 3):
        az = row.get(f'panel_azimuth_{i}', '').strip()
        ti = row.get(f'panel_elevation_{i}', '').strip()
        af = row.get(f'panel_area_fraction_{i}', '').strip()
        if not (az or ti or af):
            continue
        geoms.append((int(az) if az else 180, int(ti) if ti else 0,
                      float(af) if af else 1.0))
    return geoms


def load_sites(pv_sites_csv: Path) -> dict[int, dict]:
    sites = {}
    with open(pv_sites_csv) as fh:
        for row in csv.DictReader(fh):
            try:
                sid = int(row['pvoutput_system_id'])
            except (ValueError, KeyError):
                continue
            sites[sid] = row
    return sites


def predicted_annual_kwh(row: dict, store, start: dt.date, end: dt.date) -> float:
    """Run the real predict chain for one site over [start, end].

    ``predict_yield`` takes a single (tilt, azimuth); for multi-panel sites we
    run it per panel group at ``area_fraction * capacity`` and sum -- an
    approximation of the corpus's area-weighted POA. Capacities come from
    pv_sites.csv; elevation from the production elevation lookup.
    """
    lat, lon = float(row['latitude']), float(row['longitude'])
    elevation = get_grid_cell_elevation(lat, lon)
    cap = float(row['panels_capacity'])
    inv = float(row['inverter_capacity']) if row.get('inverter_capacity') else cap
    geoms = _panel_geometries(row)
    total = 0.0
    for az, tilt, af in geoms:
        res = predict_yield(
            latitude=lat, longitude=lon, elevation=elevation,
            start_date=start, end_date=end,
            panels_capacity_w=cap * af, inverter_capacity_w=inv * af,
            tilt=tilt, azimuth=az, age_years=0.0,
            pv_artifact=store.pv, weather_artifact=store.weather,
        )
        total += res.expected_annual_kwh
    return total


def actual_annual_kwh_from_raw_energy(site_csv: Path, start: dt.date, end: dt.date) -> float:
    """TRUE generation from raw PVOutput's ``energy`` column (cumulative Wh/day).

    Raw PVOutput ``energy`` resets each day and accumulates within it, so the
    day's LAST (== max) non-blank reading is that day's total generation
    (e.g. 89665/06-09 ends at 35519 Wh = 35.5 kWh). Sum those daily totals over
    [start, end].

    This is the ONLY faithful source of true kWh. Cleaned/prepared corpora drop
    night rows and strip ``energy``, and the corpus ``power x 24`` is the
    inflated daytime-weighted mean -- neither is the truth (see FINDINGS.md).
    Expects a raw PVOutput CSV (``date,time,energy,...``); concatenate the
    per-day raw files for a site into one CSV before passing them here.
    """
    df = pd.read_csv(site_csv)
    if 'energy' not in df.columns or 'date' not in df.columns:
        raise ValueError(f'{site_csv}: not a raw PVOutput file (need date,energy)')
    df = df[df['energy'].notna()]
    if df.empty:
        return float('nan')
    day = pd.to_datetime(df['date'].astype(int).astype(str), format='%Y%m%d').dt.date
    df = df.assign(_day=day)
    df = df[(df['_day'] >= start) & (df['_day'] <= end)]
    if df.empty:
        return float('nan')
    daily_wh = df.groupby('_day')['energy'].max()  # cumulative => max is the daily total
    return float(daily_wh.sum()) / 1000.0


def actual_annual_kwh_from_gcs(
    fs: 'FileSystem', site_id: int, start: dt.date, end: dt.date
) -> float:
    """True generation from daily raw PVOutput files in GCS.

    ``fs`` must be rooted at the PVOutput raw prefix (e.g.
    ``gs://pv-prospect-staging/data/raw/timeseries/pvoutput/``).
    Lists files under ``<site_id>/pvoutput_<site_id>_<YYYYMMDD>.csv``,
    filters to [start, end], and sums daily totals from the cumulative
    ``energy`` column. Makes one GCS read per in-window day -- expect a
    few minutes for a full year across all sites.
    """
    entries = fs.list_files(str(site_id), pattern='pvoutput_*.csv')
    daily_totals: dict[dt.date, float] = {}
    for entry in entries:
        date_str = entry.name.rsplit('_', 1)[-1].replace('.csv', '')
        try:
            day = dt.date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        except ValueError:
            continue
        if not (start <= day <= end):
            continue
        df = pd.read_csv(io.StringIO(fs.read_text(entry.path)))
        df = df[df['energy'].notna()]
        if not df.empty:
            daily_totals[day] = float(df['energy'].max())
    if not daily_totals:
        return float('nan')
    return sum(daily_totals.values()) / 1000.0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--store-dir', required=True,
                    help='Model store (local dir or gs:// URI) for load_store')
    ap.add_argument('--pv-sites-csv', required=True, type=Path)
    ap.add_argument('--actuals-dir', type=Path,
                    help='Dir of per-site RAW PVOutput CSVs named <system_id>.csv '
                         '(date,time,energy,... -- NOT cleaned/prepared)')
    ap.add_argument('--actuals-gcs-prefix',
                    help='GCS prefix for raw PVOutput per-day files, e.g. '
                         'gs://pv-prospect-staging/data/raw/timeseries/pvoutput/')
    ap.add_argument('--actuals-csv', type=Path,
                    help='Alt: precomputed CSV system_id,actual_annual_kwh')
    ap.add_argument('--start', required=True, type=dt.date.fromisoformat)
    ap.add_argument('--end', required=True, type=dt.date.fromisoformat)
    args = ap.parse_args()

    store = load_store(args.store_dir)
    print(f'Loaded PV={store.pv_version} weather={store.weather_version}\n')
    sites = load_sites(args.pv_sites_csv)

    precomputed: dict[int, float] = {}
    if args.actuals_csv:
        with open(args.actuals_csv) as fh:
            for r in csv.DictReader(fh):
                precomputed[int(r['system_id'])] = float(r['actual_annual_kwh'])

    gcs_fs = filesystem_for(args.actuals_gcs_prefix) if args.actuals_gcs_prefix else None

    print(f"{'site':>7} {'predicted':>10} {'actual':>10} {'pred/act':>9}")
    print('-' * 40)
    ratios = []
    for sid, row in sorted(sites.items()):
        try:
            if precomputed:
                actual: float | None = precomputed.get(sid)
            elif gcs_fs is not None:
                actual = actual_annual_kwh_from_gcs(gcs_fs, sid, args.start, args.end)
                print(f'{sid:>7}  actuals read', flush=True)
            else:
                actual = actual_annual_kwh_from_raw_energy(
                    args.actuals_dir / f'{sid}.csv', args.start, args.end)
        except FileNotFoundError:
            continue
        if actual is None or np.isnan(actual) or actual <= 0:
            continue
        predicted = predicted_annual_kwh(row, store, args.start, args.end)
        ratio = predicted / actual
        ratios.append(ratio)
        print(f'{sid:>7} {predicted:>10.0f} {actual:>10.0f} {ratio:>9.3f}')

    if ratios:
        arr = np.array(ratios)
        print('-' * 40)
        print(f'sites={len(arr)}  mean pred/act={arr.mean():.3f}  '
              f'median={np.median(arr):.3f}')
        print(f'=> systematic yield bias = {100*(arr.mean()-1):+.1f}% '
              f'(negative = API underestimates real generation)')
        print('\nIf ~0%: no yield problem; the documented ~30% was a POA-space '
              'artifact that cancels.\nIf materially negative: it is MODEL error '
              '(weather irradiance leading suspect), NOT vintage/aggregation.')


if __name__ == '__main__':
    main()
