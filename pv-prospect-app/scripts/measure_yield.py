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
would understate the API's true error. Here "actual" is the energy *integrated*
from PVOutput power (or PVOutput's own daily energy totals) -- real kWh.

Why this is the right yardstick: with a perfect model + perfect weather the
chain provably recovers true daily energy (the daytime-weighting cancels in the
``energy = power x 24`` integration -- demonstrated in energy_cancel.py). So any
systematic predicted/actual gap here is MODEL error (weather-model irradiance is
the leading suspect; PV-MLP nonlinearity and the temperature convention are
secondary), not the aggregation convention and not vintage.

PREREQUISITES (cannot run without these -- not in the instance repo by default):
  * Trained artifacts in the model store. Set ``STORE_DIR`` (local dir or
    ``gs://pv-prospect-versioned-model``); ``load_store`` reads PV + weather.
  * A directory of per-site ACTUAL PVOutput data covering the comparison window,
    one CSV per site with at least ``time,power`` (cleaned-PV format) OR a
    precomputed actuals CSV ``system_id,actual_annual_kwh``.
  * Run in the app's env:  ``cd pv-prospect-app && poetry run python
    scripts/measure_yield.py --help``

STATUS: untested -- pending the artifacts above. Faithful to the production
chain (imports the real ``predict_yield``); the only approximations are flagged
inline (multi-panel area-weighting; single-year actuals are weather-noisy, so
trust the cross-site aggregate, not individual sites).
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
from pv_prospect.app.chain import predict_yield
from pv_prospect.app.elevation import get_grid_cell_elevation
from pv_prospect.app.store import load_store


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


def actual_annual_kwh_from_power(site_csv: Path, start: dt.date, end: dt.date) -> float:
    """TRUE generation: integrate PVOutput power over [start, end] -> kWh.

    Expects a cleaned-PV CSV (``time,power``) at native cadence. Night/gaps
    contribute 0. This is real energy, NOT the corpus ``power x 24``.
    """
    df = pd.read_csv(site_csv, parse_dates=['time']).sort_values('time')
    mask = (df['time'].dt.date >= start) & (df['time'].dt.date <= end)
    df = df[mask].reset_index(drop=True)
    if df.empty:
        return float('nan')
    dt_h = df['time'].diff().dt.total_seconds().div(3600).fillna(0.0).clip(upper=0.5)
    return float((df['power'] * dt_h).sum()) / 1000.0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--store-dir', required=True,
                    help='Model store (local dir or gs:// URI) for load_store')
    ap.add_argument('--pv-sites-csv', required=True, type=Path)
    ap.add_argument('--actuals-dir', type=Path,
                    help='Dir of per-site cleaned-PV CSVs named <system_id>.csv')
    ap.add_argument('--actuals-csv', type=Path,
                    help='Alt: precomputed CSV system_id,actual_annual_kwh')
    ap.add_argument('--start', required=True, type=dt.date.fromisoformat)
    ap.add_argument('--end', required=True, type=dt.date.fromisoformat)
    args = ap.parse_args()

    store = load_store(args.store_dir)
    print(f'Loaded PV={store.pv_version} weather={store.weather_version}\n')
    sites = load_sites(args.pv_sites_csv)

    precomputed = {}
    if args.actuals_csv:
        with open(args.actuals_csv) as fh:
            for r in csv.DictReader(fh):
                precomputed[int(r['system_id'])] = float(r['actual_annual_kwh'])

    print(f"{'site':>7} {'predicted':>10} {'actual':>10} {'pred/act':>9}")
    print('-' * 40)
    ratios = []
    for sid, row in sorted(sites.items()):
        try:
            actual = (precomputed.get(sid) if precomputed else
                      actual_annual_kwh_from_power(
                          args.actuals_dir / f'{sid}.csv', args.start, args.end))
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
