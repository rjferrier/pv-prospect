from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import re
from datetime import datetime
from typing import Iterator
import time

import pandas as pd
import numpy as np
import pvlib
from pv_prospect.common import PVSite
from pv_prospect.common.pv_site_repo import build_pv_site_repo, get_pv_site_by_system_id


ALTITUDE = 0

# Weather model to analyze
WEATHER_MODEL = 'ukmo_seamless'  # Options: best_match, dmi_seamless, gem_seamless, gfs_seamless, icon_seamless, jma_seamless, kma_seamless, knmi_seamless, meteofrance_seamless, metno_seamless, ukmo_seamless

PV_SITES_PATH = Path('data-0/pv_sites.csv')

TIMESERIES_FOLDER = 'timeseries'
SOURCE_TIMESERIES_DIR = Path('data-1') / TIMESERIES_FOLDER
TARGET_TIMESERIES_DIR = Path('data-2') / TIMESERIES_FOLDER

# Columns to exclude from output
EXCLUDED_COLUMNS = {
    # 'direct_normal_irradiance',
    # 'diffuse_radiation',
    # 'cloud_cover',
    # 'visibility',
    'pressure_msl',
    'weather_code',
    'wind_speed_80m',
    'wind_speed_180m',
    'wind_direction_80m',
    'wind_direction_180m',
}


def process_timeseries_data() -> None:
    """
    Process all micro-batches in SOURCE_TIMESERIES_DIR and create processed timeseries files.
    
    Processes micro-batches in parallel for better performance.
    """
    # Load PV sites repository
    print("Loading PV sites repository...")
    with open(PV_SITES_PATH, 'r') as pv_sites_file:
        build_pv_site_repo(pv_sites_file)
    print("PV sites repository loaded successfully")
    
    # Create target directory if it doesn't exist
    TARGET_TIMESERIES_DIR.mkdir(parents=True, exist_ok=True)
    
    # Get all micro-batch files
    print(f"Scanning {SOURCE_TIMESERIES_DIR} for micro-batches...")
    micro_batches = list(SOURCE_TIMESERIES_DIR.glob('*.csv'))
    print(f"Found {len(micro_batches)} micro-batch files")
    
    # Group micro-batches by PV site ID
    site_batches = _group_batches_by_site(micro_batches)
    print(f"Processing {len(site_batches)} PV sites...\n")

    # Prepare tasks for parallel processing
    tasks = []
    for site_id in sorted(site_batches.keys()):
        for batch_file in sorted(site_batches[site_id]):
            tasks.append((batch_file, site_id))

    print(f"Processing {len(tasks)} micro-batches in parallel...")

    # Process micro-batches in parallel
    start_time = time.perf_counter()
    completed = 0
    failed = 0

    with ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(_process_batch_file, batch_file, site_id): batch_file.name
            for batch_file, site_id in tasks
        }

        for future in as_completed(futures):
            batch_filename = futures[future]
            try:
                output_filename = future.result()
                completed += 1
                print(f"[{completed}/{len(tasks)}] Completed: {output_filename}")
            except Exception as e:
                failed += 1
                print(f"[{completed + failed}/{len(tasks)}] Failed: {batch_filename} - Error: {str(e)}")

    end_time = time.perf_counter()
    total_time = end_time - start_time

    print(f"\nProcessing complete!")
    print(f"Successfully processed: {completed}")
    print(f"Failed: {failed}")
    print(f"Total: {len(tasks)}")
    print(f"Total time taken: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    if completed > 0:
        print(f"Average time per file: {total_time/completed:.2f} seconds")


def _process_batch_file(batch_file: Path, site_id: int) -> str:
    """Process a single micro-batch file - designed to be called in parallel."""
    pv_site = get_pv_site_by_system_id(site_id)
    df = _process_micro_batch(batch_file, pv_site)

    # Write processed micro-batch to data-2/timeseries with same filename format
    output_file = TARGET_TIMESERIES_DIR / batch_file.name
    df.to_csv(output_file, index=False, encoding='utf-8')

    return batch_file.name


def _group_batches_by_site(batch_files: list[Path]) -> dict[int, list[Path]]:
    """Group micro-batch files by PV site ID."""
    # Micro-batch filename format: {pv_site_id}_{start_date}_{end_date}.csv
    pattern = re.compile(r'^(\d+)_\d{8}_\d{8}\.csv$')
    
    site_batches = {}
    for batch_file in batch_files:
        match = pattern.match(batch_file.name)
        if match:
            site_id = int(match.group(1))
            if site_id not in site_batches:
                site_batches[site_id] = []
            site_batches[site_id].append(batch_file)
    
    return site_batches


def _process_micro_batch(batch_file: Path, pv_site: PVSite) -> pd.DataFrame:
    """
    Process a single micro-batch file.
    
    Returns a dataframe with:
    - time column
    - weather model columns (without suffix, excluding certain columns)
    - plane_of_array_irradiance column
    - power column (PV output)
    """

    # Load the micro-batch
    df = pd.read_csv(batch_file)
    df['time'] = pd.to_datetime(df['time'])
    
    # Select and rename columns
    processed_df = pd.DataFrame()
    processed_df['time'] = df['time']
    
    # Add all columns with the WEATHER_MODEL suffix, removing the suffix
    # but excluding specified columns
    suffix = f'_{WEATHER_MODEL}'
    for col in df.columns:
        if col.endswith(suffix):
            new_col_name = col[:-len(suffix)]
            if new_col_name not in EXCLUDED_COLUMNS:
                processed_df[new_col_name] = df[col]

    # Calculate POA irradiance (needs DNI and DHI temporarily)
    # Create temp df with needed columns for calculation
    temp_df = pd.DataFrame()
    temp_df['time'] = df['time']
    for col in ['direct_normal_irradiance', 'diffuse_radiation']:
        col_with_suffix = f'{col}_{WEATHER_MODEL}'
        if col_with_suffix in df.columns:
            temp_df[col] = df[col_with_suffix]

    processed_df['plane_of_array_irradiance'] = _calculate_poa_irradiance(
        temp_df, pv_site
    )

    # Add PV output power as the last column
    if 'power' in df.columns:
        processed_df['power'] = df['power']

    return processed_df


def _calculate_poa_irradiance(df: pd.DataFrame, pv_site: PVSite) -> pd.Series:
    """
    Calculate plane-of-array irradiance for each row in the dataframe.
    
    Uses pvlib to calculate POA based on:
    - DNI (direct_normal_irradiance)
    - DHI (diffuse_radiation)
    - Solar position
    - Panel geometries
    
    Returns a Series with POA values for each timestamp.
    """
    # Create pvlib location
    location = pvlib.location.Location(
        pv_site.location.latitude,
        pv_site.location.longitude,
        altitude=ALTITUDE,
        tz='UTC'
    )
    
    # Prepare times (subtract 30 minutes because data is right-labeled hourly intervals)
    times = df['time'] - pd.Timedelta('30min')
    times_utc = pd.DatetimeIndex(times).tz_localize('UTC')
    
    # Calculate solar position
    solar_position = location.get_solarposition(times_utc)
    apparent_zenith = solar_position['apparent_zenith']
    solar_azimuth = solar_position['azimuth']
    
    # Get DNI and DHI from dataframe
    dni = df['direct_normal_irradiance'].values
    dhi = df['diffuse_radiation'].values
    
    # Calculate GHI using the formula: GHI = DNI * cos(apparent_zenith) + DHI
    zenith_radians = np.radians(apparent_zenith)
    ghi = dni * np.cos(zenith_radians) + dhi
    
    # Calculate POA for each panel geometry and sum weighted by area_fraction
    poa_total = np.zeros(len(df))
    
    for panel_geom in pv_site.panel_geometries:
        # Calculate POA irradiance for this panel geometry
        poa_components = pvlib.irradiance.get_total_irradiance(
            surface_tilt=panel_geom.tilt,
            surface_azimuth=panel_geom.azimuth,
            dni=dni,
            ghi=ghi,
            dhi=dhi,
            solar_zenith=apparent_zenith,
            solar_azimuth=solar_azimuth,
            model='isotropic'
        )
        
        # Add weighted POA contribution
        poa_total += poa_components['poa_global'].values * panel_geom.area_fraction
    
    return pd.Series(poa_total, index=df.index)


def main() -> int:
    """Main entry point for the processing script."""
    process_timeseries_data()
    return 0


if __name__ == '__main__':
    exit(main())

