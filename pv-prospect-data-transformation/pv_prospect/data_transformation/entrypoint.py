"""Cloud Run Job entrypoint for Data Transformation.

Reads task parameters from environment variables set by the Cloud Workflow
and calls the corresponding clean or process function.

Environment variables
---------------------
TRANSFORM_STEP
    ``clean_weather``, ``clean_pvoutput``, ``process_weather``, or ``process_pv``
DATE
    ISO date ``YYYY-MM-DD`` to process
PV_SYSTEM_ID
    (Optional) integer system id, required for pv steps
"""

import io
import os
import sys

import pandas as pd

from pv_prospect.common import (
    build_location_mapping_repo,
    build_pv_site_repo,
    get_config,
    get_pv_site_by_system_id,
)
from pv_prospect.data_sources import SourceDescriptor
from pv_prospect.data_transformation.config import DataTransformationConfig
from pv_prospect.data_transformation.transformations import (
    clean_pvoutput,
    clean_weather,
    process_pv,
    process_weather,
)
from pv_prospect.etl import TIMESERIES_FOLDER, Extractor, get_config_dir
from pv_prospect.etl.storage import FileSystem, get_filesystem

CLEANED_PREFIX = 'cleaned'
PROCESSED_PREFIX = 'processed'


def _load_resources(raw_fs: FileSystem) -> None:
    """Load the PV site and location mapping repos from the raw data bucket."""
    extractor = Extractor(raw_fs)

    if extractor.file_exists('pv_sites.csv'):
        build_pv_site_repo(extractor.read_file('pv_sites.csv'))

    if extractor.file_exists('location_mapping.csv'):
        build_location_mapping_repo(extractor.read_file('location_mapping.csv'))


def _read_csv(fs: FileSystem, path: str) -> pd.DataFrame | None:
    """Read a CSV file from a FileSystem into a DataFrame."""
    if not fs.exists(path):
        return None
    data = fs.read_bytes(path)
    return pd.read_csv(io.BytesIO(data), encoding='utf-8')


def _read_parquet(fs: FileSystem, path: str) -> pd.DataFrame:
    """Read a Parquet file from a FileSystem into a DataFrame."""
    data = fs.read_bytes(path)
    return pd.read_parquet(io.BytesIO(data))


def _write_parquet(fs: FileSystem, df: pd.DataFrame, path: str) -> None:
    """Write a DataFrame as Parquet to a FileSystem."""
    buf = io.BytesIO()
    df.to_parquet(buf, engine='pyarrow', index=False)
    fs.write_bytes(path, buf.getvalue())
    print(f'    Written to: {path}')


def _staging_to_cleaned(path: str) -> str:
    """Convert a raw timeseries path to the corresponding cleaned path."""
    return f'{CLEANED_PREFIX}/{path}'.replace('.csv', '.parquet')


def _clean_weather(raw_fs: FileSystem, date_str: str) -> None:
    _load_resources(raw_fs)
    raw_extractor = Extractor(raw_fs)
    weather_prefix = f'{TIMESERIES_FOLDER}/{SourceDescriptor.OPENMETEO_HISTORICAL}'
    entries = raw_extractor.list_files(weather_prefix, pattern='*.csv')
    for entry in entries:
        if date_str not in entry.name:
            continue
        blob_path = entry.path
        print(f'[clean_weather] Processing {blob_path}')
        df = _read_csv(raw_fs, blob_path)
        if df is not None and not df.empty:
            cleaned_df = clean_weather(df)
            out_path = _staging_to_cleaned(blob_path)
            _write_parquet(raw_fs, cleaned_df, out_path)


def _clean_pvoutput(raw_fs: FileSystem, date_str: str) -> None:
    pv_system_id = os.environ['PV_SYSTEM_ID']
    pv_prefix = f'{TIMESERIES_FOLDER}/{SourceDescriptor.PVOUTPUT}/{pv_system_id}'
    in_path = f'{pv_prefix}/pvoutput_{pv_system_id}_{date_str}.csv'
    print(f'[clean_pvoutput] Processing {in_path}')
    df = _read_csv(raw_fs, in_path)
    if df is not None and not df.empty:
        cleaned_df = clean_pvoutput(df)
        out_path = _staging_to_cleaned(in_path)
        _write_parquet(raw_fs, cleaned_df, out_path)


def _process_weather(raw_fs: FileSystem, model_fs: FileSystem, date_str: str) -> None:
    raw_extractor = Extractor(raw_fs)
    cleaned_weather_prefix = (
        f'{CLEANED_PREFIX}/{TIMESERIES_FOLDER}/'
        f'{SourceDescriptor.OPENMETEO_QUARTERHOURLY}'
    )
    entries = raw_extractor.list_files(cleaned_weather_prefix, pattern='*.parquet')
    for entry in entries:
        if date_str not in entry.name:
            continue
        blob_path = entry.path
        print(f'[process_weather] Processing {blob_path}')
        cleaned_df = _read_parquet(raw_fs, blob_path)
        processed_df = process_weather(cleaned_df)
        out_path = (
            f'{PROCESSED_PREFIX}/{TIMESERIES_FOLDER}/'
            f'{SourceDescriptor.OPENMETEO_QUARTERHOURLY}/{entry.name}'
        )
        _write_parquet(model_fs, processed_df, out_path)


def _process_pv(raw_fs: FileSystem, model_fs: FileSystem, date_str: str) -> None:
    pv_sys_id = int(os.environ['PV_SYSTEM_ID'])
    _load_resources(raw_fs)
    pv_site = get_pv_site_by_system_id(pv_sys_id)

    cleaned_pv_prefix = (
        f'{CLEANED_PREFIX}/{TIMESERIES_FOLDER}/{SourceDescriptor.PVOUTPUT}/{pv_sys_id}'
    )
    in_pv_path = f'{cleaned_pv_prefix}/pvoutput_{pv_sys_id}_{date_str}.parquet'
    if not raw_fs.exists(in_pv_path):
        print(f'[process_pv] Cleaned PV data not found: {in_pv_path}')
        return

    cleaned_pv_df = _read_parquet(raw_fs, in_pv_path)

    cleaned_weather_prefix = (
        f'{CLEANED_PREFIX}/{TIMESERIES_FOLDER}/'
        f'{SourceDescriptor.OPENMETEO_QUARTERHOURLY}'
    )
    raw_extractor = Extractor(raw_fs)
    weather_entries = raw_extractor.list_files(
        cleaned_weather_prefix, pattern='*.parquet'
    )
    weather_entry = None
    for entry in weather_entries:
        if date_str in entry.name:
            weather_entry = entry
            break

    if not weather_entry:
        print(f'[process_pv] Cleaned weather data not found for date {date_str}')
        return

    cleaned_weather_df = _read_parquet(raw_fs, weather_entry.path)
    print(f'[process_pv] Joining weather={weather_entry.path} with pv={in_pv_path}')
    processed_df = process_pv(
        weather_df=cleaned_weather_df,
        pvoutput_df=cleaned_pv_df,
        pv_site=pv_site,
    )
    out_path = (
        f'{PROCESSED_PREFIX}/{TIMESERIES_FOLDER}/'
        f'{SourceDescriptor.PVOUTPUT}/{pv_sys_id}/'
        f'processed_pv_{pv_sys_id}_{date_str}.parquet'
    )
    _write_parquet(model_fs, processed_df, out_path)


def main() -> None:
    step = os.environ.get('TRANSFORM_STEP', '')
    target_date = os.environ['DATE']
    date_str = target_date.replace('-', '')

    config = get_config(DataTransformationConfig, base_config_dirs=[get_config_dir()])
    raw_fs = get_filesystem(config.staged_raw_data_storage)
    model_fs = get_filesystem(config.staged_model_data_storage)

    print(f'[entrypoint] Starting {step} for date {target_date}')

    if step == 'clean_weather':
        _clean_weather(raw_fs, date_str)
    elif step == 'clean_pvoutput':
        _clean_pvoutput(raw_fs, date_str)
    elif step == 'process_weather':
        _process_weather(raw_fs, model_fs, date_str)
    elif step == 'process_pv':
        _process_pv(raw_fs, model_fs, date_str)
    else:
        print(
            f'[entrypoint] ERROR: unknown TRANSFORM_STEP={step}',
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == '__main__':
    main()
