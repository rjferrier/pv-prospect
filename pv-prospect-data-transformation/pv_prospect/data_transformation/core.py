"""Shared step implementations for the data transformation pipeline.

Both the local runner and the Cloud Run entrypoint delegate to these functions.
All parameters are explicit — no environment variable reads.
"""

import io

import pandas as pd

from pv_prospect.common import get_pv_site_by_system_id
from pv_prospect.data_sources import SourceDescriptor
from pv_prospect.data_transformation.transformations import (
    clean_pv as _clean_pv_transform,
)
from pv_prospect.data_transformation.transformations import (
    clean_weather as _clean_weather_transform,
)
from pv_prospect.data_transformation.transformations import (
    prepare_pv as _prepare_pv_transform,
)
from pv_prospect.data_transformation.transformations import (
    prepare_weather as _prepare_weather_transform,
)
from pv_prospect.etl import TIMESERIES_FOLDER, Extractor
from pv_prospect.etl.storage import FileSystem

# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def read_csv(fs: FileSystem, path: str) -> pd.DataFrame | None:
    """Read a CSV file from a FileSystem, returning None if it doesn't exist."""
    if not fs.exists(path):
        return None
    return pd.read_csv(io.BytesIO(fs.read_bytes(path)), encoding='utf-8')


def read_parquet(fs: FileSystem, path: str) -> pd.DataFrame:
    """Read a Parquet file from a FileSystem."""
    return pd.read_parquet(io.BytesIO(fs.read_bytes(path)))


def write_parquet(fs: FileSystem, df: pd.DataFrame, path: str) -> None:
    """Write a DataFrame as Parquet to a FileSystem."""
    buf = io.BytesIO()
    df.to_parquet(buf, engine='pyarrow', index=False)
    fs.write_bytes(path, buf.getvalue())
    print(f'    Written to: {path}')


def _csv_to_parquet_path(path: str) -> str:
    return path.replace('.csv', '.parquet')


# ---------------------------------------------------------------------------
# Step implementations
# ---------------------------------------------------------------------------


def run_clean_weather(
    raw_fs: FileSystem,
    cleaned_fs: FileSystem,
    weather_descriptor: SourceDescriptor,
    date_str: str,
) -> None:
    """Clean raw weather CSVs for a given date into cleaned Parquet."""
    raw_extractor = Extractor(raw_fs)
    weather_prefix = f'{TIMESERIES_FOLDER}/{weather_descriptor}'
    for entry in raw_extractor.list_files(
        weather_prefix, pattern='*.csv', recursive=True
    ):
        if date_str not in entry.name:
            continue
        print(f'    [clean_weather] Processing {entry.path}')
        df = read_csv(raw_fs, entry.path)
        if df is not None and not df.empty:
            write_parquet(
                cleaned_fs,
                _clean_weather_transform(df),
                _csv_to_parquet_path(entry.path),
            )


def run_clean_pv(
    raw_fs: FileSystem,
    cleaned_fs: FileSystem,
    pv_descriptor: SourceDescriptor,
    pv_system_id: int,
    date_str: str,
) -> None:
    """Clean a raw PV CSV for a given system and date into cleaned Parquet."""
    pv_prefix = f'{TIMESERIES_FOLDER}/{pv_descriptor}/{pv_system_id}'
    in_path = f'{pv_prefix}/pvoutput_{pv_system_id}_{date_str}.csv'
    print(f'    [clean_pv] Processing {in_path}')
    df = read_csv(raw_fs, in_path)
    if df is not None and not df.empty:
        write_parquet(
            cleaned_fs,
            _clean_pv_transform(df),
            _csv_to_parquet_path(in_path),
        )


def run_prepare_weather(
    cleaned_fs: FileSystem,
    prepared_fs: FileSystem,
    weather_descriptor: SourceDescriptor,
    date_str: str,
) -> None:
    """Prepare cleaned weather Parquet for a given date into prepared Parquet."""
    cleaned_extractor = Extractor(cleaned_fs)
    cleaned_weather_prefix = f'{TIMESERIES_FOLDER}/{weather_descriptor}'
    for entry in cleaned_extractor.list_files(
        cleaned_weather_prefix, pattern='*.parquet', recursive=True
    ):
        if date_str not in entry.name:
            continue
        print(f'    [prepare_weather] Processing {entry.path}')
        cleaned_df = read_parquet(cleaned_fs, entry.path)
        write_parquet(prepared_fs, _prepare_weather_transform(cleaned_df), entry.path)


def run_prepare_pv(
    cleaned_fs: FileSystem,
    prepared_fs: FileSystem,
    pv_descriptor: SourceDescriptor,
    weather_descriptor: SourceDescriptor,
    pv_system_id: int,
    date_str: str,
) -> None:
    """Join cleaned PV and weather data for a given system and date."""
    pv_site = get_pv_site_by_system_id(pv_system_id)

    cleaned_pv_prefix = f'{TIMESERIES_FOLDER}/{pv_descriptor}/{pv_system_id}'
    in_pv_path = f'{cleaned_pv_prefix}/pvoutput_{pv_system_id}_{date_str}.parquet'
    if not cleaned_fs.exists(in_pv_path):
        print(f'    [prepare_pv] Cleaned PV data not found: {in_pv_path}')
        return

    cleaned_weather_prefix = f'{TIMESERIES_FOLDER}/{weather_descriptor}'
    cleaned_extractor = Extractor(cleaned_fs)
    weather_entry = next(
        (
            e
            for e in cleaned_extractor.list_files(
                cleaned_weather_prefix, pattern='*.parquet', recursive=True
            )
            if date_str in e.name
        ),
        None,
    )
    if not weather_entry:
        print(f'    [prepare_pv] Cleaned weather data not found for date {date_str}')
        return

    print(f'    [prepare_pv] Joining weather={weather_entry.path} with pv={in_pv_path}')
    prepared_df = _prepare_pv_transform(
        weather_df=read_parquet(cleaned_fs, weather_entry.path),
        pv_df=read_parquet(cleaned_fs, in_pv_path),
        pv_site=pv_site,
    )
    out_path = (
        f'{TIMESERIES_FOLDER}/{pv_descriptor}/'
        f'{pv_system_id}/prepared_pv_{pv_system_id}_{date_str}.parquet'
    )
    write_parquet(prepared_fs, prepared_df, out_path)
