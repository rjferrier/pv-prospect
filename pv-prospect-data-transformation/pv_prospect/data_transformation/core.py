"""Shared step implementations for the data transformation pipeline.

Both the local runner and the Cloud Run entrypoint delegate to these functions.
All parameters are explicit — no environment variable reads.
"""

import io

import pandas as pd

from pv_prospect.common import get_pv_site_by_system_id
from pv_prospect.data_sources import (
    OpenMeteoTimeSeriesDescriptor,
    PVOutputTimeSeriesDescriptor,
    SourceDescriptor,
)
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
from pv_prospect.etl import TIMESERIES_FOLDER
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


def _build_path(
    source: SourceDescriptor,
    ts_desc: OpenMeteoTimeSeriesDescriptor | PVOutputTimeSeriesDescriptor,
    date_str: str,
    ext: str,
) -> str:
    """Build a file path mirroring build_csv_file_path but taking a date string."""
    source_str = str(source)
    ts_str = str(ts_desc)
    filename = f'{source_str.replace("/", "-")}_{ts_str}_{date_str}.{ext}'
    return f'{TIMESERIES_FOLDER}/{source_str}/{ts_str}/{filename}'


# ---------------------------------------------------------------------------
# Step implementations
# ---------------------------------------------------------------------------


def run_clean_weather(
    raw_fs: FileSystem,
    cleaned_fs: FileSystem,
    weather_descriptor: SourceDescriptor,
    location: OpenMeteoTimeSeriesDescriptor,
    date_str: str,
) -> None:
    """Clean a raw weather CSV for a given location and date into cleaned Parquet."""
    in_path = _build_path(weather_descriptor, location, date_str, 'csv')
    print(f'    [clean_weather] Processing {in_path}')
    df = read_csv(raw_fs, in_path)
    if df is None:
        raise FileNotFoundError(f'CSV not found: {in_path}')
    if df.empty:
        raise ValueError(f'CSV is empty: {in_path}')
    write_parquet(
        cleaned_fs,
        _clean_weather_transform(df),
        _build_path(weather_descriptor, location, date_str, 'parquet'),
    )


def run_clean_pv(
    raw_fs: FileSystem,
    cleaned_fs: FileSystem,
    pv_descriptor: SourceDescriptor,
    pv_ts: PVOutputTimeSeriesDescriptor,
    date_str: str,
) -> None:
    """Clean a raw PV CSV for a given system and date into cleaned Parquet."""
    in_path = _build_path(pv_descriptor, pv_ts, date_str, 'csv')
    print(f'    [clean_pv] Processing {in_path}')
    df = read_csv(raw_fs, in_path)
    if df is None:
        raise FileNotFoundError(f'CSV not found: {in_path}')
    if df.empty:
        raise ValueError(f'CSV is empty: {in_path}')
    write_parquet(
        cleaned_fs,
        _clean_pv_transform(df),
        _build_path(pv_descriptor, pv_ts, date_str, 'parquet'),
    )


def run_prepare_weather(
    cleaned_fs: FileSystem,
    prepared_fs: FileSystem,
    weather_descriptor: SourceDescriptor,
    location: OpenMeteoTimeSeriesDescriptor,
    date_str: str,
) -> None:
    """Prepare cleaned weather Parquet for a given location and date."""
    path = _build_path(weather_descriptor, location, date_str, 'parquet')
    print(f'    [prepare_weather] Processing {path}')
    cleaned_df = read_parquet(cleaned_fs, path)
    write_parquet(prepared_fs, _prepare_weather_transform(cleaned_df), path)


def run_prepare_pv(
    cleaned_fs: FileSystem,
    prepared_fs: FileSystem,
    pv_descriptor: SourceDescriptor,
    weather_descriptor: SourceDescriptor,
    pv_ts: PVOutputTimeSeriesDescriptor,
    weather_location: OpenMeteoTimeSeriesDescriptor,
    date_str: str,
) -> None:
    """Join cleaned PV and weather data for a given system and date."""
    pv_site = get_pv_site_by_system_id(pv_ts.pv_system_id)

    in_pv_path = _build_path(pv_descriptor, pv_ts, date_str, 'parquet')
    if not cleaned_fs.exists(in_pv_path):
        print(f'    [prepare_pv] Cleaned PV data not found: {in_pv_path}')
        return

    weather_path = _build_path(
        weather_descriptor, weather_location, date_str, 'parquet'
    )
    if not cleaned_fs.exists(weather_path):
        print(f'    [prepare_pv] Cleaned weather data not found: {weather_path}')
        return

    print(f'    [prepare_pv] Joining weather={weather_path} with pv={in_pv_path}')
    prepared_df = _prepare_pv_transform(
        weather_df=read_parquet(cleaned_fs, weather_path),
        pv_df=read_parquet(cleaned_fs, in_pv_path),
        pv_site=pv_site,
    )
    out_path = _build_path(pv_descriptor, pv_ts, date_str, 'parquet')
    write_parquet(prepared_fs, prepared_df, out_path)
