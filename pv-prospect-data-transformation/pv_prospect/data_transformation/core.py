"""Shared step implementations for the data transformation pipeline.

Both the local runner and the Cloud Run entrypoint delegate to these functions.
All parameters are explicit — no environment variable reads.
"""

import io
import logging

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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------

PREPARED_WEATHER_PATH = 'weather.csv'
WEATHER_BATCH_PREFIX = 'weather'
PV_BATCH_PREFIX = 'pv'

WEATHER_COLUMNS = [
    'latitude',
    'longitude',
    'time',
    'temperature',
    'direct_normal_irradiance',
    'diffuse_radiation',
]
PV_COLUMNS = ['time', 'temperature', 'plane_of_array_irradiance', 'power']

# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def read_csv(fs: FileSystem, path: str) -> pd.DataFrame | None:
    """Read a CSV file from a FileSystem, returning None if it doesn't exist."""
    if not fs.exists(path):
        return None
    return pd.read_csv(io.BytesIO(fs.read_bytes(path)), encoding='utf-8')


def write_csv(fs: FileSystem, df: pd.DataFrame, path: str, header: bool = True) -> None:
    """Write a DataFrame as CSV to a FileSystem."""
    fs.write_text(path, df.to_csv(index=False, header=header))
    logger.info('Written to: %s', path)


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


def _prepared_pv_path(system_id: int) -> str:
    return f'{PV_BATCH_PREFIX}/{system_id}.csv'


def _weather_batch_path(location_id: str, date_str: str) -> str:
    return f'{WEATHER_BATCH_PREFIX}/{location_id}_{date_str}.csv'


def _pv_batch_path(system_id: int, date_str: str) -> str:
    return f'{PV_BATCH_PREFIX}/{system_id}_{date_str}.csv'


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
    """Clean a raw weather CSV for a given location and date into cleaned CSV."""
    in_path = _build_path(weather_descriptor, location, date_str, 'csv')
    logger.info('[clean_weather] Processing %s', in_path)
    df = read_csv(raw_fs, in_path)
    if df is None:
        raise FileNotFoundError(f'CSV not found: {in_path}')
    if df.empty:
        raise ValueError(f'CSV is empty: {in_path}')
    write_csv(
        cleaned_fs,
        _clean_weather_transform(df),
        _build_path(weather_descriptor, location, date_str, 'csv'),
    )


def run_clean_pv(
    raw_fs: FileSystem,
    cleaned_fs: FileSystem,
    pv_descriptor: SourceDescriptor,
    pv_ts: PVOutputTimeSeriesDescriptor,
    date_str: str,
) -> None:
    """Clean a raw PV CSV for a given system and date into cleaned CSV."""
    in_path = _build_path(pv_descriptor, pv_ts, date_str, 'csv')
    logger.info('[clean_pv] Processing %s', in_path)
    df = read_csv(raw_fs, in_path)
    if df is None:
        raise FileNotFoundError(f'CSV not found: {in_path}')
    if df.empty:
        raise ValueError(f'CSV is empty: {in_path}')
    write_csv(
        cleaned_fs,
        _clean_pv_transform(df),
        _build_path(pv_descriptor, pv_ts, date_str, 'csv'),
    )


def run_prepare_weather(
    cleaned_fs: FileSystem,
    batches_fs: FileSystem,
    weather_descriptor: SourceDescriptor,
    location: OpenMeteoTimeSeriesDescriptor,
    date_str: str,
) -> None:
    """Prepare cleaned weather CSV and write a headerless CSV batch."""
    path = _build_path(weather_descriptor, location, date_str, 'csv')
    logger.info('[prepare_weather] Processing %s', path)
    cleaned_df = read_csv(cleaned_fs, path)
    if cleaned_df is None:
        raise FileNotFoundError(f'CSV not found: {path}')
    prepared_df = _prepare_weather_transform(cleaned_df)
    prepared_df.insert(0, 'latitude', float(location.latitude))
    prepared_df.insert(1, 'longitude', float(location.longitude))
    write_csv(
        batches_fs,
        prepared_df,
        _weather_batch_path(location.location_id, date_str),
        header=False,
    )


def run_prepare_pv(
    cleaned_fs: FileSystem,
    batches_fs: FileSystem,
    pv_descriptor: SourceDescriptor,
    weather_descriptor: SourceDescriptor,
    pv_ts: PVOutputTimeSeriesDescriptor,
    weather_location: OpenMeteoTimeSeriesDescriptor,
    date_str: str,
) -> None:
    """Join cleaned PV and weather data and write a headerless CSV batch."""
    pv_site = get_pv_site_by_system_id(pv_ts.pv_system_id)

    in_pv_path = _build_path(pv_descriptor, pv_ts, date_str, 'csv')
    if not cleaned_fs.exists(in_pv_path):
        logger.warning('[prepare_pv] Cleaned PV data not found: %s', in_pv_path)
        return

    weather_path = _build_path(weather_descriptor, weather_location, date_str, 'csv')
    if not cleaned_fs.exists(weather_path):
        logger.warning('[prepare_pv] Cleaned weather data not found: %s', weather_path)
        return

    logger.info('[prepare_pv] Joining weather=%s with pv=%s', weather_path, in_pv_path)
    pv_df = read_csv(cleaned_fs, in_pv_path)
    weather_df = read_csv(cleaned_fs, weather_path)
    if pv_df is None or weather_df is None:
        return
    prepared_df = _prepare_pv_transform(
        weather_df=weather_df,
        pv_df=pv_df,
        pv_site=pv_site,
    )
    write_csv(
        batches_fs,
        prepared_df,
        _pv_batch_path(pv_ts.pv_system_id, date_str),
        header=False,
    )


# ---------------------------------------------------------------------------
# Assembly functions
# ---------------------------------------------------------------------------


def assemble_prepared_weather(
    batches_fs: FileSystem,
    prepared_fs: FileSystem,
) -> None:
    """Merge weather batch CSVs into a single master CSV."""
    batch_files = batches_fs.list_files(WEATHER_BATCH_PREFIX, '*.csv')
    if not batch_files:
        logger.warning('[assemble_weather] No batches to assemble.')
        return

    frames: list[pd.DataFrame] = []

    existing = read_csv(prepared_fs, PREPARED_WEATHER_PATH)
    if existing is not None:
        frames.append(existing)

    for entry in batch_files:
        content = batches_fs.read_text(entry.path)
        df = pd.read_csv(io.StringIO(content), names=WEATHER_COLUMNS, header=None)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(
        subset=['latitude', 'longitude', 'time'], keep='last'
    )
    combined = combined.sort_values(['latitude', 'longitude', 'time']).reset_index(
        drop=True
    )
    write_csv(prepared_fs, combined, PREPARED_WEATHER_PATH)

    for entry in batch_files:
        batches_fs.delete(entry.path)


def assemble_prepared_pv(
    batches_fs: FileSystem,
    prepared_fs: FileSystem,
    system_id: int,
) -> None:
    """Merge PV batch CSVs for a single system into a master CSV."""
    all_pv_batches = batches_fs.list_files(PV_BATCH_PREFIX, '*.csv')
    batch_files = [e for e in all_pv_batches if e.name.startswith(f'{system_id}_')]
    if not batch_files:
        logger.warning('[assemble_pv] No batches for system %s.', system_id)
        return

    frames: list[pd.DataFrame] = []

    master_path = _prepared_pv_path(system_id)
    existing = read_csv(prepared_fs, master_path)
    if existing is not None:
        frames.append(existing)

    for entry in batch_files:
        content = batches_fs.read_text(entry.path)
        df = pd.read_csv(io.StringIO(content), names=PV_COLUMNS, header=None)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=['time'], keep='last')
    combined = combined.sort_values('time').reset_index(drop=True)
    write_csv(prepared_fs, combined, master_path)

    for entry in batch_files:
        batches_fs.delete(entry.path)
