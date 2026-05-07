"""Shared step implementations for the data transformation pipeline.

Both the local runner and the Cloud Run entrypoint delegate to these functions.
All parameters are explicit — no environment variable reads.
"""

import io
import json
import logging
from typing import Any, Callable

import pandas as pd

from pv_prospect.common.domain import AnySite, DateRange, Period, PVSite
from pv_prospect.data_sources import (
    DataSource,
    build_time_series_csv_file_path,
    csv_path_to_metadata_path,
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
    'elevation',
    'time',
    'temperature',
    'direct_normal_irradiance',
    'diffuse_radiation',
]
PV_COLUMNS = ['time', 'temperature', 'plane_of_array_irradiance', 'power']

# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def read_csv(fs: FileSystem, path: str) -> pd.DataFrame:
    """Read a CSV file from a FileSystem, returning None if it doesn't exist."""
    if not fs.exists(path):
        raise FileNotFoundError(f'CSV not found in {fs}/{path}')
    return pd.read_csv(io.BytesIO(fs.read_bytes(path)), encoding='utf-8')


def write_csv(fs: FileSystem, df: pd.DataFrame, path: str, header: bool = True) -> None:
    """Write a DataFrame as CSV to a FileSystem."""
    fs.write_text(path, df.to_csv(index=False, header=header))
    logger.info('Written to: %s', path)


def read_metadata(fs: FileSystem, csv_path: str) -> dict[str, Any]:
    """Read the metadata JSON companion to a CSV file."""
    meta_path = csv_path_to_metadata_path(csv_path)
    return json.loads(fs.read_text(meta_path))


def write_metadata(fs: FileSystem, csv_path: str, metadata: dict[str, Any]) -> None:
    """Write a metadata JSON companion alongside a CSV file."""
    meta_path = csv_path_to_metadata_path(csv_path)
    fs.write_text(meta_path, json.dumps(metadata))


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
    weather_data_source: DataSource,
    site: AnySite,
    date_range: DateRange,
    by_week: bool = False,
) -> None:
    """Clean raw weather CSVs for a date range.

    Splits *date_range* by ``Period.WEEK`` (when *by_week*) or
    ``Period.DAY``.  Each chunk reads one raw file keyed by the chunk's
    start date (matching the extraction naming convention) and writes one
    cleaned CSV per day.
    """
    split_period = Period.WEEK if by_week else Period.DAY
    for chunk in date_range.split_by(split_period):
        _clean_weather_chunk(raw_fs, cleaned_fs, weather_data_source, site, chunk)


def _clean_weather_chunk(
    raw_fs: FileSystem,
    cleaned_fs: FileSystem,
    weather_data_source: DataSource,
    site: AnySite,
    date_range: DateRange,
) -> None:
    """Clean a single raw weather file and write per-day cleaned CSVs."""
    in_path = build_time_series_csv_file_path(
        TIMESERIES_FOLDER, weather_data_source, site, date_range
    )
    logger.info('[clean_weather] Processing %s', in_path)
    df = read_csv(raw_fs, in_path)
    if df.empty:
        raise ValueError(f'CSV is empty: {in_path}')
    cleaned = _clean_weather_transform(df)
    metadata = read_metadata(raw_fs, in_path)

    for day_range in date_range.split_by(Period.DAY):
        day_df = cleaned[cleaned['time'].dt.date == day_range.start]
        if day_df.empty:
            logger.warning(
                '[clean_weather] No data for %s in %s',
                day_range.start.strftime('%Y%m%d'),
                in_path,
            )
            continue
        out_path = build_time_series_csv_file_path(
            TIMESERIES_FOLDER,
            weather_data_source,
            site,
            day_range,
        )
        write_csv(cleaned_fs, day_df, out_path)
        write_metadata(cleaned_fs, out_path, metadata)


def run_clean_pv(
    raw_fs: FileSystem,
    cleaned_fs: FileSystem,
    pv_data_source: DataSource,
    pv_site: PVSite,
    date_range: DateRange,
) -> None:
    """Clean raw PV CSVs for a date range (one file per day)."""
    for day_range in date_range.split_by(Period.DAY):
        path = build_time_series_csv_file_path(
            TIMESERIES_FOLDER, pv_data_source, pv_site, day_range
        )
        logger.info('[clean_pv] Processing %s', path)
        df = read_csv(raw_fs, path)
        if df.empty:
            raise ValueError(f'CSV is empty: {path}')
        write_csv(
            cleaned_fs,
            _clean_pv_transform(df),
            path,
        )


def run_prepare_weather(
    cleaned_fs: FileSystem,
    batches_fs: FileSystem,
    weather_data_source: DataSource,
    site: AnySite,
    date_range: DateRange,
) -> None:
    """Prepare cleaned weather CSVs for a date range into batch files."""
    for day_range in date_range.split_by(Period.DAY):
        date_str = day_range.start.strftime('%Y%m%d')
        path = build_time_series_csv_file_path(
            TIMESERIES_FOLDER,
            weather_data_source,
            site,
            day_range,
        )
        logger.info('[prepare_weather] Processing %s', path)
        metadata = read_metadata(cleaned_fs, path)
        cleaned_df = read_csv(cleaned_fs, path)
        prepared_df = _prepare_weather_transform(cleaned_df)
        prepared_df.insert(0, 'latitude', metadata['latitude'])
        prepared_df.insert(1, 'longitude', metadata['longitude'])
        prepared_df.insert(2, 'elevation', metadata['elevation'])
        write_csv(
            batches_fs,
            prepared_df,
            _weather_batch_path(
                site.location.to_coordinate_string(filename_friendly=True),
                date_str,
            ),
        )


def run_prepare_pv(
    cleaned_fs: FileSystem,
    batches_fs: FileSystem,
    pv_data_source: DataSource,
    weather_data_source: DataSource,
    pv_site: PVSite,
    date_range: DateRange,
    get_pv_site: Callable[[int], PVSite],
) -> None:
    """Join cleaned PV and weather data for a date range into batch files."""
    pv_site_full = get_pv_site(pv_site.pvo_sys_id)

    for day_range in date_range.split_by(Period.DAY):
        date_str = day_range.start.strftime('%Y%m%d')
        in_pv_path = build_time_series_csv_file_path(
            TIMESERIES_FOLDER, pv_data_source, pv_site, day_range
        )
        if not cleaned_fs.exists(in_pv_path):
            logger.warning('[prepare_pv] Cleaned PV data not found: %s', in_pv_path)
            continue

        weather_path = build_time_series_csv_file_path(
            TIMESERIES_FOLDER,
            weather_data_source,
            pv_site,
            day_range,
        )
        if not cleaned_fs.exists(weather_path):
            logger.warning(
                '[prepare_pv] Cleaned weather data not found: %s', weather_path
            )
            continue

        logger.info(
            '[prepare_pv] Joining weather=%s with pv=%s', weather_path, in_pv_path
        )
        pv_df = read_csv(cleaned_fs, in_pv_path)
        weather_df = read_csv(cleaned_fs, weather_path)
        prepared_df = _prepare_pv_transform(
            weather_df=weather_df,
            pv_df=pv_df,
            pv_site=pv_site_full,
        )
        write_csv(
            batches_fs,
            prepared_df,
            _pv_batch_path(pv_site.pvo_sys_id, date_str),
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

    try:
        existing = read_csv(prepared_fs, PREPARED_WEATHER_PATH)
        frames.append(existing)
    except FileNotFoundError:
        pass

    for entry in batch_files:
        content = batches_fs.read_text(entry.path)
        df = pd.read_csv(io.StringIO(content))
        if list(df.columns) != WEATHER_COLUMNS:
            raise ValueError(
                f'Batch {entry.path} has columns {list(df.columns)!r},'
                f' expected {WEATHER_COLUMNS!r}'
            )
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
    try:
        existing = read_csv(prepared_fs, master_path)
        frames.append(existing)
    except FileNotFoundError:
        pass

    for entry in batch_files:
        content = batches_fs.read_text(entry.path)
        df = pd.read_csv(io.StringIO(content))
        if list(df.columns) != PV_COLUMNS:
            raise ValueError(
                f'Batch {entry.path} has columns {list(df.columns)!r},'
                f' expected {PV_COLUMNS!r}'
            )
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=['time'], keep='last')
    combined = combined.sort_values('time').reset_index(drop=True)
    write_csv(prepared_fs, combined, master_path)

    for entry in batch_files:
        batches_fs.delete(entry.path)
