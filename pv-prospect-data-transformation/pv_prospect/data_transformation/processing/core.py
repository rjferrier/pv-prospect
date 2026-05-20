"""Shared step implementations for the data transformation pipeline.

Both the local runner and the Cloud Run entrypoint delegate to these functions.
All parameters are explicit — no environment variable reads.
"""

import io
import json
import logging
import threading
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
from pv_prospect.etl.storage import FileEntry, FileSystem

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
    logger.debug('Written to: %s', path)


def _read_batch_csv(
    fs: FileSystem, path: str, expected_columns: list[str]
) -> pd.DataFrame:
    """Read one prepared-batch CSV, validating its column layout."""
    df = pd.read_csv(io.StringIO(fs.read_text(path)))
    if list(df.columns) != expected_columns:
        raise ValueError(
            f'Batch {path} has columns {list(df.columns)!r},'
            f' expected {expected_columns!r}'
        )
    return df


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
# Prepared-batch collector
# ---------------------------------------------------------------------------


class PreparedBatchCollector:
    """In-memory accumulator of prepared DataFrames for a single-process run.

    Replaces the prepared-batch CSV files as the ``prepare`` → ``assemble``
    handoff when the whole transform runs in one process (the backfill).
    ``prepare_*`` adds a frame; ``assemble_*`` drains the matching frames
    and merges them — so there is no per-batch GCS write, list, read, or
    delete. The distributed daily transform leaves the collector unset and
    keeps the batch CSVs, which are its only cross-process handoff.

    Thread-safe: ``prepare`` fans its units across a thread pool.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._weather: list[pd.DataFrame] = []
        self._pv: dict[int, list[pd.DataFrame]] = {}

    def add_weather(self, df: pd.DataFrame) -> None:
        """Buffer one prepared weather frame. Thread-safe."""
        with self._lock:
            self._weather.append(df)

    def add_pv(self, system_id: int, df: pd.DataFrame) -> None:
        """Buffer one prepared PV frame for *system_id*. Thread-safe."""
        with self._lock:
            self._pv.setdefault(system_id, []).append(df)

    def weather_frames(self) -> list[pd.DataFrame]:
        """Return the buffered prepared weather frames."""
        with self._lock:
            return list(self._weather)

    def pv_frames(self, system_id: int) -> list[pd.DataFrame]:
        """Return the buffered prepared PV frames for *system_id*."""
        with self._lock:
            return list(self._pv.get(system_id, []))


# ---------------------------------------------------------------------------
# Step implementations
# ---------------------------------------------------------------------------


def run_clean_weather(
    raw_fs: FileSystem,
    cleaned_fs: FileSystem,
    weather_data_source: DataSource,
    site: AnySite,
    date_range: DateRange,
) -> None:
    """Clean the raw weather file covering *date_range* into per-day CSVs.

    The raw file is read as a single unit; its name is derived from the
    full *date_range*, so this transparently handles both the daily
    pipeline's single-date files (``..._YYYYMMDD.csv``) and the backfill's
    window-spanning range files (``..._YYYYMMDD_YYYYMMDD.csv``). One
    cleaned CSV (plus its metadata companion) is written per day in the
    range.
    """
    in_path = build_time_series_csv_file_path(
        TIMESERIES_FOLDER, weather_data_source, site, date_range
    )
    logger.debug('[clean_weather] Processing %s', in_path)
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
    """Clean raw PV CSVs for a date range (one file per day).

    Empty per-day CSVs are skipped with a warning, mirroring
    :func:`run_clean_weather`'s behaviour for no-data days within a
    window. The PVOutput extractor records the API call as ``completed``
    even when the system returned no readings (offline, newly-installed,
    decommissioned, ...), so the empty file is the truth about that day
    — not a corruption signal — and shouldn't fail the whole window.
    """
    for day_range in date_range.split_by(Period.DAY):
        path = build_time_series_csv_file_path(
            TIMESERIES_FOLDER, pv_data_source, pv_site, day_range
        )
        logger.debug('[clean_pv] Processing %s', path)
        df = read_csv(raw_fs, path)
        if df.empty:
            logger.warning(
                '[clean_pv] No data for %s in %s',
                day_range.start.strftime('%Y%m%d'),
                path,
            )
            continue
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
    collector: PreparedBatchCollector | None = None,
) -> None:
    """Prepare cleaned weather CSVs for a date range.

    Each day's prepared frame is written as a batch CSV to *batches_fs*.
    When a *collector* is supplied (the single-process backfill), the
    unit's days are buffered in memory as one concatenated frame instead,
    so :func:`assemble_prepared_weather` can merge them without the
    per-batch GCS round-trip.
    """
    day_frames: list[pd.DataFrame] = []
    for day_range in date_range.split_by(Period.DAY):
        date_str = day_range.start.strftime('%Y%m%d')
        path = build_time_series_csv_file_path(
            TIMESERIES_FOLDER,
            weather_data_source,
            site,
            day_range,
        )
        logger.debug('[prepare_weather] Processing %s', path)
        metadata = read_metadata(cleaned_fs, path)
        cleaned_df = read_csv(cleaned_fs, path)
        prepared_df = _prepare_weather_transform(cleaned_df)
        prepared_df.insert(0, 'latitude', metadata['latitude'])
        prepared_df.insert(1, 'longitude', metadata['longitude'])
        prepared_df.insert(2, 'elevation', metadata['elevation'])
        if collector is not None:
            day_frames.append(prepared_df)
        else:
            write_csv(
                batches_fs,
                prepared_df,
                _weather_batch_path(
                    site.location.to_coordinate_string(filename_friendly=True),
                    date_str,
                ),
            )
    if collector is not None and day_frames:
        collector.add_weather(pd.concat(day_frames, ignore_index=True))


def run_prepare_pv(
    cleaned_fs: FileSystem,
    batches_fs: FileSystem,
    pv_data_source: DataSource,
    weather_data_source: DataSource,
    pv_site: PVSite,
    date_range: DateRange,
    get_pv_site: Callable[[int], PVSite],
    collector: PreparedBatchCollector | None = None,
) -> None:
    """Join cleaned PV and weather data for a date range.

    Each day's joined frame is written as a batch CSV to *batches_fs*.
    When a *collector* is supplied it is buffered in memory as one
    concatenated frame instead, for :func:`assemble_prepared_pv` to merge.
    """
    pv_site_full = get_pv_site(pv_site.pvo_sys_id)
    day_frames: list[pd.DataFrame] = []

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

        logger.debug(
            '[prepare_pv] Joining weather=%s with pv=%s', weather_path, in_pv_path
        )
        pv_df = read_csv(cleaned_fs, in_pv_path)
        weather_df = read_csv(cleaned_fs, weather_path)
        prepared_df = _prepare_pv_transform(
            weather_df=weather_df,
            pv_df=pv_df,
            pv_site=pv_site_full,
        )
        if collector is not None:
            day_frames.append(prepared_df)
        else:
            write_csv(
                batches_fs,
                prepared_df,
                _pv_batch_path(pv_site.pvo_sys_id, date_str),
            )
    if collector is not None and day_frames:
        collector.add_pv(pv_site.pvo_sys_id, pd.concat(day_frames, ignore_index=True))


# ---------------------------------------------------------------------------
# Assembly functions
# ---------------------------------------------------------------------------


def assemble_prepared_weather(
    batches_fs: FileSystem,
    prepared_fs: FileSystem,
    collector: PreparedBatchCollector | None = None,
) -> None:
    """Merge prepared weather frames into the single master CSV.

    The batch frames come from the in-memory *collector* when supplied
    (the single-process backfill), otherwise from the batch CSVs on
    *batches_fs*, which are deleted once merged.
    """
    batch_files: list[FileEntry] = []
    if collector is not None:
        batch_frames = collector.weather_frames()
    else:
        batch_files = batches_fs.list_files(WEATHER_BATCH_PREFIX, '*.csv')
        batch_frames = [
            _read_batch_csv(batches_fs, entry.path, WEATHER_COLUMNS)
            for entry in batch_files
        ]
    if not batch_frames:
        logger.warning('[assemble_weather] No batches to assemble.')
        return

    frames: list[pd.DataFrame] = []
    try:
        frames.append(read_csv(prepared_fs, PREPARED_WEATHER_PATH))
    except FileNotFoundError:
        pass
    frames.extend(batch_frames)

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
    collector: PreparedBatchCollector | None = None,
) -> None:
    """Merge prepared PV frames for a single system into its master CSV.

    The batch frames come from the in-memory *collector* when supplied,
    otherwise from this system's batch CSVs on *batches_fs*, which are
    deleted once merged.
    """
    batch_files: list[FileEntry] = []
    if collector is not None:
        batch_frames = collector.pv_frames(system_id)
    else:
        all_pv_batches = batches_fs.list_files(PV_BATCH_PREFIX, '*.csv')
        batch_files = [e for e in all_pv_batches if e.name.startswith(f'{system_id}_')]
        batch_frames = [
            _read_batch_csv(batches_fs, entry.path, PV_COLUMNS) for entry in batch_files
        ]
    if not batch_frames:
        logger.warning('[assemble_pv] No batches for system %s.', system_id)
        return

    frames: list[pd.DataFrame] = []
    master_path = _prepared_pv_path(system_id)
    try:
        frames.append(read_csv(prepared_fs, master_path))
    except FileNotFoundError:
        pass
    frames.extend(batch_frames)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=['time'], keep='last')
    combined = combined.sort_values('time').reset_index(drop=True)
    write_csv(prepared_fs, combined, master_path)

    for entry in batch_files:
        batches_fs.delete(entry.path)
