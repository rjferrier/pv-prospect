"""Shared step implementations for the data transformation pipeline.

Both the local runner and the Cloud Run entrypoint delegate to these functions.
All parameters are explicit — no environment variable reads.

Prepared output is partitioned into content-named CSV files under two
segregated corpora: ``weather/`` (grid-point weather, consumed by the
weather model) and ``pv/`` (PV power joined with on-site weather, consumed
by the pv model).
"""

import io
import json
import logging
import threading
from datetime import date, timedelta
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

# The two segregated prepared-data corpora.
WEATHER_PREPARED_PREFIX = 'weather'
PV_PREPARED_PREFIX = 'pv'

# The daily transform hands prepare -> assemble through per-day batch CSVs
# on the batches filesystem, since its steps run as separate Cloud Run
# tasks. The single-process backfill uses PreparedBatchCollector instead.
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
    """Read a CSV file from a FileSystem, raising if it doesn't exist."""
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


# ---------------------------------------------------------------------------
# Partition-file paths
# ---------------------------------------------------------------------------


def weather_partition_path(
    start: str, end: str, grid_point_sample_index: int, grid_definition_version: int
) -> str:
    """Path of the weather partition file for one (sample file, window).

    *start* (inclusive) and *end* (exclusive) are ISO ``YYYY-MM-DD``
    strings. *grid_definition_version* is encoded into the filename so a
    regridding starts a fresh, non-colliding set of files alongside the
    existing corpus (see ``weather_grid.version`` in the data-sources
    config). A weather window is produced complete by a single run, so it
    is named for its nominal extraction window — a stable identity that
    keeps a re-transform updating one file rather than orphaning others.
    """
    return (
        f'{WEATHER_PREPARED_PREFIX}/weather_{start}_{end}'
        f'_{grid_definition_version}-{grid_point_sample_index:02d}.csv'
    )


def pv_partition_path(system_id: int, start: str, end: str) -> str:
    """Path of the PV partition file covering *start*..*end* for *system_id*.

    *start* (inclusive) and *end* (exclusive) are ISO ``YYYY-MM-DD``
    strings. A PV file is named for the range it *actually* covers: it
    accumulates day by day within its ISO week, so the name is recomputed
    — and the file rewritten under the new name — on every merge.
    """
    return f'{PV_PREPARED_PREFIX}/{system_id}/pv_{system_id}_{start}_{end}.csv'


def _pv_batch_path(system_id: int, date_str: str) -> str:
    return f'{PV_BATCH_PREFIX}/{system_id}_{date_str}.csv'


def _iso_week_start(d: date) -> date:
    """Return the Monday of the ISO week containing *d*."""
    return d - timedelta(days=d.weekday())


def _parse_pv_partition_dates(name: str) -> tuple[date, date] | None:
    """Parse ``pv_{system}_{start}_{end}.csv`` into its (start, end) dates.

    Returns ``None`` for a name that does not match the pattern, so a
    stray file in the directory does not crash assembly.
    """
    stem = name.removeprefix('pv_').removesuffix('.csv')
    parts = stem.split('_')
    if len(parts) != 3:
        return None
    try:
        return date.fromisoformat(parts[1]), date.fromisoformat(parts[2])
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Prepared-batch collector
# ---------------------------------------------------------------------------


class PreparedBatchCollector:
    """In-memory accumulator of prepared DataFrames for a single-process run.

    Replaces the prepared-batch CSV files as the ``prepare`` → ``assemble``
    handoff when the whole transform runs in one process (the backfill).
    ``prepare_*`` adds a frame under its partition key; ``assemble_*``
    drains the matching groups and writes them — so there is no per-batch
    GCS write, list, read, or delete. The distributed daily transform
    leaves the collector unset and keeps the PV batch CSVs, which are its
    only cross-process handoff.

    Weather frames are grouped by ``(grid_point_sample_index, start, end)`` —
    one group per weather partition file — and PV frames by
    ``(system_id, start, end)``; ``start`` / ``end`` are the transform
    unit's ISO window strings.

    Thread-safe: ``prepare`` fans its units across a thread pool.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._weather: dict[tuple[int, str, str], list[pd.DataFrame]] = {}
        self._pv: dict[tuple[int, str, str], list[pd.DataFrame]] = {}

    def add_weather(
        self, grid_point_sample_index: int, start: str, end: str, df: pd.DataFrame
    ) -> None:
        """Buffer one prepared weather frame for a (sample, window). Thread-safe."""
        key = (grid_point_sample_index, start, end)
        with self._lock:
            self._weather.setdefault(key, []).append(df)

    def add_pv(self, system_id: int, start: str, end: str, df: pd.DataFrame) -> None:
        """Buffer one prepared PV frame for a (system, window). Thread-safe."""
        with self._lock:
            self._pv.setdefault((system_id, start, end), []).append(df)

    def weather_groups(self) -> dict[tuple[int, str, str], list[pd.DataFrame]]:
        """Return weather frames grouped by (grid_point_sample_index, start, end)."""
        with self._lock:
            return {key: list(frames) for key, frames in self._weather.items()}

    def pv_groups(self, system_id: int) -> dict[tuple[str, str], list[pd.DataFrame]]:
        """Return *system_id*'s buffered PV frames grouped by (start, end)."""
        with self._lock:
            return {
                (start, end): list(frames)
                for (sid, start, end), frames in self._pv.items()
                if sid == system_id
            }


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
    weather_data_source: DataSource,
    site: AnySite,
    date_range: DateRange,
    grid_point_sample_index: int,
    collector: PreparedBatchCollector,
) -> None:
    """Prepare cleaned grid-point weather for one (grid point, window).

    Each day's prepared frame carries the grid point's lat/lon/elevation.
    The window's days are concatenated and buffered in the *collector*
    under the ``(grid_point_sample_index, window)`` key, so
    :func:`assemble_prepared_weather` can write the whole sample file's
    window as one partition file. Weather is grid-point data produced
    only by the single-process weather-grid backfill, so the collector is
    always present.
    """
    day_frames: list[pd.DataFrame] = []
    for day_range in date_range.split_by(Period.DAY):
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
        day_frames.append(prepared_df)
    if day_frames:
        collector.add_weather(
            grid_point_sample_index,
            date_range.start.isoformat(),
            date_range.end.isoformat(),
            pd.concat(day_frames, ignore_index=True),
        )


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
    When a *collector* is supplied (the single-process backfill) the
    window's days are buffered in memory under the ``(system, window)``
    key instead, for :func:`assemble_prepared_pv` to merge.
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
        collector.add_pv(
            pv_site.pvo_sys_id,
            date_range.start.isoformat(),
            date_range.end.isoformat(),
            pd.concat(day_frames, ignore_index=True),
        )


# ---------------------------------------------------------------------------
# Assembly functions
# ---------------------------------------------------------------------------


def _merge_prepared_frames(frames: list[pd.DataFrame], keys: list[str]) -> pd.DataFrame:
    """Concatenate prepared *frames*, then de-duplicate and sort on *keys*.

    Normalises ``time`` to ``datetime64`` first. A partition file is read
    back from CSV as ``str``, while the in-memory collector frames carry
    pandas ``Timestamp`` values straight from ``prepare_*``. A mixed-type
    ``time`` column breaks both following steps: ``drop_duplicates`` would
    treat a ``str`` and an equal ``Timestamp`` as distinct (so a
    re-prepared day wouldn't replace its row), and ``sort_values`` raises
    ``TypeError`` comparing ``Timestamp`` with ``str``.

    The conversion is pinned to ``format='ISO8601'``. A file's ``time``
    column can hold a mix of ``YYYY-MM-DD`` and ``YYYY-MM-DD HH:MM:SS``
    strings — ``to_csv`` renders a midnight ``Timestamp`` as a bare date
    but one with a time component in full. The default format-inferring
    ``to_datetime`` locks onto the first row's format and then raises on
    any row that differs; every value is ISO 8601 regardless, so parsing
    each independently as ISO 8601 is both correct and strict.

    *keys* drives both de-duplication (``keep='last'``, so freshly prepared
    rows win over the existing file) and the final sort.
    """
    combined = pd.concat(frames, ignore_index=True)
    combined['time'] = pd.to_datetime(combined['time'], format='ISO8601')
    combined = combined.drop_duplicates(subset=keys, keep='last')
    return combined.sort_values(keys).reset_index(drop=True)


def assemble_prepared_weather(
    prepared_fs: FileSystem,
    collector: PreparedBatchCollector,
    grid_definition_version: int,
) -> None:
    """Write one weather partition file per (sample file, window).

    Drains the in-memory *collector* — weather is grid-point data
    produced only by the single-process weather-grid backfill — groups
    the prepared frames by ``(grid_point_sample_index, window)``, and writes
    each group as ``weather/weather_{start}_{end}_{gv}-{NN}.csv`` (``gv``
    being *grid_definition_version*). An existing file of the same name is
    merged in first, so a re-run is idempotent.
    """
    groups = collector.weather_groups()
    if not groups:
        logger.warning('[assemble_weather] No prepared weather to assemble.')
        return

    for (grid_point_sample_index, start, end), batch_frames in groups.items():
        path = weather_partition_path(
            start, end, grid_point_sample_index, grid_definition_version
        )
        frames: list[pd.DataFrame] = []
        try:
            frames.append(read_csv(prepared_fs, path))
        except FileNotFoundError:
            pass
        frames.extend(batch_frames)
        combined = _merge_prepared_frames(frames, ['latitude', 'longitude', 'time'])
        write_csv(prepared_fs, combined, path)


def _find_pv_partition_for_week(
    prepared_fs: FileSystem, system_id: int, week_start: date
) -> str | None:
    """Return the path of *system_id*'s PV file for the ISO week starting
    *week_start*, or ``None`` if none exists yet.

    PV partition files are content-named, so the open file's path cannot
    be computed: it is the one in the system's directory whose start date
    falls in that ISO week.
    """
    directory = f'{PV_PREPARED_PREFIX}/{system_id}'
    for entry in prepared_fs.list_files(directory, '*.csv'):
        dates = _parse_pv_partition_dates(entry.name)
        if dates is not None and _iso_week_start(dates[0]) == week_start:
            return entry.path
    return None


def _write_pv_partition(
    prepared_fs: FileSystem,
    system_id: int,
    new_frames: list[pd.DataFrame],
    existing_path: str | None,
) -> None:
    """Merge *new_frames* into one content-named PV partition file.

    Reads *existing_path* (when given) so the open week file accumulates,
    de-duplicates and sorts on ``time``, names the result for the date
    range it actually covers, writes it, and deletes the old file when
    the recomputed name differs. The new file is written before the old
    is removed, so a crash leaves a harmless duplicate rather than a gap.
    """
    frames = list(new_frames)
    if existing_path is not None:
        frames.insert(0, read_csv(prepared_fs, existing_path))
    combined = _merge_prepared_frames(frames, ['time'])
    start = combined['time'].min().date().isoformat()
    end = (combined['time'].max().normalize() + timedelta(days=1)).date().isoformat()
    new_path = pv_partition_path(system_id, start, end)
    write_csv(prepared_fs, combined, new_path)
    if existing_path is not None and existing_path != new_path:
        prepared_fs.delete(existing_path)


def assemble_prepared_pv(
    batches_fs: FileSystem,
    prepared_fs: FileSystem,
    system_id: int,
    collector: PreparedBatchCollector | None = None,
) -> None:
    """Merge prepared PV frames for one system into its partition files.

    With a *collector* (the single-process backfill) each ``(system,
    window)`` group becomes one content-named file. Otherwise the daily
    transform's per-day batch CSVs are read, bucketed into ISO weeks, and
    each week merged into its open file — which grows day by day and is
    renamed to match the range it covers (see :func:`_write_pv_partition`).
    """
    if collector is not None:
        for batch_frames in collector.pv_groups(system_id).values():
            _write_pv_partition(prepared_fs, system_id, batch_frames, None)
        return

    all_pv_batches = batches_fs.list_files(PV_BATCH_PREFIX, '*.csv')
    batch_files = [e for e in all_pv_batches if e.name.startswith(f'{system_id}_')]
    if not batch_files:
        logger.warning('[assemble_pv] No batches for system %s.', system_id)
        return

    batch_frames = [
        _read_batch_csv(batches_fs, entry.path, PV_COLUMNS) for entry in batch_files
    ]
    new_rows = pd.concat(batch_frames, ignore_index=True)
    new_rows['time'] = pd.to_datetime(new_rows['time'], format='ISO8601')

    for _, week_rows in new_rows.groupby(new_rows['time'].dt.to_period('W')):
        week_start = _iso_week_start(week_rows['time'].iloc[0].date())
        existing_path = _find_pv_partition_for_week(prepared_fs, system_id, week_start)
        _write_pv_partition(prepared_fs, system_id, [week_rows], existing_path)

    for entry in batch_files:
        batches_fs.delete(entry.path)
