"""Per-slice task bodies for the pull-based transform backfill.

Each function takes one :class:`PVSlice` or :class:`WeatherSlice`,
reads its raw inputs, runs clean → prepare → assemble in memory, and
writes the one prepared partition file the slice maps to. Returns
the :class:`SliceOutcome` the caller routes into the ledger:

  * ``status='completed'`` — all inputs present.
  * ``status='partial'`` — output written, but some raw inputs were
    missing; ``missing_inputs_count`` records how many. The
    application log (Cloud Logging) records the missing paths.
  * a raised exception — the caller records ``status='failed'`` and
    re-raises.

No intermediate ``cleaned/`` files are written; the per-slice
clean→prepare→assemble chain runs entirely in memory. The
extraction-side path convention is followed only for *raw* input
paths.
"""

import logging
from dataclasses import dataclass
from typing import Callable, Literal

import pandas as pd

from pv_prospect.common.domain import (
    ArbitrarySite,
    DateRange,
    Location,
    Period,
    PVSite,
)
from pv_prospect.data_sources import (
    DataSource,
    build_time_series_csv_file_path,
)
from pv_prospect.data_transformation.processing.core import (
    PV_COLUMNS,
    WEATHER_COLUMNS,
    merge_prepared_frames,
    pv_partition_path,
    read_csv,
    read_metadata,
    weather_partition_path,
    write_csv,
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
from pv_prospect.etl import TIMESERIES_FOLDER, PVSlice, WeatherSlice
from pv_prospect.etl.storage import FileSystem

logger = logging.getLogger(__name__)

SliceStatus = Literal['completed', 'partial']


@dataclass(frozen=True)
class SliceOutcome:
    """The result of producing one slice.

    ``status`` distinguishes a clean run (all inputs present) from a
    partial one (some inputs missing — see the application log for
    the specific paths). ``missing_inputs_count`` is non-zero only
    when ``status='partial'``. A raised exception (not represented
    here) is the caller's signal to record ``status='failed'``.
    """

    status: SliceStatus
    missing_inputs_count: int = 0


def _grid_point_sites(locations: list[str]) -> list[ArbitrarySite]:
    return [ArbitrarySite(Location.from_coordinate_string(loc)) for loc in locations]


def produce_weather_slice(
    weather_slice: WeatherSlice,
    raw_fs: FileSystem,
    prepared_fs: FileSystem,
    weather_data_source: DataSource,
    grid_point_locations: list[str],
    grid_definition_version: int,
) -> SliceOutcome:
    """Read raw, clean → prepare → assemble one weather slice.

    Iterates the grid-point locations in the slice's sample file,
    reads each grid point's raw weather window file (plus its
    metadata sidecar), cleans and prepares each in memory, then
    assembles them into one prepared partition file under
    ``prepared/weather/``. An existing partition file at the same
    path is merged in first, so a re-run with newly-extracted inputs
    upgrades a previous partial output to a complete one.
    """
    date_range = DateRange(weather_slice.start_date, weather_slice.end_date)
    grid_point_sites = _grid_point_sites(grid_point_locations)

    prepared_frames: list[pd.DataFrame] = []
    missing_paths: list[str] = []
    for grid_point_site in grid_point_sites:
        raw_path = build_time_series_csv_file_path(
            TIMESERIES_FOLDER, weather_data_source, grid_point_site, date_range
        )
        if not raw_fs.exists(raw_path):
            missing_paths.append(raw_path)
            continue
        raw_df = read_csv(raw_fs, raw_path)
        metadata = read_metadata(raw_fs, raw_path)
        cleaned_df = _clean_weather_transform(raw_df)
        prepared_df = _prepare_weather_transform(cleaned_df)
        prepared_df.insert(0, 'latitude', metadata['latitude'])
        prepared_df.insert(1, 'longitude', metadata['longitude'])
        prepared_df.insert(2, 'elevation', metadata['elevation'])
        prepared_frames.append(prepared_df[list(WEATHER_COLUMNS)])

    if missing_paths:
        logger.warning(
            '[produce_weather_slice] %s: %d missing raw inputs: %s',
            weather_slice,
            len(missing_paths),
            missing_paths,
        )

    if not prepared_frames:
        return SliceOutcome(status='partial', missing_inputs_count=len(missing_paths))

    path = weather_partition_path(
        weather_slice.start_date.isoformat(),
        weather_slice.end_date.isoformat(),
        weather_slice.grid_point_sample_index,
        grid_definition_version,
    )
    frames_to_merge: list[pd.DataFrame] = []
    if prepared_fs.exists(path):
        frames_to_merge.append(read_csv(prepared_fs, path))
    frames_to_merge.extend(prepared_frames)
    combined = merge_prepared_frames(frames_to_merge, ['latitude', 'longitude', 'time'])
    write_csv(prepared_fs, combined, path)

    return SliceOutcome(
        status='partial' if missing_paths else 'completed',
        missing_inputs_count=len(missing_paths),
    )


def produce_pv_slice(
    pv_slice: PVSlice,
    raw_fs: FileSystem,
    prepared_fs: FileSystem,
    pv_data_source: DataSource,
    weather_data_source: DataSource,
    get_pv_site: Callable[[int], PVSite],
) -> SliceOutcome:
    """Read raw, clean → prepare → assemble one PV slice.

    Reads the one window-spanning OpenMeteo weather raw file for the
    PV site's location, then iterates one PVOutput raw file per day
    in ``[start, end)``. For each day where both inputs are present,
    cleans both in memory, joins via ``prepare_pv`` (which computes
    POA irradiance), and accumulates the per-day prepared frame.
    Assembles the per-day frames into one prepared PV partition file
    under ``prepared/pv/{system_id}/``. An existing partition file
    at the same path is merged in first, so a re-run with
    newly-extracted inputs upgrades a previous partial output to a
    complete one.

    A day's PV raw file that exists but is empty is *not* counted as
    missing — PVOutput records the API call as completed even when a
    system returned no readings (offline / decommissioned / etc.), so
    an empty file is the truth about that day. A missing weather file
    or missing PV file for a day, by contrast, is counted toward the
    partial-input total.
    """
    pv_site = get_pv_site(pv_slice.pv_system_id)
    date_range = DateRange(pv_slice.start_date, pv_slice.end_date)

    weather_raw_path = build_time_series_csv_file_path(
        TIMESERIES_FOLDER, weather_data_source, pv_site, date_range
    )
    if not raw_fs.exists(weather_raw_path):
        logger.warning(
            '[produce_pv_slice] %s: missing PV-site weather window file %s',
            pv_slice,
            weather_raw_path,
        )
        return SliceOutcome(status='partial', missing_inputs_count=1)
    weather_raw_df = read_csv(raw_fs, weather_raw_path)
    weather_cleaned_df = _clean_weather_transform(weather_raw_df)
    # The merged frame in clean_weather carries ``time`` as datetime64;
    # we slice per-day below by ``time.dt.date``.

    missing_paths: list[str] = []
    day_frames: list[pd.DataFrame] = []
    for day_range in date_range.split_by(Period.DAY):
        pv_raw_path = build_time_series_csv_file_path(
            TIMESERIES_FOLDER, pv_data_source, pv_site, day_range
        )
        if not raw_fs.exists(pv_raw_path):
            missing_paths.append(pv_raw_path)
            continue
        pv_raw_df = read_csv(raw_fs, pv_raw_path)
        if pv_raw_df.empty:
            # Empty file means "no PV readings this day" — a fact, not
            # a missing input. Skip the day's contribution without
            # counting it as missing.
            continue
        pv_cleaned_df = _clean_pv_transform(pv_raw_df)
        day_weather_df = weather_cleaned_df[
            weather_cleaned_df['time'].dt.date == day_range.start
        ]
        if day_weather_df.empty:
            missing_paths.append(f'{weather_raw_path}[{day_range.start}]')
            continue
        prepared_df = _prepare_pv_transform(
            weather_df=day_weather_df,
            pv_df=pv_cleaned_df,
            pv_site=pv_site,
        )
        day_frames.append(prepared_df[list(PV_COLUMNS)])

    if missing_paths:
        logger.warning(
            '[produce_pv_slice] %s: %d missing raw inputs: %s',
            pv_slice,
            len(missing_paths),
            missing_paths,
        )

    if not day_frames:
        return SliceOutcome(status='partial', missing_inputs_count=len(missing_paths))

    path = pv_partition_path(
        pv_slice.pv_system_id,
        pv_slice.start_date.isoformat(),
        pv_slice.end_date.isoformat(),
    )
    frames_to_merge: list[pd.DataFrame] = []
    if prepared_fs.exists(path):
        frames_to_merge.append(read_csv(prepared_fs, path))
    frames_to_merge.extend(day_frames)
    combined = merge_prepared_frames(frames_to_merge, ['time'])
    write_csv(prepared_fs, combined, path)

    return SliceOutcome(
        status='partial' if missing_paths else 'completed',
        missing_inputs_count=len(missing_paths),
    )
