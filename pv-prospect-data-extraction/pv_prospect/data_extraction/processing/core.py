"""Core extraction logic — pure functions with no Celery dependency.

These are called by:
- ``runner.py``     for local development (concurrent.futures)
- ``entrypoint.py`` for Cloud Run Jobs
- ``tasks.py``      for legacy Celery execution (optional)
"""

import logging
from typing import Any, Callable

from pv_prospect.common import DateRange
from pv_prospect.data_extraction import (
    SourceDescriptor,
    TimeSeriesDataExtractor,
    TimeSeriesDescriptor,
)
from pv_prospect.data_extraction.processing.value_objects import Result, Task
from pv_prospect.data_sources import (
    LOCATION_MAPPING_CSV_FILE,
    PV_SITES_CSV_FILE,
    SUPPORTING_RESOURCES,
    build_csv_file_path,
)
from pv_prospect.etl import TIMESERIES_FOLDER, Extractor, Loader
from pv_prospect.etl.storage import FileSystem

logger = logging.getLogger(__name__)

# Re-export for callers that access these via core.PV_SITES_CSV_FILE etc.
__all__ = [
    'PV_SITES_CSV_FILE',
    'LOCATION_MAPPING_CSV_FILE',
    'SUPPORTING_RESOURCES',
    'TIMESERIES_FOLDER',
]


def preprocess(
    staged_resources_fs: FileSystem,
    source_descriptor: SourceDescriptor,
) -> list[str | None]:
    """
    Preprocess before extraction: create folder structure for time series data.

    Args:
        staged_resources_fs: FileSystem for writing to the staging location.
        source_descriptor: The source descriptor identifying the data source folder.
    """
    loader = Loader(staged_resources_fs)

    folder_ids: list[str | None] = [
        loader.create_folder(f'{TIMESERIES_FOLDER}/{source_descriptor}')
    ]

    return folder_ids


def extract_and_load(
    get_pv_site: Callable[[int], Any],
    get_ts_data_extractor: Callable[[SourceDescriptor], TimeSeriesDataExtractor],
    source_descriptor: SourceDescriptor,
    staging_fs: FileSystem,
    pv_system_id: int,
    date_range: DateRange,
    overwrite: bool,
    dry_run: bool,
) -> Result:
    """
    Extract data for a single PV system/date-range and load to storage.

    Args:
        get_pv_site: Retrieves a PVSite by integer system ID, or None if not found.
        get_ts_data_extractor: Returns a data extractor for the given source descriptor.
        source_descriptor: Identifies the data source and its extractor.
        staging_fs: FileSystem for both reading existing files and writing time series CSVs.
        pv_system_id: PV system identifier.
        date_range: DateRange containing start date and optional end date.
        overwrite: If True, overwrite existing files.
        dry_run: If True, preview without writing files.

    Returns:
        Result of the extraction and load operation.
    """
    task = Task(source_descriptor, pv_system_id, date_range)

    if dry_run:
        logger.info('%s: dry run — not writing', task)
        return Result.skipped_dry_run(task)

    staging_extractor = Extractor(staging_fs)
    staging_loader = Loader(staging_fs)

    def get_csv_path(ts_descriptor: TimeSeriesDescriptor) -> str:
        return build_csv_file_path(
            TIMESERIES_FOLDER,
            source_descriptor,
            ts_descriptor,
            date_range.start,
        )

    def is_processable(ts_descriptor: TimeSeriesDescriptor) -> bool:
        file_path = get_csv_path(ts_descriptor)
        return overwrite or not staging_extractor.file_exists(file_path)

    try:
        extractor = get_ts_data_extractor(source_descriptor)
        pv_site = get_pv_site(pv_system_id)
        if not pv_site or not pv_site.pvo_sys_id:
            raise ValueError('Unable to retrieve PVSite object')

        desired_ts_descriptors = extractor.get_time_series_descriptors(pv_site)
        processable_ts_descriptors = [
            ts for ts in desired_ts_descriptors if is_processable(ts)
        ]

        if not processable_ts_descriptors:
            logger.info('%s: all output files already exist', task)
            return Result.skipped_existing(task)

        timeseries = extractor.extract(
            processable_ts_descriptors,
            date_range.start,
            date_range.end,
        )

        for ts in timeseries:
            ts_file_path = get_csv_path(ts.descriptor)
            staging_loader.write_csv(ts_file_path, ts.rows, overwrite=overwrite)

        logger.info('%s: success', task)
        return Result.success(task)

    except Exception as e:
        logger.error('%s: %s', task, e)
        return Result.failure(task, e)
