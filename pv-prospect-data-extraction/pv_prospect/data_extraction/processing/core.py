"""Core extraction logic — pure functions with no Celery dependency.

These are called by:
- ``runner.py``     for local development (concurrent.futures)
- ``entrypoint.py`` for Cloud Run Jobs
- ``tasks.py``      for legacy Celery execution (optional)
"""

import json
import logging
from typing import Callable

from pv_prospect.common.domain import AnySite, DateRange, Site
from pv_prospect.data_extraction import (
    DataSource,
    TimeSeriesDataExtractor,
)
from pv_prospect.data_extraction.processing.value_objects import Result, Task
from pv_prospect.data_sources import (
    PV_SITES_CSV_FILE,
    SUPPORTING_RESOURCES,
    build_time_series_csv_file_path,
    csv_path_to_metadata_path,
)
from pv_prospect.etl import TIMESERIES_FOLDER, Loader
from pv_prospect.etl.storage import FileSystem

logger = logging.getLogger(__name__)

__all__ = [
    'PV_SITES_CSV_FILE',
    'SUPPORTING_RESOURCES',
    'TIMESERIES_FOLDER',
]


def preprocess(
    staged_resources_fs: FileSystem,
    data_source: DataSource,
) -> list[str | None]:
    """
    Preprocess before extraction: create folder structure for time series data.

    Args:
        staged_resources_fs: FileSystem for writing to the staging location.
        data_source: The data source identifying the folder.
    """
    loader = Loader(staged_resources_fs)

    folder_ids: list[str | None] = [
        loader.create_folder(f'{TIMESERIES_FOLDER}/{data_source}')
    ]

    return folder_ids


def extract_and_load(
    get_ts_data_extractor: Callable[[DataSource], TimeSeriesDataExtractor],
    data_source: DataSource,
    staging_fs: FileSystem,
    site: AnySite,
    date_range: DateRange,
    dry_run: bool,
) -> Result:
    """
    Extract data for a single target/date-range and load to storage.

    Args:
        get_ts_data_extractor: Returns a data extractor for the given data source.
        data_source: Identifies the data source and its extractor.
        staging_fs: FileSystem for both reading existing files and writing time series CSVs.
        site: The PV site or weather grid point to extract data for.
        date_range: DateRange containing start date and optional end date.
        dry_run: If True, preview without writing files.

    Returns:
        Result of the extraction and load operation.
    """
    task = Task(data_source, site, date_range)

    if dry_run:
        logger.info('%s: dry run — not writing', task)
        return Result.skipped_dry_run(task)

    staging_loader = Loader(staging_fs)

    def get_csv_path(site_: Site) -> str:
        return build_time_series_csv_file_path(
            TIMESERIES_FOLDER,
            data_source,
            site_,
            date_range,
        )

    try:
        extractor = get_ts_data_extractor(data_source)

        timeseries = extractor.extract(
            [site],
            date_range.start,
            date_range.end,
        )

        for ts in timeseries:
            ts_file_path = get_csv_path(site)
            # We always overwrite now.
            staging_loader.write_csv(ts_file_path, ts.rows, overwrite=True)
            if ts.metadata is not None:
                meta_path = csv_path_to_metadata_path(ts_file_path)
                staging_loader.write_text(
                    meta_path, json.dumps(ts.metadata), overwrite=True
                )

        logger.info('%s: success', task)
        return Result.success(task)

    except Exception as e:
        logger.error('%s: %s', task, e)
        return Result.failure(task, e)
