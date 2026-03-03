"""Core extraction logic — pure functions with no Celery dependency.

These are called by:
- ``runner.py``     for local development (concurrent.futures)
- ``entrypoint.py`` for Cloud Run Jobs
- ``tasks.py``      for legacy Celery execution (optional)
"""
from datetime import date
import os

from pv_prospect.common import (
    DateRange,
    build_pv_site_repo,
    build_openmeteo_bounding_box_repo,
    get_pv_site_by_system_id,
)
from pv_prospect.data_extraction.extractors import get_extractor, SourceDescriptor
from pv_prospect.data_extraction.extractors.base import TimeSeriesDescriptor
from pv_prospect.data_extraction.loaders import get_storage_client
from pv_prospect.data_extraction.processing.value_objects import Task, Result

TIMESERIES_FOLDER = 'timeseries'
METADATA_FOLDER = 'metadata'
PV_SITES_CSV_FILE = 'pv_sites.csv'
OM_BOUNDING_BOXES_CSV_FILE = 'openmeteo_bounding_boxes.csv'

SUPPORTING_RESOURCES = [PV_SITES_CSV_FILE, OM_BOUNDING_BOXES_CSV_FILE]
DVC_FILE_PATH = os.environ.get('DVC_FILE_PATH', '/app/resources.dvc')


def preprocess(
        source_descriptor: SourceDescriptor,
        local_dir: str | None,
        include_metadata: bool,
) -> list[str]:
    """
    Preprocess before extraction: create folder structure and provision
    supporting resources (pv_sites.csv, openmeteo_bounding_boxes.csv).

    Args:
        source_descriptor: The source descriptor identifying the data source folder.
        local_dir: If provided, a local directory path where files will be created.
        include_metadata: If True, also create a metadata folder.
    """
    storage_client = get_storage_client(local_dir)
    parent_folders = (
        [TIMESERIES_FOLDER, METADATA_FOLDER] if include_metadata else [TIMESERIES_FOLDER]
    )
    folder_ids = [
        storage_client.create_folder(f"{parent}/{source_descriptor}")
        for parent in parent_folders
    ]

    storage_client.provision_supporting_resources(DVC_FILE_PATH, SUPPORTING_RESOURCES)

    return folder_ids


def extract_and_load(
        source_descriptor: SourceDescriptor,
        pv_system_id: int,
        date_range: DateRange,
        local_dir: str | None,
        write_metadata: bool,
        overwrite: bool,
        dry_run: bool,
) -> Result:
    """
    Extract data for a single PV system/date-range and load to storage.

    Args:
        source_descriptor: The source descriptor identifying the extractor.
        pv_system_id: PV system identifier.
        date_range: DateRange containing start date and optional end date.
        local_dir: Local directory path, or None for GCS.
        write_metadata: If True, write metadata JSON alongside the CSV.
        overwrite: If True, overwrite existing files.
        dry_run: If True, perform a dry run (do not write files).

    Returns:
        Result of the extraction and load operation.
    """
    task = Task(source_descriptor, pv_system_id, date_range)

    storage_client = get_storage_client(local_dir)
    build_pv_site_repo(
        storage_client.read_file(PV_SITES_CSV_FILE)
    )
    build_openmeteo_bounding_box_repo(
        storage_client.read_file(OM_BOUNDING_BOXES_CSV_FILE)
    )

    if dry_run:
        print(f"    {task}: Dry run - not writing")
        return Result.skipped_dry_run(task)

    def get_csv_path(ts_descriptor: TimeSeriesDescriptor) -> str:
        return _build_csv_file_path(
            TIMESERIES_FOLDER, source_descriptor, ts_descriptor, date_range.start,
        )

    def is_processable(ts_descriptor: TimeSeriesDescriptor) -> bool:
        file_path = get_csv_path(ts_descriptor)
        return overwrite or not storage_client.file_exists(file_path)

    try:
        extractor = get_extractor(source_descriptor)
        pv_site = get_pv_site_by_system_id(pv_system_id)
        if not pv_site or not pv_site.pvo_sys_id:
            raise ValueError("Unable to retrieve PVSite object")

        desired_ts_descriptors = extractor.get_time_series_descriptors(pv_site)
        processable_ts_descriptors = [
            ts for ts in desired_ts_descriptors if is_processable(ts)
        ]

        if not processable_ts_descriptors:
            print(f"    {task}: All output files already exist")
            return Result.skipped_existing(task)

        timeseries = extractor.extract(
            processable_ts_descriptors, date_range.start, date_range.end,
        )

        for ts in timeseries:
            ts_file_path = get_csv_path(ts.descriptor)
            storage_client.write_csv(ts_file_path, ts.rows, overwrite=overwrite)

        print(f"    {task}: Success")
        return Result.success(task)

    except Exception as e:
        print(f"    {task}: ERROR: {e}")
        return Result.failure(task, e)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_csv_file_path(
        time_series_folder: str,
        source_descriptor: 'SourceDescriptor',
        time_series_descriptor: 'TimeSeriesDescriptor',
        date_: date,
) -> str:
    filename_parts = [
        str(source_descriptor).replace('/', '-'),
        str(time_series_descriptor),
        _format_date(date_),
    ]
    filename = '_'.join(filename_parts) + '.csv'
    return os.path.join(time_series_folder, source_descriptor, filename)


def _format_date(date_: date) -> str:
    return "%04d%02d%02d" % (date_.year, date_.month, date_.day)
