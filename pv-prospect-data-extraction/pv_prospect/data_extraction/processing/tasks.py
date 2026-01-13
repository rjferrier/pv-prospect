from datetime import date
import os

from pv_prospect.common import DateRange, build_pv_site_repo, build_openmeteo_bounding_box_repo, get_pv_site_by_system_id
from pv_prospect.data_extraction.extractors import get_extractor, SourceDescriptor
from pv_prospect.data_extraction.extractors.base import TimeSeriesDescriptor
from pv_prospect.data_extraction.loaders import get_storage_client
from pv_prospect.data_extraction.processing.value_objects import Task, Result
from pv_prospect.data_extraction.processing.worker import app

TIMESERIES_FOLDER = 'timeseries'
METADATA_FOLDER = 'metadata'
PV_SITES_CSV_FILE = 'pv_sites.csv'
OM_BOUNDING_BOXES_CSV_FILE = 'openmeteo_bounding_boxes.csv'

@app.task
def create_folders(
        source_descriptor: SourceDescriptor,
        local_dir: str | None,
        include_metadata: bool
) -> list[str]:
    """
    Create folder structure for a data source.

    Args:
        source_descriptor: The source descriptor enum identifying the data source folder.
        local_dir: If provided, a local directory path where folders will be created instead of Google Drive.
    """
    storage_client = get_storage_client(local_dir)
    parent_folders = [TIMESERIES_FOLDER, METADATA_FOLDER] if include_metadata else [TIMESERIES_FOLDER]
    folder_ids = [
        storage_client.create_folder(f"{parent}/{source_descriptor}")
        for parent in parent_folders
    ]
    return folder_ids


@app.task
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
    Process a single extraction and load for a given PV system and date (or date range).

    Args:
        source_descriptor: The source descriptor enum identifying the extractor to use.
        pv_system_id: The integer PV system identifier to extract data for.
        date_range: DateRange containing start date and optional end date.
        local_dir: If provided, a local directory path where files will be written instead of Google Drive.
        write_metadata: If True, write metadata JSON alongside the CSV when available.
        dry_run: If True, perform a dry run (do not write files).
        overwrite: If True, overwrite existing files. Otherwise, skip existing files.

    Returns:
        Result: The result of the extraction and load operation.
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
        return _build_csv_file_path(TIMESERIES_FOLDER, source_descriptor, ts_descriptor, date_range.start)

    def is_processable(ts_descriptor: TimeSeriesDescriptor) -> bool:
        file_path = get_csv_path(ts_descriptor)
        return overwrite or not storage_client.file_exists(file_path)

    try:
        # Call extractor with appropriate arguments
        extractor = get_extractor(source_descriptor)
        pv_site = get_pv_site_by_system_id(pv_system_id)
        if not pv_site or not pv_site.pvo_sys_id:
            raise ValueError("Unable to retrieve PVSite object")

        desired_ts_descriptors = extractor.get_time_series_descriptors(pv_site)
        processable_ts_descriptors = [
            ts_descriptor for ts_descriptor in desired_ts_descriptors if is_processable(ts_descriptor)
        ]

        if not processable_ts_descriptors:
            print(f"    {task}: All output files already exist")
            return Result.skipped_existing(task)

        timeseries = extractor.extract(processable_ts_descriptors, date_range.start, date_range.end)

        # Write each time series to storage
        for ts in timeseries:
            ts_file_path = get_csv_path(ts.descriptor)
            storage_client.write_csv(ts_file_path, ts.rows, overwrite=overwrite)

        print(f"    {task}: Success")
        return Result.success(task)

    except Exception as e:
        print(f"    {task}: ERROR: {e}")
        return Result.failure(task, e)


def _build_csv_file_path(
        time_series_folder: str,
        source_descriptor: 'SourceDescriptor',
        time_series_descriptor: 'TimeSeriesDescriptor',
        date_: date
) -> str:
    filename_parts = [
        str(source_descriptor).replace('/', '-'),
        str(time_series_descriptor),
        _format_date(date_)
    ]
    filename = '_'.join(filename_parts) + '.csv'
    return os.path.join(time_series_folder, source_descriptor, filename)


def _format_date(date_: date) -> str:
    return "%04d%02d%02d" % (date_.year, date_.month, date_.day)
