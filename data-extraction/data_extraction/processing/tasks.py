from pathlib import Path

from domain import DateRange
from extractors import get_extractor, SourceDescriptor
from loaders import build_csv_file_path, get_storage_client
from .pv_site_repo import get_pv_site_by_system_id, build_pv_site_repo
from .value_objects import Task, Result
from .worker import app


TIMESERIES_FOLDER = 'timeseries'
METADATA_FOLDER = 'metadata'


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
    file_path = build_csv_file_path(source_descriptor, pv_system_id, date_range.start)
    timeseries_file_path = f"{TIMESERIES_FOLDER}/{file_path}"

    storage_client = get_storage_client(local_dir)
    build_pv_site_repo(storage_client)

    # Check if file already exists using polymorphic method
    if storage_client.file_exists(timeseries_file_path) and not overwrite:
        print(f"    {task}: File already exists")
        return Result.skipped_existing(task)

    if dry_run:
        print(f"    {task}: Dry run - not writing")
        return Result.skipped_dry_run(task)

    try:
        # Call extractor with appropriate arguments
        extractor = get_extractor(source_descriptor)
        pv_site = get_pv_site_by_system_id(pv_system_id)
        extraction_result = extractor.extract(pv_site, date_range.start, date_range.end)

        # Write CSV data using polymorphic method, pass overwrite flag
        storage_client.write_csv(timeseries_file_path, extraction_result.data, overwrite=overwrite)

        # Optionally write metadata JSON if requested and available
        if write_metadata and extraction_result.metadata:
            metadata_file_path = f"{METADATA_FOLDER}/{file_path}"
            try:
                storage_client.write_metadata(metadata_file_path, extraction_result.metadata)
            except Exception as meta_e:
                print(f"    {task}: WARNING - failed to write metadata: {meta_e}")

        print(f"    {task}: Success")
        return Result.success(task)

    except Exception as e:
        print(f"    {task}: ERROR: {e}")
        return Result.failure(task, e)

