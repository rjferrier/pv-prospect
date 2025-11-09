import os

from domain import DateRange
from extractors import get_extractor, SourceDescriptor
from loaders import build_csv_file_path, build_folder_path
from loaders.gdrive import GDriveClient
from loaders.local import LocalStorageClient
from .worker import app
from .pv_site_repo import get_pv_site_by_system_id

from .value_objects import Task, Result


@app.task
def create_folder(
        source_descriptor: SourceDescriptor,
        local_dir: str | None,
) -> None:
    """
    Create folder structure for a data source.

    Args:
        source_descriptor: The source descriptor enum identifying the data source folder.
        local_dir: If provided, a local directory path where folders will be created instead of Google Drive.
    """
    storage_client = _get_storage_client(local_dir)
    folder_path = build_folder_path(source_descriptor)
    storage_client.create_folder(folder_path)
    print(f"Created folder structure for {source_descriptor}")


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

    storage_client = _get_storage_client(local_dir)

    # Check if file already exists using polymorphic method
    if storage_client.file_exists(file_path) and not overwrite:
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
        storage_client.write_csv(file_path, extraction_result.data, overwrite=overwrite)

        # Optionally write metadata JSON if requested and available
        if write_metadata and extraction_result.metadata:
            try:
                storage_client.write_metadata(file_path, extraction_result.metadata)
            except Exception as meta_e:
                print(f"    {task}: WARNING - failed to write metadata: {meta_e}")

        print(f"    {task}: Success")
        return Result.success(task)

    except Exception as e:
        print(f"    {task}: ERROR: {e}")
        return Result.failure(task, e)


def _get_storage_client(local_dir: str | None) -> LocalStorageClient | GDriveClient:
    if local_dir:
        return LocalStorageClient(local_dir)
    else:
        return GDriveClient.build_service()
