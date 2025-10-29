from domain import DateRange
from domain.pv_site import PVSite
from loaders.helpers import build_csv_file_path

from .value_objects import Task, Result

ALLOW_DUPLICATE_FILES = False


def extract_and_load(
        extractor,
        storage_client,
        source_descriptor: str,
        pv_site: PVSite,
        date_range: DateRange,
        dry_run: bool,
        write_metadata: bool
) -> Result:
    """
    Process a single extraction for a given PV site and date (or date range).

    Args:
        storage_client: Storage client (LocalStorageClient or GDriveClient)
        extractor: The data extractor instance
        source_descriptor: Data source descriptor string (e.g., 'openmeteo/hourly')
        pv_site: PV site to extract data for
        date_range: DateRange containing start date and optional end date
        dry_run: If True, don't actually write files
        write_metadata: If True, write metadata JSON alongside CSV

    Returns:
        Result: The result of the extraction and load operation
    """
    task = Task(source_descriptor=source_descriptor, date_range=date_range, pv_site=pv_site)
    file_path = build_csv_file_path(source_descriptor, pv_site, date_range.start)

    # Check if file already exists using polymorphic method
    if storage_client.file_exists(file_path) and not ALLOW_DUPLICATE_FILES:
        print(f"    File already exists: {file_path}")
        return Result.skipped_existing(task)

    if dry_run:
        print(f"    Dry run - not writing: {file_path}")
        return Result.skipped_dry_run(task)

    try:
        # Call extractor with appropriate arguments
        extraction_result = extractor.extract(pv_site, date_range.start, date_range.end)

        # Write CSV data using polymorphic method
        storage_client.write_csv(file_path, extraction_result.data)

        # Optionally write metadata JSON if requested and available
        if write_metadata and extraction_result.metadata:
            try:
                storage_client.write_metadata(file_path, extraction_result.metadata)
            except Exception as meta_e:
                print(f"    WARNING: failed to write metadata for {file_path}: {meta_e}")

        return Result.success(task)

    except Exception as e:
        print(f"    ERROR: {e}")
        return Result.failure(task, e)
