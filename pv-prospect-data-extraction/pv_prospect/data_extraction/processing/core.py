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
    map_from_env,
    VarMapping,
)
from dataclasses import dataclass
from pv_prospect.etl.extractors.gcs import GcsExtractor
from pv_prospect.data_extraction.extractors import get_extractor, SourceDescriptor
from pv_prospect.data_extraction.extractors.base import TimeSeriesDescriptor
from pv_prospect.etl.factory import get_loader, get_extractor as get_storage_extractor
from pv_prospect.data_extraction.processing.value_objects import Task, Result

TIMESERIES_FOLDER = 'timeseries'
PV_SITES_CSV_FILE = 'pv_sites.csv'
OM_BOUNDING_BOXES_CSV_FILE = 'openmeteo_bounding_boxes.csv'

SUPPORTING_RESOURCES = [PV_SITES_CSV_FILE, OM_BOUNDING_BOXES_CSV_FILE]


@dataclass(frozen=True)
class CoreConfig:
    dvc_file_path: str

    @classmethod
    def from_env(cls) -> 'CoreConfig':
        return map_from_env(cls, {
            'dvc_file_path': VarMapping('DVC_FILE_PATH', str)
        })


def preprocess(
        source_descriptor: SourceDescriptor,
        local_dir: str | None,
        config: CoreConfig | None = None,
) -> list[str]:
    """
    Preprocess before extraction: create folder structure and provision
    supporting resources (pv_sites.csv, openmeteo_bounding_boxes.csv).

    Args:
        source_descriptor: The source descriptor identifying the data source folder.
        local_dir: If provided, a local directory path where files will be created.
        config: Configuration containing DVC file path.
    """
    if config is None:
        config = CoreConfig.from_env()

    loader = get_loader(local_dir)
    parent_folders = [TIMESERIES_FOLDER]
    folder_ids = [
        loader.create_folder(f"{parent}/{source_descriptor}")
        for parent in parent_folders
    ]

    gcs_extractor = GcsExtractor.from_env()
    blob_paths = gcs_extractor.resolve_dvc_blob_paths(config.dvc_file_path, SUPPORTING_RESOURCES)
    
    for filename, src_blob_path in blob_paths.items():
        if loader.file_exists(filename):
            print(f"    {filename} already exists, skipping provisioning")
            continue
            
        with gcs_extractor.read_file(src_blob_path) as f:
            loader.write_text(filename, f.read(), overwrite=False)

    return folder_ids


def extract_and_load(
        source_descriptor: SourceDescriptor,
        pv_system_id: int,
        date_range: DateRange,
        local_dir: str | None,
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
        overwrite: If True, overwrite existing files.
        dry_run: If True, perform a dry run (do not write files).

    Returns:
        Result of the extraction and load operation.
    """
    task = Task(source_descriptor, pv_system_id, date_range)

    storage_extractor = get_storage_extractor(local_dir)
    loader = get_loader(local_dir)
    build_pv_site_repo(
        storage_extractor.read_file(PV_SITES_CSV_FILE)
    )
    build_openmeteo_bounding_box_repo(
        storage_extractor.read_file(OM_BOUNDING_BOXES_CSV_FILE)
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
        return overwrite or not storage_extractor.file_exists(file_path)

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
            loader.write_csv(ts_file_path, ts.rows, overwrite=overwrite)

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
