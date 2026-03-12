"""Core extraction logic — pure functions with no Celery dependency.

These are called by:
- ``runner.py``     for local development (concurrent.futures)
- ``entrypoint.py`` for Cloud Run Jobs
- ``tasks.py``      for legacy Celery execution (optional)
"""

import os
from datetime import date

from pv_prospect.common import (
    DateRange,
    build_location_mapping_repo,
    build_pv_site_repo,
    get_pv_site_by_system_id,
)
from pv_prospect.common.config_parser import get_config
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.extractors import SourceDescriptor, get_extractor
from pv_prospect.data_extraction.extractors.base import TimeSeriesDescriptor
from pv_prospect.data_extraction.processing.value_objects import Result, Task
from pv_prospect.etl.extract.resolve.dvc import resolve_path
from pv_prospect.etl.factory import get_extractor as get_file_extractor
from pv_prospect.etl.factory import get_loader as get_file_loader
from pv_prospect.etl.storage_config import (
    GcsStorageConfig,
    LocalStorageConfig,
)

TIMESERIES_FOLDER = 'timeseries'
PV_SITES_CSV_FILE = 'pv_sites.csv'
LOCATION_MAPPING_CSV_FILE = 'location_mapping.csv'


SUPPORTING_RESOURCES = [PV_SITES_CSV_FILE, LOCATION_MAPPING_CSV_FILE]


def preprocess(source_descriptor: SourceDescriptor, local_dir: str | None) -> list[str]:
    """
    Preprocess before extraction: create folder structure and provision
    supporting resources (pv_sites.csv, location_mapping.csv).

    Args:
        source_descriptor: The source descriptor identifying the data source folder.
        local_dir: If provided, a local directory path where files will be created.
    """
    config = get_config(DataExtractionConfig)

    versioned_resources_extractor = get_file_extractor(
        config.versioned_resources_storage
    )

    staging_location_config = (
        LocalStorageConfig(base_dir=local_dir, tracking=None)
        if local_dir
        else config.staged_raw_data_storage
    )
    staged_resources_loader = get_file_loader(staging_location_config)

    parent_folders = [TIMESERIES_FOLDER]
    folder_ids = [
        staged_resources_loader.create_folder(f'{parent}/{source_descriptor}')
        for parent in parent_folders
    ]

    dvc_config = config.versioned_resources_storage.tracking
    if isinstance(dvc_config, LocalStorageConfig):
        dvc_prefix = dvc_config.base_dir
    elif isinstance(dvc_config, GcsStorageConfig):
        dvc_prefix = dvc_config.prefix
    else:
        dvc_prefix = ''

    resources = [
        (filename, resolve_path(f'{dvc_prefix}/{filename}.dvc'))
        for filename in SUPPORTING_RESOURCES
    ]

    for filename, blob_path in resources:
        if staged_resources_loader.file_exists(filename):
            print(f'    {filename} already exists, skipping provisioning')
            continue

        with versioned_resources_extractor.read_file(blob_path) as f:
            staged_resources_loader.write_text(filename, f.read(), overwrite=False)

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

    config = get_config(DataExtractionConfig)

    staging_location_config = (
        LocalStorageConfig(base_dir=local_dir, tracking=None)
        if local_dir
        else config.staged_raw_data_storage
    )
    resources_extractor = get_file_extractor(staging_location_config)
    time_series_loader = get_file_loader(staging_location_config)

    build_pv_site_repo(resources_extractor.read_file(PV_SITES_CSV_FILE))
    build_location_mapping_repo(
        resources_extractor.read_file(LOCATION_MAPPING_CSV_FILE)
    )

    if dry_run:
        print(f'    {task}: Dry run - not writing')
        return Result.skipped_dry_run(task)

    def get_csv_path(ts_descriptor: TimeSeriesDescriptor) -> str:
        return _build_csv_file_path(
            TIMESERIES_FOLDER,
            source_descriptor,
            ts_descriptor,
            date_range.start,
        )

    def is_processable(ts_descriptor: TimeSeriesDescriptor) -> bool:
        file_path = get_csv_path(ts_descriptor)
        return overwrite or not resources_extractor.file_exists(file_path)

    try:
        extractor = get_extractor(source_descriptor)
        pv_site = get_pv_site_by_system_id(pv_system_id)
        if not pv_site or not pv_site.pvo_sys_id:
            raise ValueError('Unable to retrieve PVSite object')

        desired_ts_descriptors = extractor.get_time_series_descriptors(pv_site)
        processable_ts_descriptors = [
            ts for ts in desired_ts_descriptors if is_processable(ts)
        ]

        if not processable_ts_descriptors:
            print(f'    {task}: All output files already exist')
            return Result.skipped_existing(task)

        timeseries = extractor.extract(
            processable_ts_descriptors,
            date_range.start,
            date_range.end,
        )

        for ts in timeseries:
            ts_file_path = get_csv_path(ts.descriptor)
            time_series_loader.write_csv(ts_file_path, ts.rows, overwrite=overwrite)

        print(f'    {task}: Success')
        return Result.success(task)

    except Exception as e:
        print(f'    {task}: ERROR: {e}')
        return Result.failure(task, e)


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
    return '%04d%02d%02d' % (date_.year, date_.month, date_.day)
