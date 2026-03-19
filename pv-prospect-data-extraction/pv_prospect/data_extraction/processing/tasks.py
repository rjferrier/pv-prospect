"""Celery task wrappers — thin shims that delegate to :mod:`core`.

Kept for backward compatibility with the existing Celery-based local
development setup.  The Cloud Run path bypasses this module entirely.
"""

from pv_prospect.common import (
    DateRange,
    build_location_mapping_repo,
    build_pv_site_repo,
    get_config,
    get_pv_site_by_system_id,
)
from pv_prospect.data_extraction import SourceDescriptor, get_extractor
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.processing import core
from pv_prospect.data_extraction.processing.value_objects import Result
from pv_prospect.data_extraction.processing.worker import app
from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
from pv_prospect.etl import Extractor
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import FileSystem, get_filesystem
from pv_prospect.etl.storage.backends import LocalStorageConfig
from pv_prospect.etl.storage.resolve import resolve_dvc_path

# Re-export constants so existing imports (e.g. task_producer) keep working.
PV_SITES_CSV_FILE = core.PV_SITES_CSV_FILE
LOCATION_MAPPING_CSV_FILE = core.LOCATION_MAPPING_CSV_FILE
SUPPORTING_RESOURCES = core.SUPPORTING_RESOURCES


def _resolve_storage(local_dir: str | None) -> tuple[DataExtractionConfig, FileSystem]:
    """Resolve storage backends from config and optional local override."""
    config = get_config(
        DataExtractionConfig,
        base_config_dirs=[get_etl_config_dir(), get_ds_config_dir()],
    )
    staging_config = (
        LocalStorageConfig(prefix=local_dir)
        if local_dir
        else config.staged_raw_data_storage
    )
    return config, get_filesystem(staging_config)


@app.task
def preprocess(
    source_descriptor: SourceDescriptor,
    local_dir: str | None,
) -> list[str | None]:
    config, staging_fs = _resolve_storage(local_dir)
    versioned_resources_fs = get_filesystem(config.versioned_resources_storage)
    dvc_prefix = (
        config.versioned_resources_storage.tracking.prefix
        if config.versioned_resources_storage.tracking
        else ''
    )
    return core.preprocess(
        resolve_dvc_path,
        versioned_resources_fs,
        staging_fs,
        dvc_prefix,
        source_descriptor,
    )


@app.task
def extract_and_load(
    source_descriptor: SourceDescriptor,
    pv_system_id: int,
    date_range: DateRange,
    local_dir: str | None,
    overwrite: bool,
    dry_run: bool,
) -> Result:
    _config, staging_fs = _resolve_storage(local_dir)

    staging_extractor = Extractor(staging_fs)
    build_pv_site_repo(staging_extractor.read_file(core.PV_SITES_CSV_FILE))
    build_location_mapping_repo(
        staging_extractor.read_file(core.LOCATION_MAPPING_CSV_FILE)
    )

    return core.extract_and_load(
        get_pv_site_by_system_id,
        get_extractor,
        source_descriptor,
        staging_fs,
        pv_system_id,
        date_range,
        overwrite,
        dry_run,
    )
