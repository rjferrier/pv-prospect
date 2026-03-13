"""Celery task wrappers — thin shims that delegate to :mod:`core`.

Kept for backward compatibility with the existing Celery-based local
development setup.  The Cloud Run path bypasses this module entirely.
"""

from pv_prospect.common import (
    DateRange,
    build_location_mapping_repo,
    build_pv_site_repo,
    get_pv_site_by_system_id,
)
from pv_prospect.common.config_parser import get_config
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.extractors import SourceDescriptor, get_extractor
from pv_prospect.data_extraction.processing import core
from pv_prospect.data_extraction.processing.value_objects import Result
from pv_prospect.data_extraction.processing.worker import app
from pv_prospect.etl.extract.resolve import dvc
from pv_prospect.etl.factory import get_extractor as get_storage_extractor
from pv_prospect.etl.factory import get_loader as get_storage_loader
from pv_prospect.etl.storage_config import LocalStorageConfig

# Re-export constants so existing imports (e.g. task_producer) keep working.
PV_SITES_CSV_FILE = core.PV_SITES_CSV_FILE
LOCATION_MAPPING_CSV_FILE = core.LOCATION_MAPPING_CSV_FILE
SUPPORTING_RESOURCES = core.SUPPORTING_RESOURCES


def _resolve_storage(local_dir: str | None) -> tuple:
    """Resolve storage backends from config and optional local override."""
    config = get_config(DataExtractionConfig)
    staging_config = (
        LocalStorageConfig(prefix=local_dir)
        if local_dir
        else config.staged_raw_data_storage
    )
    return (
        config,
        get_storage_extractor(staging_config),
        get_storage_loader(staging_config),
    )


@app.task
def preprocess(
    source_descriptor: SourceDescriptor,
    local_dir: str | None,
) -> list[str | None]:
    config, _extractor, staging_loader = _resolve_storage(local_dir)
    versioned_extractor = get_storage_extractor(config.versioned_resources_storage)
    dvc_prefix = (
        config.versioned_resources_storage.tracking.prefix
        if config.versioned_resources_storage.tracking
        else ''
    )
    return core.preprocess(
        dvc.resolve_path,
        versioned_extractor,
        staging_loader,
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
    _config, staging_extractor, staging_loader = _resolve_storage(local_dir)

    build_pv_site_repo(staging_extractor.read_file(core.PV_SITES_CSV_FILE))
    build_location_mapping_repo(
        staging_extractor.read_file(core.LOCATION_MAPPING_CSV_FILE)
    )

    return core.extract_and_load(
        get_pv_site_by_system_id,
        get_extractor,
        source_descriptor,
        staging_extractor,
        staging_loader,
        pv_system_id,
        date_range,
        overwrite,
        dry_run,
    )
