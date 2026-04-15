"""Celery task wrappers — thin shims that delegate to :mod:`core`.

Kept for backward compatibility with the existing Celery-based local
development setup.  The Cloud Run path bypasses this module entirely.
"""

from pv_prospect.common import (
    build_pv_site_repo,
    get_config,
)
from pv_prospect.common.domain import (
    AnySite,
    DateRange,
)
from pv_prospect.data_extraction import DataSource, get_extractor
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.processing import core
from pv_prospect.data_extraction.processing.value_objects import Result
from pv_prospect.data_extraction.processing.worker import app
from pv_prospect.data_extraction.resources import get_config_dir as get_de_config_dir
from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
from pv_prospect.etl import Extractor
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import FileSystem, get_filesystem
from pv_prospect.etl.storage.backends import LocalStorageConfig

# Re-export constants so existing imports (e.g. task_producer) keep working.
PV_SITES_CSV_FILE = core.PV_SITES_CSV_FILE
SUPPORTING_RESOURCES = core.SUPPORTING_RESOURCES


def _resolve_storage(local_dir: str | None) -> tuple[DataExtractionConfig, FileSystem]:
    """Resolve storage backends from config and optional local override."""
    config = get_config(
        DataExtractionConfig,
        base_config_dirs=[
            get_etl_config_dir(),
            get_ds_config_dir(),
            get_de_config_dir(),
        ],
    )
    staging_config = (
        LocalStorageConfig(prefix=local_dir)
        if local_dir
        else config.staged_raw_data_storage
    )
    return config, get_filesystem(staging_config)


@app.task
def preprocess(
    data_source: DataSource,
    local_dir: str | None,
) -> list[str | None]:
    _config, staging_fs = _resolve_storage(local_dir)
    return core.preprocess(staging_fs, data_source)


@app.task
def extract_and_load(
    data_source: DataSource,
    site: AnySite,
    date_range: DateRange,
    local_dir: str | None,
    dry_run: bool,
) -> Result:
    config, staging_fs = _resolve_storage(local_dir)

    resources_fs = get_filesystem(config.resources_storage)
    resources_extractor = Extractor(resources_fs)
    build_pv_site_repo(resources_extractor.read_file(core.PV_SITES_CSV_FILE))

    return core.extract_and_load(
        get_extractor,
        data_source,
        staging_fs,
        site,
        date_range,
        dry_run,
    )
