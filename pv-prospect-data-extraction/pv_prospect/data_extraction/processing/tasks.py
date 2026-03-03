"""Celery task wrappers — thin shims that delegate to :mod:`core`.

Kept for backward compatibility with the existing Celery-based local
development setup.  The Cloud Run path bypasses this module entirely.
"""
from pv_prospect.common import DateRange
from pv_prospect.data_extraction.extractors import SourceDescriptor
from pv_prospect.data_extraction.processing import core
from pv_prospect.data_extraction.processing.value_objects import Result
from pv_prospect.data_extraction.processing.worker import app

# Re-export constants so existing imports (e.g. task_producer) keep working.
PV_SITES_CSV_FILE = core.PV_SITES_CSV_FILE
OM_BOUNDING_BOXES_CSV_FILE = core.OM_BOUNDING_BOXES_CSV_FILE
SUPPORTING_RESOURCES = core.SUPPORTING_RESOURCES
DVC_FILE_PATH = core.DVC_FILE_PATH


@app.task
def preprocess(
        source_descriptor: SourceDescriptor,
        local_dir: str | None,
        include_metadata: bool,
) -> list[str]:
    return core.preprocess(source_descriptor, local_dir, include_metadata)


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
    return core.extract_and_load(
        source_descriptor, pv_system_id, date_range,
        local_dir, write_metadata, overwrite, dry_run,
    )
