"""Cloud Run Job entrypoint for Data Transformation.

Reads task parameters from environment variables set by the Cloud Workflow
and calls the corresponding core function.

Environment variables
---------------------
TRANSFORM_STEP
    ``clean_weather``, ``clean_pv``, ``prepare_weather``, ``prepare_pv``,
    ``assemble_weather``, or ``assemble_pv``
START_DATE
    ISO date ``YYYY-MM-DD`` (start of the date range to process).
    Alias: ``DATE`` (clearer when no end date is given).
END_DATE
    ISO date ``YYYY-MM-DD``, exclusive (optional; defaults to
    START_DATE + 1 day)
SPLIT_BY
    ``day`` or ``week`` (omit to use the full date range as a single chunk).
    When ``week``, ``clean_weather`` reads a single raw file spanning the
    week and writes per-day cleaned files.  Other steps always iterate per day.
PV_SYSTEM_ID
    (Optional) integer system id; required for pv steps. For weather steps,
    accepted as an alternative to ``LOCATION`` — the location is
    derived via the location mapping repo.
LOCATION
    (Optional) comma-separated lat,lon (e.g. ``50.49,-3.54``); required for
    weather steps unless ``PV_SYSTEM_ID`` is provided instead. Exactly one
    of the two must be set.
"""

import logging
import os
import sys
from datetime import date

from pv_prospect.common import (
    build_pv_site_repo,
    configure_logging,
    get_config,
    get_pv_site_by_system_id,
)
from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
from pv_prospect.data_sources import resolve_site
from pv_prospect.data_transformation.config import DataTransformationConfig
from pv_prospect.data_transformation.core import (
    assemble_prepared_pv,
    assemble_prepared_weather,
    run_clean_pv,
    run_clean_weather,
    run_prepare_pv,
    run_prepare_weather,
)
from pv_prospect.data_transformation.resources import (
    get_config_dir as get_dt_config_dir,
)
from pv_prospect.data_transformation.transformation import (
    TRANSFORMATIONS_NEEDING_PV_SITE,
    Transformation,
)
from pv_prospect.etl import DegenerateDateRange, Extractor, build_date_range
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import (
    AnyStorageConfig,
    FileSystem,
    LoggingFileSystem,
    consolidate_logs,
    get_filesystem,
)

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).lower() in ('true', '1', 'yes')


def _env_int(name: str) -> int | None:
    val = os.environ.get(name)
    return int(val) if val else None


def _load_resources(resources_fs: FileSystem) -> None:
    """Load the PV site and location mapping repos from the raw data bucket."""
    extractor = Extractor(resources_fs)

    if extractor.file_exists('pv_sites.csv'):
        build_pv_site_repo(extractor.read_file('pv_sites.csv'))


def _get_logging_filesystem(
    storage_config: 'AnyStorageConfig',
    log_storage: 'AnyStorageConfig | None',
    workflow_name: str,
    label: str,
) -> FileSystem:
    fs: FileSystem = get_filesystem(storage_config)
    if workflow_name and log_storage:
        log_fs = get_filesystem(log_storage)
        return LoggingFileSystem(fs, log_fs, workflow_name, label)
    logger.warning(
        'Write-logging disabled for %s (no WORKFLOW_NAME or log_storage)', label
    )
    return fs


def _run_consolidate_logs(config: DataTransformationConfig) -> None:
    workflow_name = os.environ.get('WORKFLOW_NAME', '')
    if not workflow_name or not config.log_storage:
        logger.warning('consolidate_logs: WORKFLOW_NAME or log_storage not configured')
        return
    log_fs = get_filesystem(config.log_storage)
    today = date.today()
    consolidate_logs(log_fs, workflow_name, today)


def main() -> None:
    job_type = os.environ.get('JOB_TYPE', '')

    config = get_config(
        DataTransformationConfig,
        base_config_dirs=[
            get_etl_config_dir(),
            get_ds_config_dir(),
            get_dt_config_dir(),
        ],
    )

    if job_type == 'plan_transform':
        resources_fs = get_filesystem(config.resources_storage)
        _run_plan_transform(resources_fs)
        return
    elif job_type == 'consolidate_logs':
        _run_consolidate_logs(config)
        return

    transformation = Transformation(os.environ.get('TRANSFORM_STEP', ''))
    workflow_name = os.environ.get('WORKFLOW_NAME', '')

    split_by = os.environ.get('SPLIT_BY')
    pv_system_id = _env_int('PV_SYSTEM_ID')
    location_str = os.environ.get('LOCATION')

    start_date_str = os.environ.get('START_DATE') or os.environ.get('DATE')
    if not start_date_str:
        raise ValueError('START_DATE (or DATE) must be set.')

    try:
        date_range = build_date_range(start_date_str, os.environ.get('END_DATE'))
    except DegenerateDateRange as e:
        logger.error('%s', e)
        sys.exit(1)

    resources_fs = get_filesystem(config.resources_storage)
    raw_fs = get_filesystem(config.staged_raw_data_storage)
    cleaned_fs = _get_logging_filesystem(
        config.staged_cleaned_data_storage, config.log_storage, workflow_name, 'cleaned'
    )
    batches_fs = _get_logging_filesystem(
        config.staged_prepared_batches_data_storage,
        config.log_storage,
        workflow_name,
        'prepared-batches',
    )
    prepared_fs = _get_logging_filesystem(
        config.staged_prepared_data_storage,
        config.log_storage,
        workflow_name,
        'prepared',
    )

    _load_resources(resources_fs)

    if transformation in TRANSFORMATIONS_NEEDING_PV_SITE and pv_system_id is None:
        raise ValueError('PV_SYSTEM_ID must be set for PV steps.')

    logger.info('Starting %s for %s, split_by=%s', transformation, date_range, split_by)

    if transformation is Transformation.CLEAN_WEATHER:
        site = resolve_site(
            config.data_sources.weather.type,
            get_pv_site_by_system_id,
            pv_system_id=pv_system_id,
            location_str=location_str,
        )
        run_clean_weather(
            raw_fs,
            cleaned_fs,
            config.data_sources.weather,
            site,
            date_range,
            split_by == 'week',
        )

    elif transformation is Transformation.CLEAN_PV:
        pv_site = get_pv_site_by_system_id(pv_system_id)  # type: ignore[arg-type]
        run_clean_pv(
            raw_fs,
            cleaned_fs,
            config.data_sources.pv,
            pv_site,
            date_range,
        )

    elif transformation is Transformation.PREPARE_WEATHER:
        site = resolve_site(
            config.data_sources.weather.type,
            get_pv_site_by_system_id,
            pv_system_id=pv_system_id,
            location_str=location_str,
        )
        run_prepare_weather(
            cleaned_fs,
            batches_fs,
            config.data_sources.weather,
            site,
            date_range,
        )

    elif transformation is Transformation.PREPARE_PV:
        pv_site = get_pv_site_by_system_id(pv_system_id)  # type: ignore[arg-type]
        run_prepare_pv(
            cleaned_fs,
            batches_fs,
            config.data_sources.pv,
            config.data_sources.weather,
            pv_site,
            date_range,
            get_pv_site_by_system_id,
        )

    elif transformation is Transformation.ASSEMBLE_WEATHER:
        assemble_prepared_weather(batches_fs, prepared_fs)

    elif transformation is Transformation.ASSEMBLE_PV:
        assemble_prepared_pv(batches_fs, prepared_fs, pv_system_id)  # type: ignore[arg-type]

    else:
        logger.error('unknown TRANSFORM_STEP=%s', transformation)
        sys.exit(1)

    task_hash = os.environ.get('TASK_HASH')
    if task_hash:
        start_date_str = (
            os.environ.get('START_DATE')
            or os.environ.get('DATE')
            or date.today().isoformat()
        )
        from pv_prospect.etl import WorkflowOrchestrator

        orchestrator = WorkflowOrchestrator(resources_fs, workflow_name, start_date_str)
        orchestrator.mark_task_completed(task_hash)


def _run_plan_transform(resources_fs: FileSystem) -> None:
    import json

    from pv_prospect.etl import WorkflowOrchestrator, build_env_list, inject_task_hash

    workflow_name = os.environ.get('WORKFLOW_NAME', 'pv-prospect-transform')
    start_date_str = (
        os.environ.get('START_DATE')
        or os.environ.get('DATE')
        or date.today().isoformat()
    )

    pv_system_ids = json.loads(os.environ.get('PV_SYSTEM_IDS', '[]'))
    locations = json.loads(os.environ.get('LOCATIONS', '[]'))
    split_by = os.environ.get('SPLIT_BY', '')

    orchestrator = WorkflowOrchestrator(resources_fs, workflow_name, start_date_str)

    phases = []

    # Phase 0: Clean
    phase0 = []
    for pv_id in pv_system_ids:
        env = build_env_list(
            TRANSFORM_STEP='clean_pv',
            PV_SYSTEM_ID=str(pv_id),
            DATE=start_date_str,
            START_DATE=start_date_str,
            SPLIT_BY=split_by,
            WORKFLOW_NAME=workflow_name,
        )
        phase0.append(inject_task_hash(env))

        env = build_env_list(
            TRANSFORM_STEP='clean_weather',
            PV_SYSTEM_ID=str(pv_id),
            DATE=start_date_str,
            START_DATE=start_date_str,
            SPLIT_BY=split_by,
            WORKFLOW_NAME=workflow_name,
        )
        phase0.append(inject_task_hash(env))

    for loc in locations:
        env = build_env_list(
            TRANSFORM_STEP='clean_weather',
            LOCATION=loc,
            DATE=start_date_str,
            START_DATE=start_date_str,
            SPLIT_BY=split_by,
            WORKFLOW_NAME=workflow_name,
        )
        phase0.append(inject_task_hash(env))

    phases.append(orchestrator.filter_remaining_tasks(phase0))

    # Phase 1: Prepare
    phase1 = []
    for pv_id in pv_system_ids:
        env = build_env_list(
            TRANSFORM_STEP='prepare_pv',
            PV_SYSTEM_ID=str(pv_id),
            DATE=start_date_str,
            START_DATE=start_date_str,
            SPLIT_BY=split_by,
            WORKFLOW_NAME=workflow_name,
        )
        phase1.append(inject_task_hash(env))

        env = build_env_list(
            TRANSFORM_STEP='prepare_weather',
            PV_SYSTEM_ID=str(pv_id),
            DATE=start_date_str,
            START_DATE=start_date_str,
            SPLIT_BY=split_by,
            WORKFLOW_NAME=workflow_name,
        )
        phase1.append(inject_task_hash(env))

    for loc in locations:
        env = build_env_list(
            TRANSFORM_STEP='prepare_weather',
            LOCATION=loc,
            DATE=start_date_str,
            START_DATE=start_date_str,
            SPLIT_BY=split_by,
            WORKFLOW_NAME=workflow_name,
        )
        phase1.append(inject_task_hash(env))

    phases.append(orchestrator.filter_remaining_tasks(phase1))

    # Phase 2: Assemble
    phase2 = []
    for pv_id in pv_system_ids:
        env = build_env_list(
            TRANSFORM_STEP='assemble_pv',
            PV_SYSTEM_ID=str(pv_id),
            DATE=start_date_str,
            START_DATE=start_date_str,
            SPLIT_BY=split_by,
            WORKFLOW_NAME=workflow_name,
        )
        phase2.append(inject_task_hash(env))

    env = build_env_list(
        TRANSFORM_STEP='assemble_weather',
        DATE=start_date_str,
        START_DATE=start_date_str,
        SPLIT_BY=split_by,
        WORKFLOW_NAME=workflow_name,
    )
    phase2.append(inject_task_hash(env))

    phases.append(orchestrator.filter_remaining_tasks(phase2))

    orchestrator.write_manifest(phases)
    logger.info('plan_transform: wrote manifest with %d phases', len(phases))


if __name__ == '__main__':
    configure_logging()
    try:
        main()
    except Exception:
        logger.exception('Unhandled exception')
        sys.exit(1)
