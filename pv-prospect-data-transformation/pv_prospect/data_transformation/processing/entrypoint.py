"""Cloud Run Job entrypoint for Data Transformation.

Reads task parameters from environment variables set by the Cloud Workflow
and calls the corresponding core function.

Environment variables
---------------------
JOB_TYPE
    ``plan_transform``, ``plan_transform_backfill``,
    ``commit_transform_backfill``, or ``consolidate_logs``. If unset the
    job runs a single transform step (selected by ``TRANSFORM_STEP``).
BACKFILL_SCOPE
    Required for ``plan_transform_backfill`` and
    ``commit_transform_backfill``. ``pv_sites`` or ``weather_grid`` —
    selects which transform-backfill cursor to advance.
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

import json
import logging
import os
from datetime import date

from pv_prospect.common import (
    build_pv_site_repo,
    configure_logging,
    get_config,
    get_pv_site_by_system_id,
)
from pv_prospect.common.domain import DateRange
from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
from pv_prospect.data_sources import resolve_site
from pv_prospect.data_transformation.config import DataTransformationConfig
from pv_prospect.data_transformation.processing import (
    TRANSFORMATIONS_NEEDING_PV_SITE,
    Transformation,
    assemble_prepared_pv,
    assemble_prepared_weather,
    commit_transform_backfill,
    plan_transform_backfill,
    run_clean_pv,
    run_clean_weather,
    run_prepare_pv,
    run_prepare_weather,
)
from pv_prospect.data_transformation.resources import (
    get_config_dir as get_dt_config_dir,
)
from pv_prospect.etl import (
    BackfillScope,
    DegenerateDateRange,
    Extractor,
    WorkflowOrchestrator,
    WorkflowTerminatingError,
    build_date_range,
    build_env_list,
    inject_task_hash,
    run_entrypoint,
)
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import (
    AnyStorageConfig,
    FileSystem,
    LoggingFileSystem,
    consolidate_ledger,
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
    run_date: str,
    label: str,
) -> FileSystem:
    fs: FileSystem = get_filesystem(storage_config)
    if workflow_name and log_storage:
        log_fs = get_filesystem(log_storage)
        return LoggingFileSystem(fs, log_fs, workflow_name, run_date, label)
    logger.warning(
        'Write-logging disabled for %s (no WORKFLOW_NAME or log_storage)', label
    )
    return fs


def _run_consolidate_logs(config: DataTransformationConfig, run_date: str) -> None:
    workflow_name = os.environ.get('WORKFLOW_NAME', '')
    if not workflow_name:
        logger.warning('consolidate_logs: WORKFLOW_NAME not configured')
        return
    run_date_obj = date.fromisoformat(run_date)
    if config.log_storage:
        log_fs = get_filesystem(config.log_storage)
        consolidate_logs(log_fs, workflow_name, run_date_obj)
    if config.ledger_storage:
        ledger_fs = get_filesystem(config.ledger_storage)
        consolidate_ledger(ledger_fs, workflow_name, run_date_obj)


def _resolve_run_date() -> str:
    """Return the workflow's UTC trigger date.

    Read from ``RUN_DATE`` (set once by the Cloud Workflow ``init`` step
    and propagated to every task); fall back to ``date.today()`` for
    local one-off invocations.
    """
    return os.environ.get('RUN_DATE') or date.today().isoformat()


def _required(fs: FileSystem | None, name: str) -> FileSystem:
    if fs is None:
        raise WorkflowTerminatingError(
            f'{name} is required for this JOB_TYPE but not configured'
        )
    return fs


def _build_transform_descriptor(
    transformation: 'Transformation',
    start_date_str: str,
    pv_system_id: int | None,
    location_str: str | None,
) -> dict[str, str]:
    descriptor: dict[str, str] = {
        'transform_step': transformation.value,
        'start_date': start_date_str,
    }
    if pv_system_id is not None:
        descriptor['pv_system_id'] = str(pv_system_id)
    if location_str:
        descriptor['location'] = location_str
    end_date = os.environ.get('END_DATE')
    if end_date:
        descriptor['end_date'] = end_date
    return descriptor


def main() -> None:
    job_type = os.environ.get('JOB_TYPE', '')
    run_date = _resolve_run_date()

    config = get_config(
        DataTransformationConfig,
        base_config_dirs=[
            get_etl_config_dir(),
            get_ds_config_dir(),
            get_dt_config_dir(),
        ],
    )

    manifests_fs = (
        get_filesystem(config.manifests_storage) if config.manifests_storage else None
    )
    cursors_fs = (
        get_filesystem(config.cursors_storage) if config.cursors_storage else None
    )
    ledger_fs = get_filesystem(config.ledger_storage) if config.ledger_storage else None

    if job_type == 'plan_transform':
        _run_plan_transform(
            run_date, _required(manifests_fs, 'manifests_storage'), ledger_fs
        )
        return
    elif job_type == 'plan_transform_backfill':
        _run_plan_transform_backfill(
            run_date,
            _required(cursors_fs, 'cursors_storage'),
            _required(manifests_fs, 'manifests_storage'),
        )
        return
    elif job_type == 'commit_transform_backfill':
        _run_commit_transform_backfill(
            run_date,
            _required(cursors_fs, 'cursors_storage'),
            _required(manifests_fs, 'manifests_storage'),
        )
        return
    elif job_type == 'consolidate_logs':
        _run_consolidate_logs(config, run_date)
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
        raise WorkflowTerminatingError(str(e)) from e

    resources_fs = get_filesystem(config.resources_storage)
    raw_fs = get_filesystem(config.staged_raw_data_storage)
    cleaned_fs = _get_logging_filesystem(
        config.staged_cleaned_data_storage,
        config.log_storage,
        workflow_name,
        run_date,
        'cleaned',
    )
    batches_fs = _get_logging_filesystem(
        config.staged_prepared_batches_data_storage,
        config.log_storage,
        workflow_name,
        run_date,
        'prepared-batches',
    )
    prepared_fs = _get_logging_filesystem(
        config.staged_prepared_data_storage,
        config.log_storage,
        workflow_name,
        run_date,
        'prepared',
    )

    _load_resources(resources_fs)

    if transformation in TRANSFORMATIONS_NEEDING_PV_SITE and pv_system_id is None:
        raise ValueError('PV_SYSTEM_ID must be set for PV steps.')

    logger.info('Starting %s for %s, split_by=%s', transformation, date_range, split_by)

    task_hash = os.environ.get('TASK_HASH', '')
    descriptor = _build_transform_descriptor(
        transformation, start_date_str, pv_system_id, location_str
    )
    orchestrator = WorkflowOrchestrator(workflow_name, run_date, ledger_fs=ledger_fs)

    try:
        _run_transform_step(
            transformation,
            raw_fs,
            cleaned_fs,
            batches_fs,
            prepared_fs,
            config,
            pv_system_id,
            location_str,
            date_range,
            split_by,
        )
    except Exception as e:
        orchestrator.record_outcome(task_hash, descriptor, 'failed', error=repr(e))
        raise

    orchestrator.record_outcome(task_hash, descriptor, 'completed')


def _run_transform_step(
    transformation: 'Transformation',
    raw_fs: FileSystem,
    cleaned_fs: FileSystem,
    batches_fs: FileSystem,
    prepared_fs: FileSystem,
    config: DataTransformationConfig,
    pv_system_id: int | None,
    location_str: str | None,
    date_range: DateRange,
    split_by: str | None,
) -> None:
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
        raise WorkflowTerminatingError(f'unknown TRANSFORM_STEP={transformation}')


def _transform_task_env(
    transform_step: str,
    start_date_str: str,
    end_date_str: str | None,
    split_by: str,
    workflow_name: str,
    run_date: str,
    pv_system_id: int | None = None,
    location: str | None = None,
) -> list[dict[str, str]]:
    """Build the env list for one transform task, with TASK_HASH injected.

    ``END_DATE`` is included only when *end_date_str* is set, so the
    container processes the whole ``[START_DATE, END_DATE)`` window. When
    omitted the container falls back to a single day — the daily-transform
    behaviour.
    """
    env: dict[str, str] = {
        'TRANSFORM_STEP': transform_step,
        'DATE': start_date_str,
        'START_DATE': start_date_str,
        'SPLIT_BY': split_by,
        'WORKFLOW_NAME': workflow_name,
        'RUN_DATE': run_date,
    }
    if end_date_str:
        env['END_DATE'] = end_date_str
    if pv_system_id is not None:
        env['PV_SYSTEM_ID'] = str(pv_system_id)
    if location is not None:
        env['LOCATION'] = location
    return inject_task_hash(build_env_list(**env))


def build_transform_phases(
    workflow_name: str,
    start_date_str: str,
    end_date_str: str | None,
    pv_system_ids: list[int],
    locations: list[str],
    split_by: str,
    run_date: str,
) -> list[list[list[dict[str, str]]]]:
    """Enumerate the clean / prepare / assemble phases for a transform run.

    Every task env carries the full ``[START_DATE, END_DATE)`` window so the
    container transforms the whole range rather than only the start date;
    ``END_DATE`` is omitted only when *end_date_str* is falsy, which the
    container reads as a single day. The returned phases are unfiltered —
    the caller applies :meth:`WorkflowOrchestrator.filter_remaining_tasks`.
    """

    def task(
        transform_step: str,
        pv_system_id: int | None = None,
        location: str | None = None,
    ) -> list[dict[str, str]]:
        return _transform_task_env(
            transform_step,
            start_date_str,
            end_date_str,
            split_by,
            workflow_name,
            run_date,
            pv_system_id=pv_system_id,
            location=location,
        )

    clean: list[list[dict[str, str]]] = []
    prepare: list[list[dict[str, str]]] = []
    for pv_id in pv_system_ids:
        clean.append(task('clean_pv', pv_system_id=pv_id))
        clean.append(task('clean_weather', pv_system_id=pv_id))
        prepare.append(task('prepare_pv', pv_system_id=pv_id))
        prepare.append(task('prepare_weather', pv_system_id=pv_id))
    for loc in locations:
        clean.append(task('clean_weather', location=loc))
        prepare.append(task('prepare_weather', location=loc))

    assemble: list[list[dict[str, str]]] = [
        task('assemble_pv', pv_system_id=pv_id) for pv_id in pv_system_ids
    ]
    assemble.append(task('assemble_weather'))

    return [clean, prepare, assemble]


def _run_plan_transform(
    run_date: str,
    manifests_fs: FileSystem,
    ledger_fs: FileSystem | None = None,
) -> None:
    workflow_name = os.environ.get('WORKFLOW_NAME', 'pv-prospect-transform')
    start_date_str = (
        os.environ.get('START_DATE')
        or os.environ.get('DATE')
        or date.today().isoformat()
    )
    end_date_str = os.environ.get('END_DATE')

    pv_system_ids = json.loads(os.environ.get('PV_SYSTEM_IDS', '[]'))
    locations = json.loads(os.environ.get('LOCATIONS', '[]'))
    split_by = os.environ.get('SPLIT_BY', '')

    orchestrator = WorkflowOrchestrator(
        workflow_name, run_date, manifests_fs=manifests_fs, ledger_fs=ledger_fs
    )

    phases = build_transform_phases(
        workflow_name,
        start_date_str,
        end_date_str,
        pv_system_ids,
        locations,
        split_by,
        run_date,
    )
    filtered = [orchestrator.filter_remaining_tasks(phase) for phase in phases]
    orchestrator.write_manifest(filtered)
    logger.info('plan_transform: wrote manifest with %d phases', len(filtered))


def parse_backfill_scope(raw: str) -> BackfillScope:
    """Parse a ``BACKFILL_SCOPE`` env-var value, raising on invalid input."""
    try:
        return BackfillScope(raw)
    except ValueError as e:
        raise ValueError(
            f'BACKFILL_SCOPE must be one of '
            f'{[s.value for s in BackfillScope]!r}; got {raw!r}'
        ) from e


def _run_plan_transform_backfill(
    run_date: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
) -> None:
    scope = parse_backfill_scope(os.environ.get('BACKFILL_SCOPE', ''))
    today = date.today()
    plan = plan_transform_backfill(scope, today, run_date, cursors_fs, manifests_fs)
    logger.info(
        'plan_transform_backfill[%s]: wrote manifest (%s to %s)',
        scope.value,
        plan.start_date,
        plan.end_date,
    )


def _run_commit_transform_backfill(
    run_date: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
) -> None:
    scope = parse_backfill_scope(os.environ.get('BACKFILL_SCOPE', ''))
    cursor = commit_transform_backfill(scope, run_date, cursors_fs, manifests_fs)
    logger.info(
        'commit_transform_backfill[%s]: advanced cursor to %s',
        scope.value,
        cursor.next_end_date,
    )


if __name__ == '__main__':
    configure_logging()
    run_entrypoint(main)
