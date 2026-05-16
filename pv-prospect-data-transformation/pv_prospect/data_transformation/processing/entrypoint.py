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
    selects which transform backfill (and its consumed-through marker) to
    plan or commit.
MAX_EXTRACT_RUNS
    (Optional, ``plan_transform_backfill`` only) how many unconsumed
    extraction consolidated ledgers one run may consume. Defaults to 4.
TRANSFORM_STEP
    ``clean_weather``, ``clean_pv``, ``prepare_weather``, ``prepare_pv``,
    ``assemble_weather``, or ``assemble_pv``
START_DATE
    ISO date ``YYYY-MM-DD`` (start of the date range to process).
    Alias: ``DATE`` (clearer when no end date is given).
END_DATE
    ISO date ``YYYY-MM-DD``, exclusive (optional; defaults to
    START_DATE + 1 day)
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
from dataclasses import dataclass
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
    TransformUnit,
    assemble_prepared_pv,
    assemble_prepared_weather,
    build_transform_phases,
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
    run_label: str,
) -> FileSystem:
    fs: FileSystem = get_filesystem(storage_config)
    if workflow_name and log_storage:
        log_fs = get_filesystem(log_storage)
        return LoggingFileSystem(
            fs, log_fs, workflow_name, run_date, label, run_label=run_label
        )
    logger.warning(
        'Write-logging disabled for %s (no WORKFLOW_NAME or log_storage)', label
    )
    return fs


def _run_consolidate_logs(config: DataTransformationConfig, run_date: str) -> None:
    workflow_name = os.environ.get('WORKFLOW_NAME', '')
    if not workflow_name:
        logger.warning('consolidate_logs: WORKFLOW_NAME not configured')
        return
    run_label = os.environ.get('RUN_LABEL', '')
    run_date_obj = date.fromisoformat(run_date)
    if config.log_storage:
        log_fs = get_filesystem(config.log_storage)
        consolidate_logs(log_fs, workflow_name, run_date_obj, run_label=run_label)
    if config.ledger_storage:
        ledger_fs = get_filesystem(config.ledger_storage)
        consolidate_ledger(ledger_fs, workflow_name, run_date_obj, run_label=run_label)


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
            _required(ledger_fs, 'ledger_storage'),
            _required(manifests_fs, 'manifests_storage'),
            _required(cursors_fs, 'cursors_storage'),
        )
        return
    elif job_type == 'commit_transform_backfill':
        _run_commit_transform_backfill(
            run_date,
            _required(manifests_fs, 'manifests_storage'),
            _required(cursors_fs, 'cursors_storage'),
        )
        return
    elif job_type == 'consolidate_logs':
        _run_consolidate_logs(config, run_date)
        return
    elif job_type == 'process_transform_chunk':
        _run_process_transform_chunk(
            run_date,
            config,
            _required(manifests_fs, 'manifests_storage'),
            ledger_fs,
        )
        return

    # Per-task path (legacy daily-transform dispatch shape: one Cloud Run
    # task per transform unit). The chunk path above is what the transform
    # backfill uses now — see `_run_process_transform_chunk` for why.
    workflow_name = os.environ.get('WORKFLOW_NAME', '')
    run_label = os.environ.get('RUN_LABEL', '')
    shared = _build_chunk_runtime(config, workflow_name, run_date, run_label)
    orchestrator = WorkflowOrchestrator(
        workflow_name, run_date, ledger_fs=ledger_fs, run_label=run_label
    )
    _run_one_transform_unit(_task_env_from_environ(), shared, config, orchestrator)


def _task_env_from_environ() -> dict[str, str]:
    """Snapshot the per-task env vars from ``os.environ`` into a plain dict.

    Keeps the per-task path symmetrical with the chunk path, which builds
    the same dict by reconstructing common_env + zip(task_keys, row).
    """
    keys = (
        'TRANSFORM_STEP',
        'PV_SYSTEM_ID',
        'LOCATION',
        'START_DATE',
        'DATE',
        'END_DATE',
        'TASK_HASH',
    )
    return {k: os.environ[k] for k in keys if k in os.environ}


@dataclass(frozen=True)
class _ChunkRuntime:
    """Shared per-chunk resources. Built once, reused for every unit in the
    chunk to amortise config loading, GCS handle setup, and the
    PV-site/location-mapping repo load."""

    raw_fs: FileSystem
    cleaned_fs: FileSystem
    batches_fs: FileSystem
    prepared_fs: FileSystem


def _build_chunk_runtime(
    config: DataTransformationConfig,
    workflow_name: str,
    run_date: str,
    run_label: str,
) -> _ChunkRuntime:
    resources_fs = get_filesystem(config.resources_storage)
    raw_fs = get_filesystem(config.staged_raw_data_storage)
    cleaned_fs = _get_logging_filesystem(
        config.staged_cleaned_data_storage,
        config.log_storage,
        workflow_name,
        run_date,
        'cleaned',
        run_label,
    )
    batches_fs = _get_logging_filesystem(
        config.staged_prepared_batches_data_storage,
        config.log_storage,
        workflow_name,
        run_date,
        'prepared-batches',
        run_label,
    )
    prepared_fs = _get_logging_filesystem(
        config.staged_prepared_data_storage,
        config.log_storage,
        workflow_name,
        run_date,
        'prepared',
        run_label,
    )
    _load_resources(resources_fs)
    return _ChunkRuntime(raw_fs, cleaned_fs, batches_fs, prepared_fs)


def _run_one_transform_unit(
    task_env: dict[str, str],
    shared: _ChunkRuntime,
    config: DataTransformationConfig,
    orchestrator: WorkflowOrchestrator,
) -> None:
    """Run a single transform unit described by *task_env*.

    *task_env* is the per-task env-var dict — either parsed from
    ``os.environ`` (daily-transform per-task dispatch) or reconstructed
    from a manifest row plus the phase's hoisted common env (transform-
    backfill chunk dispatch). The shared runtime + config are loaded
    once per Cloud Run execution; the orchestrator owns the ledger
    writes.

    Records a ``failed`` ledger entry and re-raises on any exception,
    or a ``completed`` entry on success.
    """
    transformation = Transformation(task_env.get('TRANSFORM_STEP', ''))
    pv_system_id = (
        int(task_env['PV_SYSTEM_ID']) if task_env.get('PV_SYSTEM_ID') else None
    )
    location_str = task_env.get('LOCATION')

    start_date_str = task_env.get('START_DATE') or task_env.get('DATE')
    if not start_date_str:
        raise ValueError('START_DATE (or DATE) must be set.')

    try:
        date_range = build_date_range(start_date_str, task_env.get('END_DATE'))
    except DegenerateDateRange as e:
        raise WorkflowTerminatingError(str(e)) from e

    if transformation in TRANSFORMATIONS_NEEDING_PV_SITE and pv_system_id is None:
        raise ValueError('PV_SYSTEM_ID must be set for PV steps.')

    task_hash = task_env.get('TASK_HASH', '')
    descriptor: dict[str, str] = {
        'transform_step': transformation.value,
        'start_date': start_date_str,
    }
    if pv_system_id is not None:
        descriptor['pv_system_id'] = str(pv_system_id)
    if location_str:
        descriptor['location'] = location_str
    if task_env.get('END_DATE'):
        descriptor['end_date'] = task_env['END_DATE']

    logger.info('Starting %s for %s', transformation, date_range)

    try:
        _run_transform_step(
            transformation,
            shared.raw_fs,
            shared.cleaned_fs,
            shared.batches_fs,
            shared.prepared_fs,
            config,
            pv_system_id,
            location_str,
            date_range,
        )
    except Exception as e:
        orchestrator.record_outcome(task_hash, descriptor, 'failed', error=repr(e))
        raise

    orchestrator.record_outcome(task_hash, descriptor, 'completed')


def _run_process_transform_chunk(
    run_date: str,
    config: DataTransformationConfig,
    manifests_fs: FileSystem,
    ledger_fs: FileSystem | None,
) -> None:
    """Run every transform unit listed in one manifest phase-part file.

    The transform-backfill workflow's queue routinely exceeds Cloud
    Workflows' 100K-step-per-execution ceiling when each unit is a
    separate Cloud Run dispatch (~50 workflow steps/unit × tens of
    thousands of units). The fix is to dispatch one Cloud Run execution
    per chunk file and process the chunk's rows in-container, so the
    workflow's step count scales with chunk count (hundreds) not unit
    count (tens of thousands).

    Required env vars:

    * ``CHUNK_FILE``  -- relative path of the chunk file on
      ``manifests_fs`` (e.g.
      ``<run_date>/<workflow>.phase-0.part-3.json``).
    * ``INDEX_FILE``  -- relative path of the manifest index, used to
      recover the phase-level ``common_env`` and ``task_keys`` schema
      so the chunk file can stay row-only.
    * ``PHASE_INDEX`` -- which phase this chunk belongs to, indexing
      ``index.phases[]``.

    Per-unit ``Exception`` failures are logged and swallowed (the
    orchestrator's ledger entry already records ``failed`` for that
    unit's task_hash), so the chunk runs to completion and other units
    aren't blocked. ``WorkflowTerminatingError`` propagates and aborts
    the chunk (Cloud Run exit 2 → workflow's outer except re-raises
    and skips the commit step).
    """
    chunk_file = os.environ.get('CHUNK_FILE')
    if not chunk_file:
        raise WorkflowTerminatingError(
            'CHUNK_FILE is required for process_transform_chunk'
        )
    index_file = os.environ.get('INDEX_FILE')
    if not index_file:
        raise WorkflowTerminatingError(
            'INDEX_FILE is required for process_transform_chunk'
        )
    phase_index_raw = os.environ.get('PHASE_INDEX')
    if phase_index_raw is None:
        raise WorkflowTerminatingError(
            'PHASE_INDEX is required for process_transform_chunk'
        )
    phase_index = int(phase_index_raw)

    workflow_name = os.environ.get('WORKFLOW_NAME', '')
    run_label = os.environ.get('RUN_LABEL', '')

    index = json.loads(manifests_fs.read_text(index_file))
    phase = index['phases'][phase_index]
    common_env: dict[str, str] = {e['name']: e['value'] for e in phase['common_env']}
    task_keys: list[str] = phase['task_keys']

    chunk = json.loads(manifests_fs.read_text(chunk_file))
    rows: list[list[str | None]] = chunk['rows']

    shared = _build_chunk_runtime(config, workflow_name, run_date, run_label)
    orchestrator = WorkflowOrchestrator(
        workflow_name, run_date, ledger_fs=ledger_fs, run_label=run_label
    )

    logger.info(
        'process_transform_chunk: %d units in phase %d (%s)',
        len(rows),
        phase_index,
        chunk_file,
    )
    succeeded = 0
    failed = 0
    for row in rows:
        task_env = dict(common_env)
        for k, v in zip(task_keys, row, strict=True):
            if v is not None:
                task_env[k] = v
        try:
            _run_one_transform_unit(task_env, shared, config, orchestrator)
            succeeded += 1
        except WorkflowTerminatingError:
            # Terminating errors abort the whole chunk so the outer
            # workflow leaves the marker unchanged. Re-raise to main().
            raise
        except Exception:
            # Non-terminating failure: ledger already recorded 'failed';
            # log and continue so the rest of the chunk still runs.
            logger.exception('Unit failed; continuing with next unit')
            failed += 1
    logger.info(
        'process_transform_chunk: %d succeeded, %d failed (chunk %s)',
        succeeded,
        failed,
        chunk_file,
    )


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

    # The daily transform plans one window for the configured systems and
    # grid points: a PV + a weather unit per system, a weather unit per
    # location. All units share the single [START_DATE, END_DATE) window.
    units: list[TransformUnit] = []
    for pv_id in pv_system_ids:
        units.append(
            TransformUnit('pv', start_date_str, end_date_str, pv_system_id=pv_id)
        )
        units.append(
            TransformUnit('weather', start_date_str, end_date_str, pv_system_id=pv_id)
        )
    for loc in locations:
        units.append(
            TransformUnit('weather', start_date_str, end_date_str, location=loc)
        )

    run_label = os.environ.get('RUN_LABEL', '')
    orchestrator = WorkflowOrchestrator(
        workflow_name,
        run_date,
        manifests_fs=manifests_fs,
        ledger_fs=ledger_fs,
        run_label=run_label,
    )

    phases = build_transform_phases(units, workflow_name, run_date)
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


_DEFAULT_MAX_EXTRACT_RUNS = 4


def _run_plan_transform_backfill(
    run_date: str,
    ledger_fs: FileSystem,
    manifests_fs: FileSystem,
    cursors_fs: FileSystem,
) -> None:
    scope = parse_backfill_scope(os.environ.get('BACKFILL_SCOPE', ''))
    max_extract_runs = int(
        os.environ.get('MAX_EXTRACT_RUNS') or _DEFAULT_MAX_EXTRACT_RUNS
    )
    next_marker = plan_transform_backfill(
        scope, run_date, ledger_fs, manifests_fs, cursors_fs, max_extract_runs
    )
    logger.info(
        'plan_transform_backfill[%s]: wrote manifest, next_marker=%r',
        scope.value,
        next_marker,
    )


def _run_commit_transform_backfill(
    run_date: str,
    manifests_fs: FileSystem,
    cursors_fs: FileSystem,
) -> None:
    scope = parse_backfill_scope(os.environ.get('BACKFILL_SCOPE', ''))
    next_marker = commit_transform_backfill(scope, run_date, manifests_fs, cursors_fs)
    logger.info(
        'commit_transform_backfill[%s]: advanced marker to %r',
        scope.value,
        next_marker,
    )


if __name__ == '__main__':
    configure_logging()
    run_entrypoint(main)
