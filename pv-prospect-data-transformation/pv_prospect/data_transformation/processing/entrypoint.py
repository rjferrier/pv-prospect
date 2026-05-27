"""Cloud Run Job entrypoint for Data Transformation.

Reads task parameters from environment variables and calls the
corresponding core function.

Environment variables
---------------------
JOB_TYPE
    ``plan_transform`` (daily transform planner),
    ``run_transform_backfill`` (in-container backfill: plan + run +
    commit + consolidate, all in one execution), or ``consolidate_logs``
    (daily transform's end-of-run step). If unset the job runs a single
    transform step (selected by ``TRANSFORM_STEP``) — the per-task
    dispatch shape used by the daily-transform workflow.
BACKFILL_SCOPE
    Required for ``run_transform_backfill``. ``pv_sites`` or
    ``weather_grid`` — selects which backfill (and its consumed-through
    marker) to run.
MAX_EXTRACT_RUNS
    (Optional, ``run_transform_backfill`` only) how many unconsumed
    extraction consolidated ledgers one run may consume. Defaults to 4.
MAX_WORKERS
    (Optional, ``run_transform_backfill`` only) thread-pool size used to
    parallelise the units within each phase. Defaults to 32. Transform
    work is GCS-bound, so threading (not multiprocessing) is the right
    primitive.
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
GRID_POINT_SAMPLE_INDEX
    (Optional) integer index of the grid-point sample file a grid-weather
    unit belongs to. Set only for the weather-grid backfill's
    ``prepare_weather`` tasks; it groups the prepared rows into one
    ``weather/`` partition file per (sample file, window).
"""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date

from pv_prospect.common import (
    build_pv_site_repo,
    configure_logging,
    get_config,
    get_pv_site_by_system_id,
)
from pv_prospect.common.domain import DateRange
from pv_prospect.data_sources import DataSourceType, resolve_site
from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
from pv_prospect.data_transformation.config import DataTransformationConfig
from pv_prospect.data_transformation.processing import (
    TRANSFORMATIONS_NEEDING_PV_SITE,
    ConsumedMarker,
    PreparedBatchCollector,
    SliceOutcome,
    Transformation,
    TransformInput,
    assemble_prepared_pv,
    assemble_prepared_weather,
    build_transform_phases,
    plan_slices,
    plan_units,
    produce_pv_slice,
    produce_weather_slice,
    run_clean_pv,
    run_clean_weather,
    run_prepare_pv,
    run_prepare_weather,
    save_marker,
    workflow_name_for,
)
from pv_prospect.data_transformation.resources import (
    get_config_dir as get_dt_config_dir,
)
from pv_prospect.etl import (
    BackfillScope,
    DegenerateDateRange,
    Extractor,
    PVSlice,
    WeatherSlice,
    WorkflowOrchestrator,
    WorkflowTerminatingError,
    build_date_range,
    build_env_list,
    compute_task_hash,
    get_logging_filesystem,
    read_sample_file,
    resolve_run_date,
    run_consolidate_logs,
    run_entrypoint,
    sample_file_path,
)
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import (
    FileSystem,
    LedgerCollector,
    LogCollector,
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


def _required(fs: FileSystem | None, name: str) -> FileSystem:
    if fs is None:
        raise WorkflowTerminatingError(
            f'{name} is required for this JOB_TYPE but not configured'
        )
    return fs


def main() -> None:
    job_type = os.environ.get('JOB_TYPE', '')
    run_date = resolve_run_date()

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
    elif job_type == 'run_transform_backfill':
        backfill_mode = os.environ.get('BACKFILL_MODE', 'push')
        if backfill_mode == 'pull':
            _run_transform_backfill_pull(
                run_date,
                config,
                _required(ledger_fs, 'ledger_storage'),
                _required(cursors_fs, 'cursors_storage'),
            )
        else:
            _run_transform_backfill(
                run_date,
                config,
                _required(ledger_fs, 'ledger_storage'),
                _required(cursors_fs, 'cursors_storage'),
            )
        return
    elif job_type == 'consolidate_logs':
        run_consolidate_logs(
            config.log_storage,
            config.ledger_storage,
            os.environ.get('WORKFLOW_NAME', ''),
            run_date,
            os.environ.get('RUN_LABEL', ''),
        )
        return

    # Per-task path: one Cloud Run task per transform unit. This shape is
    # how the daily-transform workflow still dispatches individual steps.
    workflow_name = os.environ.get('WORKFLOW_NAME', '')
    run_label = os.environ.get('RUN_LABEL', '')
    shared = _build_runtime(config, workflow_name, run_date, run_label)
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
        'GRID_POINT_SAMPLE_INDEX',
        'START_DATE',
        'DATE',
        'END_DATE',
        'TASK_HASH',
    )
    return {k: os.environ[k] for k in keys if k in os.environ}


@dataclass(frozen=True)
class _Runtime:
    """Shared per-execution resources. Built once, reused for every unit
    handled in the same Cloud Run Job execution to amortise config
    loading, GCS handle setup, and the PV-site / location-mapping repo
    load.

    ``prepared_batches`` is set only for the in-container backfill: it
    carries prepared frames from the ``prepare`` phase to the ``assemble``
    phase in memory, so no batch CSVs are written to ``batches_fs``. The
    daily transform leaves it ``None`` and uses ``batches_fs``."""

    raw_fs: FileSystem
    cleaned_fs: FileSystem
    batches_fs: FileSystem
    prepared_fs: FileSystem
    prepared_batches: PreparedBatchCollector | None = None


def _build_runtime(
    config: DataTransformationConfig,
    workflow_name: str,
    run_date: str,
    run_label: str,
    log_collector: LogCollector | None = None,
    prepared_batches: PreparedBatchCollector | None = None,
) -> _Runtime:
    resources_fs = get_filesystem(config.resources_storage)
    raw_fs = get_filesystem(config.staged_raw_data_storage)
    cleaned_fs = get_logging_filesystem(
        config.staged_cleaned_data_storage,
        config.log_storage,
        workflow_name,
        run_date,
        'cleaned',
        run_label,
        log_collector,
    )
    batches_fs = get_logging_filesystem(
        config.staged_prepared_batches_data_storage,
        config.log_storage,
        workflow_name,
        run_date,
        'prepared-batches',
        run_label,
        log_collector,
    )
    prepared_fs = get_logging_filesystem(
        config.staged_prepared_data_storage,
        config.log_storage,
        workflow_name,
        run_date,
        'prepared',
        run_label,
        log_collector,
    )
    _load_resources(resources_fs)
    return _Runtime(raw_fs, cleaned_fs, batches_fs, prepared_fs, prepared_batches)


def _run_one_transform_unit(
    task_env: dict[str, str],
    shared: _Runtime,
    config: DataTransformationConfig,
    orchestrator: WorkflowOrchestrator,
) -> None:
    """Run a single transform unit described by *task_env*.

    *task_env* is the per-task env-var dict — parsed from ``os.environ``
    in the daily-transform per-task dispatch, or built from a planned
    unit's env-list in the transform-backfill in-container handler. The
    shared runtime + config are loaded once per Cloud Run execution; the
    orchestrator owns the ledger writes.

    Records a ``failed`` ledger entry and re-raises on any exception,
    or a ``completed`` entry on success.
    """
    transformation = Transformation(task_env.get('TRANSFORM_STEP', ''))
    pv_system_id = (
        int(task_env['PV_SYSTEM_ID']) if task_env.get('PV_SYSTEM_ID') else None
    )
    location_str = task_env.get('LOCATION')
    grid_point_sample_index = (
        int(task_env['GRID_POINT_SAMPLE_INDEX'])
        if task_env.get('GRID_POINT_SAMPLE_INDEX')
        else None
    )

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
    if grid_point_sample_index is not None:
        descriptor['grid_point_sample_index'] = str(grid_point_sample_index)
    if task_env.get('END_DATE'):
        descriptor['end_date'] = task_env['END_DATE']

    logger.debug('Starting %s for %s', transformation, date_range)

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
            grid_point_sample_index,
            date_range,
            shared.prepared_batches,
        )
    except Exception as e:
        orchestrator.record_outcome(task_hash, descriptor, 'failed', error=repr(e))
        raise

    orchestrator.record_outcome(task_hash, descriptor, 'completed')


_DEFAULT_MAX_EXTRACT_RUNS = 4
_DEFAULT_MAX_WORKERS = 32


def _run_transform_backfill(
    run_date: str,
    config: DataTransformationConfig,
    ledger_fs: FileSystem,
    cursors_fs: FileSystem,
) -> None:
    """Plan, run, and commit a transform-backfill scope end-to-end.

    Reads the consumed-through marker, picks the next ``MAX_EXTRACT_RUNS``
    unconsumed extraction ledgers, turns their completed descriptors
    into transform units, runs the clean → prepare → assemble phases
    (each phase fanned out across a thread pool), writes the run's ledger
    and write-audit log as one consolidated file each, then advances the
    marker. The consolidated ledger records outcomes for cross-run
    resume; the marker only advances when every phase completes without a
    terminating error and both files are flushed.

    Because the run is a single process, task outcomes, log lines, and
    prepared batch frames are kept in memory (a :class:`LedgerCollector`,
    :class:`LogCollector`, and :class:`PreparedBatchCollector`) instead of
    fanned out to per-task / per-batch GCS files — eliminating both the
    end-of-run ledger consolidation and the ``prepare`` → ``assemble``
    batch round-trip, each of which otherwise re-reads tens of thousands
    of tiny objects one at a time.

    Replaces the workflow-orchestrated plan / dispatch / commit /
    consolidate split. Transform work is GCS-bound, so a thread pool
    inside one Cloud Run Job execution is the right primitive — and
    one execution per backfill run avoids the Workflows ceilings
    (2 MiB HTTP, 100 K steps, 256 MiB memory) that a multi-step
    dispatch had to tip-toe around.
    """
    scope = parse_backfill_scope(os.environ.get('BACKFILL_SCOPE', ''))
    max_extract_runs = int(
        os.environ.get('MAX_EXTRACT_RUNS') or _DEFAULT_MAX_EXTRACT_RUNS
    )
    max_workers = int(os.environ.get('MAX_WORKERS') or _DEFAULT_MAX_WORKERS)
    workflow_name = workflow_name_for(scope)
    run_label = os.environ.get('RUN_LABEL', '')

    transform_inputs, next_marker = plan_units(
        scope, ledger_fs, cursors_fs, max_extract_runs
    )
    if not transform_inputs:
        logger.info(
            'run_transform_backfill[%s]: no unconsumed extract ledgers; '
            'marker stays at %r',
            scope.value,
            next_marker,
        )
        return
    logger.info(
        'run_transform_backfill[%s]: %d inputs planned; will advance marker to %r',
        scope.value,
        len(transform_inputs),
        next_marker,
    )

    ledger_collector = LedgerCollector(workflow_name, run_date, run_label)
    log_collector = LogCollector(workflow_name, run_date, run_label)
    prepared_batches = PreparedBatchCollector()
    orchestrator = WorkflowOrchestrator(
        workflow_name,
        run_date,
        ledger_fs=ledger_fs,
        run_label=run_label,
        ledger_collector=ledger_collector,
    )
    phases = build_transform_phases(transform_inputs, workflow_name, run_date)
    remaining = [orchestrator.filter_remaining_tasks(phase) for phase in phases]
    shared = _build_runtime(
        config, workflow_name, run_date, run_label, log_collector, prepared_batches
    )

    for index, phase in enumerate(remaining):
        logger.info(
            'run_transform_backfill[%s]: phase %d running (%d units, workers=%d)',
            scope.value,
            index,
            len(phase),
            max_workers,
        )
        _run_phase_parallel(phase, shared, config, orchestrator, max_workers)

    # Single process: flush the in-memory ledger and write-audit log as
    # one consolidated file each. No per-task fan-out, so no O(N)
    # consolidation step that fails to fit its timeout at scale.
    ledger_collector.flush(ledger_fs)
    if config.log_storage:
        log_collector.flush(get_filesystem(config.log_storage))

    save_marker(cursors_fs, workflow_name, ConsumedMarker(consumed_through=next_marker))
    logger.info(
        'run_transform_backfill[%s]: marker advanced to %r', scope.value, next_marker
    )


def _run_phase_parallel(
    phase: list[list[dict[str, str]]],
    shared: _Runtime,
    config: DataTransformationConfig,
    orchestrator: WorkflowOrchestrator,
    max_workers: int,
) -> None:
    """Run *phase*'s tasks concurrently via a thread pool.

    Per-unit ``Exception`` is logged-and-swallowed — the per-task ledger
    entry already records 'failed'; the rest of the phase continues so a
    transient hole doesn't block the whole run. A
    :class:`WorkflowTerminatingError` propagates: pending tasks are
    cancelled, in-flight tasks finish, and this re-raises out of the
    handler so the marker stays put for re-planning next run.
    """
    if not phase:
        return

    def run_unit(env_list: list[dict[str, str]]) -> None:
        task_env = {e['name']: e['value'] for e in env_list}
        _run_one_transform_unit(task_env, shared, config, orchestrator)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(run_unit, env) for env in phase]
        for future in as_completed(futures):
            try:
                future.result()
            except WorkflowTerminatingError:
                for pending in futures:
                    pending.cancel()
                raise
            except Exception:
                logger.exception('Unit failed; continuing with next unit in phase')


def _run_transform_step(
    transformation: 'Transformation',
    raw_fs: FileSystem,
    cleaned_fs: FileSystem,
    batches_fs: FileSystem,
    prepared_fs: FileSystem,
    config: DataTransformationConfig,
    pv_system_id: int | None,
    location_str: str | None,
    grid_point_sample_index: int | None,
    date_range: DateRange,
    prepared_batches: PreparedBatchCollector | None = None,
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
        # prepare_weather is grid-weather only — it is dispatched solely by
        # the weather-grid backfill's in-container handler, which always
        # supplies both the collector and the sample-file index.
        if prepared_batches is None or grid_point_sample_index is None:
            raise WorkflowTerminatingError(
                'prepare_weather requires GRID_POINT_SAMPLE_INDEX and the '
                'in-container collector (weather-grid backfill only)'
            )
        site = resolve_site(
            config.data_sources.weather.type,
            get_pv_site_by_system_id,
            pv_system_id=pv_system_id,
            location_str=location_str,
        )
        run_prepare_weather(
            cleaned_fs,
            config.data_sources.weather,
            site,
            date_range,
            grid_point_sample_index,
            prepared_batches,
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
            collector=prepared_batches,
        )

    elif transformation is Transformation.ASSEMBLE_WEATHER:
        # assemble_weather reads its (sample, window) slice from the
        # in-container collector — weather-grid backfill only (see
        # prepare_weather above). Per-sample so distinct sample windows
        # don't share a task hash.
        if prepared_batches is None or grid_point_sample_index is None:
            raise WorkflowTerminatingError(
                'assemble_weather requires GRID_POINT_SAMPLE_INDEX and the '
                'in-container collector (weather-grid backfill only)'
            )
        assemble_prepared_weather(
            prepared_fs,
            prepared_batches,
            grid_point_sample_index,
            date_range.start.isoformat(),
            date_range.end.isoformat(),
            config.weather_grid.version,
        )

    elif transformation is Transformation.ASSEMBLE_PV:
        # Per-(system, window) so distinct slices don't share a task hash
        # — symmetric to assemble_weather above. The daily-transform path
        # (collector unset) reads its dates from the batch files and
        # ignores start/end; only the collector path keys on them.
        assemble_prepared_pv(
            batches_fs,
            prepared_fs,
            pv_system_id,  # type: ignore[arg-type]
            date_range.start.isoformat(),
            date_range.end.isoformat(),
            prepared_batches,
        )

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
    # grid points: a PV + a weather input per system, a weather input per
    # location. All inputs share the single [START_DATE, END_DATE) window.
    transform_inputs: list[TransformInput] = []
    for pv_id in pv_system_ids:
        transform_inputs.append(
            TransformInput(
                DataSourceType.PV, start_date_str, end_date_str, pv_system_id=pv_id
            )
        )
        transform_inputs.append(
            TransformInput(
                DataSourceType.WEATHER,
                start_date_str,
                end_date_str,
                pv_system_id=pv_id,
            )
        )
    for loc in locations:
        transform_inputs.append(
            TransformInput(
                DataSourceType.WEATHER, start_date_str, end_date_str, location=loc
            )
        )

    run_label = os.environ.get('RUN_LABEL', '')
    orchestrator = WorkflowOrchestrator(
        workflow_name,
        run_date,
        manifests_fs=manifests_fs,
        ledger_fs=ledger_fs,
        run_label=run_label,
    )

    phases = build_transform_phases(transform_inputs, workflow_name, run_date)
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


# ---------------------------------------------------------------------------
# Pull-based transform backfill (BACKFILL_MODE=pull)
# ---------------------------------------------------------------------------


def _pv_slice_task_env(pv_slice: PVSlice, workflow_name: str) -> list[dict[str, str]]:
    """Build the env-list used to hash one PV slice's task identity.

    Note: this is not dispatched to a Cloud Run task — the env-list is
    synthesised purely so :func:`compute_task_hash` produces a stable
    per-slice identity that the cross-run ledger filter can key on.
    """
    return build_env_list(
        SLICE_KIND='pv',
        PV_SYSTEM_ID=str(pv_slice.pv_system_id),
        START_DATE=pv_slice.start_date.isoformat(),
        END_DATE=pv_slice.end_date.isoformat(),
        WORKFLOW_NAME=workflow_name,
    )


def _weather_slice_task_env(
    weather_slice: WeatherSlice, workflow_name: str
) -> list[dict[str, str]]:
    """Build the env-list used to hash one weather slice's task identity."""
    return build_env_list(
        SLICE_KIND='weather',
        GRID_POINT_SAMPLE_INDEX=str(weather_slice.grid_point_sample_index),
        START_DATE=weather_slice.start_date.isoformat(),
        END_DATE=weather_slice.end_date.isoformat(),
        WORKFLOW_NAME=workflow_name,
    )


def _pv_slice_descriptor(pv_slice: PVSlice) -> dict[str, str]:
    return {
        'slice_kind': 'pv',
        'pv_system_id': str(pv_slice.pv_system_id),
        'start_date': pv_slice.start_date.isoformat(),
        'end_date': pv_slice.end_date.isoformat(),
    }


def _weather_slice_descriptor(weather_slice: WeatherSlice) -> dict[str, str]:
    return {
        'slice_kind': 'weather',
        'grid_point_sample_index': str(weather_slice.grid_point_sample_index),
        'start_date': weather_slice.start_date.isoformat(),
        'end_date': weather_slice.end_date.isoformat(),
    }


def _run_one_slice(
    slice_: PVSlice | WeatherSlice,
    raw_fs: FileSystem,
    prepared_fs: FileSystem,
    resources_fs: FileSystem,
    config: DataTransformationConfig,
    workflow_name: str,
    orchestrator: WorkflowOrchestrator,
) -> None:
    """Produce one slice's prepared partition file and record the outcome.

    Routes to :func:`produce_pv_slice` or :func:`produce_weather_slice`
    by slice type. On success records a ledger entry with the
    producer's reported status (``completed`` or ``partial``). On any
    exception records ``failed`` and re-raises.
    """
    if isinstance(slice_, PVSlice):
        task_env = _pv_slice_task_env(slice_, workflow_name)
        descriptor = _pv_slice_descriptor(slice_)
        producer = lambda: produce_pv_slice(  # noqa: E731
            slice_,
            raw_fs,
            prepared_fs,
            config.data_sources.pv,
            config.data_sources.weather,
            get_pv_site_by_system_id,
        )
    else:
        task_env = _weather_slice_task_env(slice_, workflow_name)
        descriptor = _weather_slice_descriptor(slice_)
        locations = read_sample_file(
            resources_fs, sample_file_path(slice_.grid_point_sample_index)
        )
        producer = lambda: produce_weather_slice(  # noqa: E731
            slice_,
            raw_fs,
            prepared_fs,
            config.data_sources.weather,
            locations,
            config.weather_grid.version,
        )

    task_hash = compute_task_hash(task_env)
    try:
        outcome: SliceOutcome = producer()
    except Exception as e:
        orchestrator.record_outcome(task_hash, descriptor, 'failed', error=repr(e))
        raise

    if outcome.missing_inputs_count:
        descriptor = {
            **descriptor,
            'missing_inputs_count': str(outcome.missing_inputs_count),
        }
    orchestrator.record_outcome(task_hash, descriptor, outcome.status)


def _run_transform_backfill_pull(
    run_date: str,
    config: DataTransformationConfig,
    ledger_fs: FileSystem,
    cursors_fs: FileSystem,
) -> None:
    """Pull-based transform backfill: one prepared partition file per slice.

    Plans slices from the extraction backfill's consolidated ledger
    via :func:`plan_slices`, filters out slices already recorded as
    ``completed`` in the cross-run ledger pool, and runs each
    remaining slice through :func:`produce_pv_slice` /
    :func:`produce_weather_slice` in a thread pool.

    No ``cleaned/`` files are written: each slice's
    clean → prepare → assemble chain runs entirely in memory. The
    ledger records one entry per slice with status ``completed``,
    ``partial`` (some raw inputs missing — output still written), or
    ``failed`` (exception during processing).

    Marker semantics are unchanged from the push handler: the marker
    advances unconditionally once every slice has been *attempted*
    (success, partial, or swallowed-Exception failure).
    """
    scope = parse_backfill_scope(os.environ.get('BACKFILL_SCOPE', ''))
    max_extract_runs = int(
        os.environ.get('MAX_EXTRACT_RUNS') or _DEFAULT_MAX_EXTRACT_RUNS
    )
    max_workers = int(os.environ.get('MAX_WORKERS') or _DEFAULT_MAX_WORKERS)
    workflow_name = workflow_name_for(scope)
    run_label = os.environ.get('RUN_LABEL', '')

    slices, next_marker = plan_slices(scope, ledger_fs, cursors_fs, max_extract_runs)
    if not slices:
        logger.info(
            'run_transform_backfill[pull/%s]: no unconsumed extract ledgers; '
            'marker stays at %r',
            scope.value,
            next_marker,
        )
        return
    logger.info(
        'run_transform_backfill[pull/%s]: %d slices planned; will advance marker to %r',
        scope.value,
        len(slices),
        next_marker,
    )

    ledger_collector = LedgerCollector(workflow_name, run_date, run_label)
    log_collector = LogCollector(workflow_name, run_date, run_label)
    orchestrator = WorkflowOrchestrator(
        workflow_name,
        run_date,
        ledger_fs=ledger_fs,
        run_label=run_label,
        ledger_collector=ledger_collector,
    )

    completed = orchestrator.completed_task_hashes()
    remaining: list[PVSlice | WeatherSlice] = []
    for slice_ in slices:
        if isinstance(slice_, PVSlice):
            task_env = _pv_slice_task_env(slice_, workflow_name)
        else:
            task_env = _weather_slice_task_env(slice_, workflow_name)
        if compute_task_hash(task_env) not in completed:
            remaining.append(slice_)

    logger.info(
        'run_transform_backfill[pull/%s]: %d slices remaining after filter '
        '(workers=%d)',
        scope.value,
        len(remaining),
        max_workers,
    )

    resources_fs = get_filesystem(config.resources_storage)
    raw_fs = get_filesystem(config.staged_raw_data_storage)
    prepared_fs = get_logging_filesystem(
        config.staged_prepared_data_storage,
        config.log_storage,
        workflow_name,
        run_date,
        'prepared',
        run_label,
        log_collector,
    )
    _load_resources(resources_fs)

    def run_slice(slice_: PVSlice | WeatherSlice) -> None:
        _run_one_slice(
            slice_,
            raw_fs,
            prepared_fs,
            resources_fs,
            config,
            workflow_name,
            orchestrator,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(run_slice, s) for s in remaining]
        for future in as_completed(futures):
            try:
                future.result()
            except WorkflowTerminatingError:
                for pending in futures:
                    pending.cancel()
                raise
            except Exception:
                logger.exception('Slice failed; continuing with the rest of the run')

    # Single process: flush the in-memory ledger and write-audit log as
    # one consolidated file each. No per-task fan-out, so no O(N)
    # consolidation step.
    ledger_collector.flush(ledger_fs)
    if config.log_storage:
        log_collector.flush(get_filesystem(config.log_storage))

    save_marker(cursors_fs, workflow_name, ConsumedMarker(consumed_through=next_marker))
    logger.info(
        'run_transform_backfill[pull/%s]: marker advanced to %r',
        scope.value,
        next_marker,
    )


if __name__ == '__main__':
    configure_logging()
    run_entrypoint(main)
