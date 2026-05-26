"""Cloud Run Job entrypoint.

Reads task parameters from environment variables set by the Cloud Workflow
and calls the corresponding core function.

Environment variables
---------------------
JOB_TYPE
    ``preprocess``, ``extract_and_load``, ``plan_extract``,
    ``plan_weather_grid_backfill``, ``commit_weather_grid_backfill``,
    ``plan_pv_site_backfill``, or ``commit_pv_site_backfill``

For **preprocess**:
    DATA_SOURCE — ``pv`` or ``weather`` (optional; defaults to weather)

For **extract_and_load**:
    DATA_SOURCE             — ``pv`` or ``weather`` (optional; defaults to
                              weather)
    PV_SYSTEM_ID            — integer system id (required for PV sources; for
                              weather sources, exactly one of PV_SYSTEM_ID,
                              LOCATION, or GRID_POINT_SAMPLE_INDEX must be set)
    LOCATION                — comma-separated lat,lon (e.g. ``50.49,-3.54``);
                              alternative to PV_SYSTEM_ID for weather sources
    GRID_POINT_SAMPLE_INDEX — integer index of a ``sample_NNN.csv`` grid-point
                              sample file on the resources filesystem. When it
                              is the only target given, processes every grid
                              point in the file sequentially (weather sources
                              only). The weather-grid backfill also sets it
                              alongside ``LOCATIONS`` so the index is recorded
                              in each per-site ledger descriptor.
    START_DATE              — ISO date ``YYYY-MM-DD``. Alias: ``DATE``
                              (clearer when no end date is given).
    END_DATE                — ISO date ``YYYY-MM-DD``, exclusive (optional;
                              defaults to START_DATE + 1 day)
    DRY_RUN                 — ``true`` or ``false`` (default ``false``)
    SPLIT_BY                — ``day`` or ``week`` (chunking hint; omit to use
                              the data source default: day for PV, unsplit
                              for weather)
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
from pv_prospect.common.domain import (
    AnySite,
    DateRange,
    Period,
    PVSite,
)
from pv_prospect.data_extraction import (
    DataSource,
    default_split_period,
    get_extractor,
    supports_multi_date,
)
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.processing import core
from pv_prospect.data_extraction.processing.pv_backfill import (
    commit_pv_site_backfill,
    plan_pv_site_backfill,
)
from pv_prospect.data_extraction.processing.sample_file import (
    count_sample_files,
    read_sample_file,
    sample_file_path,
)
from pv_prospect.data_extraction.processing.value_objects import ResultType
from pv_prospect.data_extraction.processing.weather_grid_backfill import (
    commit_weather_grid_backfill,
    plan_weather_grid_backfill,
)
from pv_prospect.data_extraction.resources import get_config_dir as get_de_config_dir
from pv_prospect.data_sources import (
    DataSourceType,
    resolve_location_strings,
    resolve_site,
)
from pv_prospect.data_sources import (
    get_config_dir as get_ds_config_dir,
)
from pv_prospect.etl import (
    DegenerateDateRange,
    Extractor,
    WorkflowOrchestrator,
    WorkflowTerminatingError,
    build_date_range,
    build_env_list,
    compute_task_hash,
    get_logging_filesystem,
    inject_task_hash,
    resolve_run_date,
    run_consolidate_logs,
    run_entrypoint,
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


def _run_preprocess(
    staging_fs: FileSystem,
    data_source: DataSource,
) -> None:
    logger.info('preprocess: %s', data_source)
    core.preprocess(staging_fs, data_source)


def _resolve_sites(
    data_source: DataSource,
    resources_fs: FileSystem,
) -> list[AnySite]:
    """Resolve the sites a single ``extract_and_load`` task should process.

    Accepts (in order of precedence):
      - ``LOCATIONS``: JSON array of ``lat,lon`` strings.
      - ``LOCATION``: a single ``lat,lon`` string.
      - ``GRID_POINT_SAMPLE_INDEX``: integer; resolves to every grid point
        in the sample file at that index. Retained for one-off local
        invocations; deployed pipelines use ``LOCATIONS``.
      - ``PV_SYSTEM_ID``: integer; resolves to a single PV site.
    """
    pv_system_id = _env_int('PV_SYSTEM_ID')
    location = os.environ.get('LOCATION')
    locations_json = os.environ.get('LOCATIONS')
    grid_point_sample_index = _env_int('GRID_POINT_SAMPLE_INDEX')

    def _resolve_sample_file(idx: int) -> list[str]:
        path = sample_file_path(idx)
        return read_sample_file(resources_fs, path)

    sites: list[AnySite] = []

    explicit_locations: list[str] | str | None
    if locations_json:
        explicit_locations = json.loads(locations_json)
    else:
        explicit_locations = location

    location_strings = resolve_location_strings(
        resolve_sample_file=_resolve_sample_file,
        location_strings=explicit_locations,
        grid_point_sample_index=grid_point_sample_index,
    )

    if location_strings:
        for loc in location_strings:
            sites.append(
                resolve_site(
                    data_source.type, get_pv_site_by_system_id, location_str=loc
                )
            )
    elif pv_system_id is not None:
        sites.append(
            resolve_site(
                data_source.type, get_pv_site_by_system_id, pv_system_id=pv_system_id
            )
        )
    else:
        raise ValueError(
            'Could not resolve any targets '
            '(need PV_SYSTEM_ID, LOCATION, LOCATIONS, or GRID_POINT_SAMPLE_INDEX).'
        )

    return sites


def _site_locator(site: AnySite) -> tuple[str, str]:
    """Return the ``(env_name, env_value)`` pair that identifies *site*."""
    if isinstance(site, PVSite):
        return ('PV_SYSTEM_ID', str(site.pvo_sys_id))
    return ('LOCATION', site.location.to_coordinate_string())


def _final_ranges(
    complete_date_range: DateRange,
    data_source: DataSource,
    split_by: str | None,
) -> list[DateRange]:
    """Split *complete_date_range* per the data source's API semantics."""
    split_period = (
        Period[split_by.upper()] if split_by else default_split_period(data_source)
    )
    if split_period is None:
        return [complete_date_range]
    sub_date_ranges = complete_date_range.split_by(split_period)
    if split_period == Period.WEEK and not supports_multi_date(data_source):
        return [d for dr in sub_date_ranges for d in dr.split_by(Period.DAY)]
    return sub_date_ranges


def _extract_one_site(
    staging_fs: FileSystem,
    data_source: DataSource,
    site: AnySite,
    final_ranges: list[DateRange],
    dry_run: bool,
) -> list[str]:
    """Extract *site* over each sub-range. Returns per-range failure strings."""
    failures: list[str] = []
    for dr in final_ranges:
        result = core.extract_and_load(
            get_extractor,
            data_source,
            staging_fs,
            site,
            dr,
            dry_run,
        )
        logger.debug('%s %s: %s', site, dr, result.type.value)
        if result.type == ResultType.FAILURE:
            failures.append(f'{site} {dr}')
    return failures


def _run_extract_and_load(
    config: DataExtractionConfig,
    data_source: DataSource,
    ledger_fs: FileSystem | None,
    run_date: str,
) -> None:
    """Run ``extract_and_load`` for every site this task is responsible for.

    Each resolved site is a sub-task with its own ledger entry, keyed by
    a hash computed from its per-site env. Sites whose hash is already
    in the ledger as ``completed`` are skipped (handles Cloud Run task-
    retry idempotency and same-day workflow re-triggers). Per-site API
    failures are recorded as ``failed`` ledger entries and the process
    still exits 0 — the workflow advances its checkpoint regardless of
    per-site outcomes. ``WorkflowTerminatingError`` still propagates to
    abort the workflow before the cursor commit.

    Per-site outcomes and write-audit log entries are buffered in memory
    via :class:`LedgerCollector` / :class:`LogCollector` and flushed as
    one consolidated per-task file each at end of run (or in ``finally``
    on any exception that wasn't site-local). This sidesteps the
    per-site GCS file fan-out that previously caused the
    end-of-workflow ``consolidate_logs`` step to time out at backfill
    scale. The handful of per-task files left behind is what the
    workflow's terminal consolidation step then merges into the daily
    consolidated file.
    """
    workflow_name = os.environ.get('WORKFLOW_NAME', 'pv-prospect-extract')
    run_label = os.environ.get('RUN_LABEL', '')
    task_hash = os.environ.get('TASK_HASH', '')
    if not task_hash:
        raise WorkflowTerminatingError(
            'TASK_HASH env var is required for extract_and_load '
            '(should be injected by the planner)'
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
    staging_fs = get_logging_filesystem(
        config.staged_raw_data_storage,
        config.log_storage,
        workflow_name,
        run_date,
        'raw',
        run_label,
        log_collector,
    )

    try:
        _extract_into_collectors(
            config, data_source, orchestrator, staging_fs, workflow_name, run_date
        )
    finally:
        # Flush whatever was buffered, even on early exit. A
        # ``WorkflowTerminatingError`` skips the cursor commit upstream,
        # so the marker stays put; the partial outcomes we preserve here
        # still let tomorrow's run skip the sites we already completed.
        if ledger_fs is not None:
            ledger_collector.flush_per_task(ledger_fs, task_hash)
        if config.log_storage is not None:
            log_collector.flush_per_task(get_filesystem(config.log_storage), task_hash)


def _extract_into_collectors(
    config: DataExtractionConfig,
    data_source: DataSource,
    orchestrator: WorkflowOrchestrator,
    staging_fs: FileSystem,
    workflow_name: str,
    run_date: str,
) -> None:
    """Per-site fetch loop for one ``extract_and_load`` Cloud Run task.

    Records each site's outcome through *orchestrator* (which routes to
    the in-memory ledger collector). Exists as a separate function so
    the caller's ``finally`` can flush the collectors without the work
    body's setup and per-site loop sitting inside a long ``try`` block.
    """
    dry_run = _env_bool('DRY_RUN')
    split_by = os.environ.get('SPLIT_BY')
    end_date_str = os.environ.get('END_DATE') or ''
    grid_point_sample_index = _env_int('GRID_POINT_SAMPLE_INDEX')

    try:
        start_date_str = os.environ.get('START_DATE') or os.environ.get('DATE')
        if not start_date_str:
            raise ValueError('START_DATE (or DATE) must be set.')
        complete_date_range = build_date_range(start_date_str, end_date_str or None)
    except DegenerateDateRange as e:
        raise WorkflowTerminatingError(str(e)) from e

    resources_fs = get_filesystem(config.resources_storage)
    resources_extractor = Extractor(resources_fs)
    build_pv_site_repo(resources_extractor.read_file(core.PV_SITES_CSV_FILE))
    sites = _resolve_sites(data_source, resources_fs)

    final_ranges = _final_ranges(complete_date_range, data_source, split_by)
    if not final_ranges:
        logger.warning(
            'extract_and_load: %s — no dates to process in %s',
            data_source,
            complete_date_range,
        )
        return

    logger.info(
        'extract_and_load: %s, %d sites, %s, split_by=%s',
        data_source,
        len(sites),
        complete_date_range,
        split_by,
    )

    completed = orchestrator.completed_task_hashes()

    base_env: dict[str, str] = {
        'JOB_TYPE': 'extract_and_load',
        'DATA_SOURCE': os.environ.get('DATA_SOURCE', ''),
        'START_DATE': start_date_str,
        'DRY_RUN': os.environ.get('DRY_RUN', 'false'),
        'WORKFLOW_NAME': workflow_name,
        'RUN_DATE': run_date,
    }
    if end_date_str:
        base_env['END_DATE'] = end_date_str
    if split_by:
        base_env['SPLIT_BY'] = split_by

    for site in sites:
        locator_name, locator_value = _site_locator(site)
        site_env = build_env_list(**{**base_env, locator_name: locator_value})
        site_hash = compute_task_hash(site_env)

        descriptor: dict[str, str] = {
            'data_source': base_env['DATA_SOURCE'],
            'start_date': start_date_str,
            locator_name.lower(): locator_value,
        }
        if end_date_str:
            descriptor['end_date'] = end_date_str
        # GRID_POINT_SAMPLE_INDEX is set only by the weather-grid
        # backfill. It rides in the descriptor (not the task hash) so
        # the transform can group prepared rows into per-(sample,
        # window) files. Other workflows leave it unset and their
        # descriptors omit the key.
        if grid_point_sample_index is not None:
            descriptor['grid_point_sample_index'] = str(grid_point_sample_index)

        if site_hash in completed:
            logger.debug('%s: skipped (ledger has completed entry)', site)
            continue

        try:
            site_failures = _extract_one_site(
                staging_fs, data_source, site, final_ranges, dry_run
            )
        except WorkflowTerminatingError:
            raise
        except Exception as e:
            orchestrator.record_outcome(site_hash, descriptor, 'failed', error=repr(e))
            logger.exception('%s: failed with exception', site)
            continue

        if site_failures:
            error_summary = '; '.join(site_failures)
            orchestrator.record_outcome(
                site_hash, descriptor, 'failed', error=error_summary
            )
            logger.error('%s: failed (%s)', site, error_summary)
        else:
            orchestrator.record_outcome(site_hash, descriptor, 'completed')


def _run_plan_extract(
    run_date: str,
    manifests_fs: FileSystem,
    ledger_fs: FileSystem | None = None,
) -> None:
    workflow_name = os.environ.get('WORKFLOW_NAME', 'pv-prospect-extract')
    start_date_str = (
        os.environ.get('START_DATE')
        or os.environ.get('DATE')
        or date.today().isoformat()
    )

    pv_system_ids = json.loads(os.environ.get('PV_SYSTEM_IDS', '[]'))
    locations = json.loads(os.environ.get('LOCATIONS', '[]'))
    pv_sources = json.loads(os.environ.get('PV_MODEL_DATA_SOURCES', '[]'))
    weather_sources = json.loads(os.environ.get('WEATHER_MODEL_DATA_SOURCES', '[]'))
    dry_run = os.environ.get('DRY_RUN', 'false')
    split_by = os.environ.get('SPLIT_BY', '')
    run_label = os.environ.get('RUN_LABEL', '')

    orchestrator = WorkflowOrchestrator(
        workflow_name,
        run_date,
        manifests_fs=manifests_fs,
        ledger_fs=ledger_fs,
        run_label=run_label,
    )
    all_tasks = []

    for pv_id in pv_system_ids:
        for ds in pv_sources:
            all_tasks.append(
                inject_task_hash(
                    build_env_list(
                        JOB_TYPE='extract_and_load',
                        DATA_SOURCE=ds,
                        PV_SYSTEM_ID=str(pv_id),
                        DATE=start_date_str,
                        START_DATE=start_date_str,
                        DRY_RUN=dry_run,
                        SPLIT_BY=split_by,
                        WORKFLOW_NAME=workflow_name,
                        RUN_DATE=run_date,
                    )
                )
            )

    for loc in locations:
        for ds in weather_sources:
            all_tasks.append(
                inject_task_hash(
                    build_env_list(
                        JOB_TYPE='extract_and_load',
                        DATA_SOURCE=ds,
                        LOCATION=loc,
                        DATE=start_date_str,
                        START_DATE=start_date_str,
                        DRY_RUN=dry_run,
                        SPLIT_BY=split_by,
                        WORKFLOW_NAME=workflow_name,
                        RUN_DATE=run_date,
                    )
                )
            )

    remaining = orchestrator.filter_remaining_tasks(all_tasks)
    orchestrator.write_manifest([remaining])
    logger.info(
        'plan_extract: wrote manifest with %d / %d tasks remaining',
        len(remaining),
        len(all_tasks),
    )


def _run_plan_weather_grid_backfill(
    run_date: str,
    resources_fs: FileSystem,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
) -> None:
    today = date.today()
    num_sample_files = count_sample_files(resources_fs)
    if num_sample_files == 0:
        raise ValueError('No sample files found on the resources filesystem.')
    data_source = os.environ.get('DATA_SOURCE', 'weather')
    dry_run = os.environ.get('DRY_RUN', 'false')
    manifest = plan_weather_grid_backfill(
        today,
        run_date,
        num_sample_files,
        data_source,
        dry_run,
        cursors_fs,
        manifests_fs,
        resources_fs,
    )
    logger.info(
        'plan_weather_grid_backfill: wrote manifest (step2=%s, step3_batches=%d)',
        manifest.step2_batch,
        len(manifest.step3_batches),
    )


def _run_commit_weather_grid_backfill(
    run_date: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
) -> None:
    cursor = commit_weather_grid_backfill(run_date, cursors_fs, manifests_fs)
    logger.info('commit_weather_grid_backfill: advanced cursor to %s', cursor)


def _run_plan_pv_site_backfill(
    run_date: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
) -> None:
    today = date.today()
    pv_system_ids = [int(s) for s in json.loads(os.environ.get('PV_SYSTEM_IDS', '[]'))]
    pv_data_source = os.environ.get('PV_DATA_SOURCE', 'pv')
    weather_data_source = os.environ.get('WEATHER_DATA_SOURCE', 'weather')
    dry_run = os.environ.get('DRY_RUN', 'false')
    plan = plan_pv_site_backfill(
        today,
        run_date,
        pv_system_ids,
        pv_data_source,
        weather_data_source,
        dry_run,
        cursors_fs,
        manifests_fs,
    )
    logger.info(
        'plan_pv_site_backfill: wrote manifest (%s to %s, %d PV system(s))',
        plan.start_date,
        plan.end_date,
        len(pv_system_ids),
    )


def _run_commit_pv_site_backfill(
    run_date: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
) -> None:
    cursor = commit_pv_site_backfill(run_date, cursors_fs, manifests_fs)
    logger.info('commit_pv_site_backfill: advanced cursor to %s', cursor)


def main() -> None:
    job_type = os.environ.get('JOB_TYPE', '')
    workflow_name = os.environ.get('WORKFLOW_NAME', '')
    run_label = os.environ.get('RUN_LABEL', '')
    run_date = resolve_run_date()
    config = get_config(
        DataExtractionConfig,
        base_config_dirs=[
            get_etl_config_dir(),
            get_ds_config_dir(),
            get_de_config_dir(),
        ],
    )

    source_env = os.environ.get('DATA_SOURCE')
    data_source_type = (
        DataSourceType(source_env) if source_env else DataSourceType.WEATHER
    )
    data_source = config.data_sources.get_data_source(data_source_type)

    resources_fs = get_filesystem(config.resources_storage)
    manifests_fs = (
        get_filesystem(config.manifests_storage) if config.manifests_storage else None
    )
    cursors_fs = (
        get_filesystem(config.cursors_storage) if config.cursors_storage else None
    )
    ledger_fs = get_filesystem(config.ledger_storage) if config.ledger_storage else None

    if job_type == 'preprocess':
        # Preprocess is a one-off prep step with negligible write volume;
        # no collector needed, just a plain write-logging filesystem.
        staging_fs = get_logging_filesystem(
            config.staged_raw_data_storage,
            config.log_storage,
            workflow_name,
            run_date,
            'raw',
            run_label,
        )
        _run_preprocess(staging_fs, data_source)
    elif job_type == 'plan_extract':
        _run_plan_extract(
            run_date, _required(manifests_fs, 'manifests_storage'), ledger_fs
        )
    elif job_type == 'extract_and_load':
        _run_extract_and_load(config, data_source, ledger_fs, run_date)
    elif job_type == 'plan_weather_grid_backfill':
        _run_plan_weather_grid_backfill(
            run_date,
            resources_fs,
            _required(cursors_fs, 'cursors_storage'),
            _required(manifests_fs, 'manifests_storage'),
        )
    elif job_type == 'commit_weather_grid_backfill':
        _run_commit_weather_grid_backfill(
            run_date,
            _required(cursors_fs, 'cursors_storage'),
            _required(manifests_fs, 'manifests_storage'),
        )
    elif job_type == 'plan_pv_site_backfill':
        _run_plan_pv_site_backfill(
            run_date,
            _required(cursors_fs, 'cursors_storage'),
            _required(manifests_fs, 'manifests_storage'),
        )
    elif job_type == 'commit_pv_site_backfill':
        _run_commit_pv_site_backfill(
            run_date,
            _required(cursors_fs, 'cursors_storage'),
            _required(manifests_fs, 'manifests_storage'),
        )
    elif job_type == 'consolidate_logs':
        run_consolidate_logs(
            config.log_storage,
            config.ledger_storage,
            workflow_name,
            run_date,
            run_label,
        )
    else:
        raise WorkflowTerminatingError(f'unknown JOB_TYPE={job_type!r}')


def _required(fs: FileSystem | None, name: str) -> FileSystem:
    if fs is None:
        raise WorkflowTerminatingError(
            f'{name} is required for this JOB_TYPE but not configured'
        )
    return fs


if __name__ == '__main__':
    configure_logging()
    run_entrypoint(main)
