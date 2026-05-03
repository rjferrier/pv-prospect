"""Cloud Run Job entrypoint.

Reads task parameters from environment variables set by the Cloud Workflow
and calls the corresponding core function.

Environment variables
---------------------
JOB_TYPE
    ``preprocess``, ``extract_and_load``,
    ``plan_weather_grid_backfill``, ``commit_weather_grid_backfill``,
    ``plan_pv_site_backfill``, or ``commit_pv_site_backfill``

For **preprocess**:
    DATA_SOURCE — ``pv`` or ``weather`` (optional; defaults to weather)

For **extract_and_load**:
    DATA_SOURCE       — ``pv`` or ``weather`` (optional; defaults to weather)
    PV_SYSTEM_ID      — integer system id (required for PV sources; for weather
                        sources, exactly one of PV_SYSTEM_ID, LOCATION, or
                        SAMPLE_FILE_INDEX must be set)
    LOCATION          — comma-separated lat,lon (e.g. ``50.49,-3.54``);
                        alternative to PV_SYSTEM_ID for weather sources
    SAMPLE_FILE_INDEX — integer index of a ``sample_NNN.csv`` grid-point
                        sample file on the resources filesystem. Processes
                        every grid point in the file sequentially. Weather
                        sources only.
    START_DATE        — ISO date ``YYYY-MM-DD``. Alias: ``DATE`` (clearer when
                        no end date is given).
    END_DATE          — ISO date ``YYYY-MM-DD``, exclusive (optional; defaults
                        to START_DATE + 1 day)
    DRY_RUN           — ``true`` or ``false`` (default ``false``)
    SPLIT_BY          — ``day`` or ``week`` (chunking hint; omit to use the
                        data source default: day for PV, unsplit for weather)
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
from pv_prospect.common.domain import (
    AnySite,
    Period,
)
from pv_prospect.data_extraction import (
    DataSource,
    default_split_period,
    get_extractor,
    supports_multi_date,
)
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.processing import core
from pv_prospect.data_extraction.processing.manifest import (
    commit_weather_grid_backfill,
    plan_weather_grid_backfill,
)
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
from pv_prospect.data_extraction.resources import get_config_dir as get_de_config_dir
from pv_prospect.data_sources import (
    DataSourceType,
    resolve_location_strings,
    resolve_site,
)
from pv_prospect.data_sources import (
    get_config_dir as get_ds_config_dir,
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
    pv_system_id = _env_int('PV_SYSTEM_ID')
    location = os.environ.get('LOCATION')
    sample_file_index = _env_int('SAMPLE_FILE_INDEX')

    def _resolve_sample_file(idx: int) -> list[str]:
        path = sample_file_path(idx)
        return read_sample_file(resources_fs, path)

    sites: list[AnySite] = []

    location_strings = resolve_location_strings(
        resolve_sample_file=_resolve_sample_file,
        location_strings=location,
        sample_file_index=sample_file_index,
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
            'Could not resolve any targets (need PV_SYSTEM_ID, LOCATION, or SAMPLE_FILE_INDEX).'
        )

    return sites


def _run_extract_and_load(
    staging_fs: FileSystem,
    resources_fs: FileSystem,
    data_source: DataSource,
) -> None:
    dry_run = _env_bool('DRY_RUN')
    split_by = os.environ.get('SPLIT_BY')

    try:
        start_date_str = os.environ.get('START_DATE') or os.environ.get('DATE')
        if not start_date_str:
            raise ValueError('START_DATE (or DATE) must be set.')
        complete_date_range = build_date_range(
            start_date_str, os.environ.get('END_DATE')
        )
    except DegenerateDateRange as e:
        logger.error('%s', e)
        sys.exit(1)

    resources_extractor = Extractor(resources_fs)
    build_pv_site_repo(resources_extractor.read_file(core.PV_SITES_CSV_FILE))

    sites = _resolve_sites(data_source, resources_fs)

    logger.info(
        'extract_and_load: %s, %d sites, %s, split_by=%s',
        data_source,
        len(sites),
        complete_date_range,
        split_by,
    )

    split_period = (
        Period[split_by.upper()] if split_by else default_split_period(data_source)
    )
    if split_period:
        sub_date_ranges = complete_date_range.split_by(split_period)
        if split_period == Period.WEEK and not supports_multi_date(data_source):
            final_ranges = []
            for dr in sub_date_ranges:
                final_ranges.extend(dr.split_by(Period.DAY))
        else:
            final_ranges = sub_date_ranges
    else:
        final_ranges = [complete_date_range]

    if not final_ranges:
        logger.warning(
            'extract_and_load: %s — no dates to process in %s',
            data_source,
            complete_date_range,
        )
        return

    failures: list[str] = []
    for site in sites:
        for dr in final_ranges:
            result = core.extract_and_load(
                get_extractor,
                data_source,
                staging_fs,
                site,
                dr,
                dry_run,
            )
            logger.info('%s %s: %s', site, dr, result.type.value)
            if result.type == ResultType.FAILURE:
                failures.append(f'{site} {dr}')

    if failures:
        logger.error(
            'extract_and_load: %d task(s) failed: %s',
            len(failures),
            ', '.join(failures),
        )
        sys.exit(1)


def _run_plan_weather_grid_backfill(resources_fs: FileSystem) -> None:
    today = date.today()
    num_sample_files = count_sample_files(resources_fs)
    if num_sample_files == 0:
        raise ValueError('No sample files found on the resources filesystem.')
    manifest = plan_weather_grid_backfill(today, num_sample_files, resources_fs)
    logger.info(
        'plan_weather_grid_backfill: wrote manifest (step2=%s, step3_batches=%d)',
        manifest.step2_batch,
        len(manifest.step3_batches),
    )


def _run_commit_weather_grid_backfill(resources_fs: FileSystem) -> None:
    cursor = commit_weather_grid_backfill(resources_fs)
    logger.info('commit_weather_grid_backfill: advanced cursor to %s', cursor)


def _run_plan_pv_site_backfill(resources_fs: FileSystem) -> None:
    today = date.today()
    plan = plan_pv_site_backfill(today, resources_fs)
    logger.info(
        'plan_pv_site_backfill: wrote manifest (%s to %s)',
        plan.start_date,
        plan.end_date,
    )


def _run_commit_pv_site_backfill(resources_fs: FileSystem) -> None:
    cursor = commit_pv_site_backfill(resources_fs)
    logger.info('commit_pv_site_backfill: advanced cursor to %s', cursor)


def _get_logging_filesystem(
    storage_config: AnyStorageConfig,
    log_storage: AnyStorageConfig | None,
    workflow_name: str,
    label: str,
) -> FileSystem:
    fs: FileSystem = get_filesystem(storage_config)
    if workflow_name and log_storage:
        log_fs = get_filesystem(log_storage)
        return LoggingFileSystem(fs, log_fs, workflow_name, label)
    return fs


def _run_consolidate_logs(config: DataExtractionConfig) -> None:
    workflow_name = os.environ.get('WORKFLOW_NAME', '')
    if not workflow_name or not config.log_storage:
        logger.warning('consolidate_logs: WORKFLOW_NAME or log_storage not configured')
        return
    log_fs = get_filesystem(config.log_storage)
    today = date.today()
    consolidate_logs(log_fs, workflow_name, today)


def main() -> None:
    job_type = os.environ.get('JOB_TYPE', '')
    workflow_name = os.environ.get('WORKFLOW_NAME', '')
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

    # Cloud Run always uses GCS — resolve storage backends once.
    staging_fs = _get_logging_filesystem(
        config.staged_raw_data_storage, config.log_storage, workflow_name, 'raw'
    )
    resources_fs = get_filesystem(config.resources_storage)

    if job_type == 'preprocess':
        _run_preprocess(staging_fs, data_source)
    elif job_type == 'extract_and_load':
        _run_extract_and_load(staging_fs, resources_fs, data_source)
    elif job_type == 'plan_weather_grid_backfill':
        _run_plan_weather_grid_backfill(resources_fs)
    elif job_type == 'commit_weather_grid_backfill':
        _run_commit_weather_grid_backfill(resources_fs)
    elif job_type == 'plan_pv_site_backfill':
        _run_plan_pv_site_backfill(resources_fs)
    elif job_type == 'commit_pv_site_backfill':
        _run_commit_pv_site_backfill(resources_fs)
    elif job_type == 'consolidate_logs':
        _run_consolidate_logs(config)
    else:
        logger.error('unknown JOB_TYPE=%r', job_type)
        sys.exit(1)


if __name__ == '__main__':
    configure_logging()
    try:
        main()
    except Exception:
        logger.exception('Unhandled exception')
        sys.exit(1)
