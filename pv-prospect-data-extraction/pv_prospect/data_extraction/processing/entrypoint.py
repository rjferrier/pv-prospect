"""Cloud Run Job entrypoint.

Reads task parameters from environment variables set by the Cloud Workflow
and calls the corresponding core function.

Environment variables
---------------------
JOB_TYPE
    ``preprocess``, ``extract_and_load``, ``plan_grid_point_backfill``, or
    ``commit_grid_point_backfill``

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
    OVERWRITE         — ``true`` or ``false`` (default ``false``)
    DRY_RUN           — ``true`` or ``false`` (default ``false``)
    SPLIT_BY          — ``day`` or ``week`` (chunking hint; omit to use the
                        full date range as a single chunk)
"""

import logging
import os
import sys
from datetime import date

from pv_prospect.common import (
    build_location_mapping_repo,
    build_pv_site_repo,
    configure_logging,
    get_config,
    get_location_by_pv_system_id,
    get_pv_site_by_system_id,
)
from pv_prospect.common.domain import (
    AnyEntity,
    Period,
)
from pv_prospect.data_extraction import (
    DataSource,
    get_extractor,
    supports_multi_date,
)
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.processing import core
from pv_prospect.data_extraction.processing.manifest import (
    commit_backfill,
    plan_backfill,
)
from pv_prospect.data_extraction.processing.sample_file import (
    count_sample_files,
    read_sample_file,
    sample_file_path,
)
from pv_prospect.data_extraction.resources import get_config_dir as get_de_config_dir
from pv_prospect.data_sources import DataSourceType, resolve_grid_point
from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
from pv_prospect.etl import DegenerateDateRange, Extractor, build_date_range
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import FileSystem, get_filesystem

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


def _resolve_entities(
    data_source: DataSource,
    resources_fs: FileSystem,
) -> list[AnyEntity]:
    pv_system_id = _env_int('PV_SYSTEM_ID')
    location = os.environ.get('LOCATION')
    sample_file_index = _env_int('SAMPLE_FILE_INDEX')

    if sample_file_index is not None:
        if data_source.type != DataSourceType.WEATHER:
            raise ValueError('SAMPLE_FILE_INDEX is only valid for weather sources.')
        path = sample_file_path(sample_file_index)
        grid_points = read_sample_file(resources_fs, path)
        return list(grid_points)

    if data_source.type == DataSourceType.PV:
        if not pv_system_id:
            raise ValueError('PV_SYSTEM_ID must be set for PV sources.')
        return [get_pv_site_by_system_id(pv_system_id)]
    if data_source.type == DataSourceType.WEATHER:
        return [
            resolve_grid_point(
                get_location_by_pv_system_id,
                pv_system_id=pv_system_id,
                location_str=location,
            )
        ]
    raise ValueError(f'Unknown data source type: {data_source.type}')


def _run_extract_and_load(
    staging_fs: FileSystem,
    resources_fs: FileSystem,
    data_source: DataSource,
) -> None:
    overwrite = _env_bool('OVERWRITE')
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
    build_location_mapping_repo(
        resources_extractor.read_file(core.LOCATION_MAPPING_CSV_FILE)
    )

    entities = _resolve_entities(data_source, resources_fs)

    logger.info(
        'extract_and_load: %s, %d entities, %s, split_by=%s',
        data_source,
        len(entities),
        complete_date_range,
        split_by,
    )

    if split_by == 'week':
        sub_date_ranges = complete_date_range.split_by(Period.WEEK)
        if not supports_multi_date(data_source):
            final_ranges = []
            for dr in sub_date_ranges:
                final_ranges.extend(dr.split_by(Period.DAY))
        else:
            final_ranges = sub_date_ranges
    elif split_by == 'day':
        final_ranges = complete_date_range.split_by(Period.DAY)
    else:
        final_ranges = [complete_date_range]

    if not final_ranges:
        logger.warning(
            'extract_and_load: %s — no dates to process in %s',
            data_source,
            complete_date_range,
        )
        return

    for entity in entities:
        for dr in final_ranges:
            result = core.extract_and_load(
                get_extractor,
                data_source,
                staging_fs,
                entity,
                dr,
                overwrite,
                dry_run,
            )
            logger.info('%s %s: %s', entity, dr, result.type.value)


def _run_plan_grid_point_backfill(resources_fs: FileSystem) -> None:
    today = date.today()
    num_sample_files = count_sample_files(resources_fs)
    if num_sample_files == 0:
        raise ValueError('No sample files found on the resources filesystem.')
    manifest = plan_backfill(today, num_sample_files, resources_fs)
    logger.info(
        'plan_grid_point_backfill: wrote manifest (step2=%s, step3_batches=%d)',
        manifest.step2_batch,
        len(manifest.step3_batches),
    )


def _run_commit_grid_point_backfill(resources_fs: FileSystem) -> None:
    cursor = commit_backfill(resources_fs)
    logger.info('commit_grid_point_backfill: advanced cursor to %s', cursor)


def main() -> None:
    job_type = os.environ.get('JOB_TYPE', '')
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
    staging_fs = get_filesystem(config.staged_raw_data_storage)
    resources_fs = get_filesystem(config.resources_storage)

    if job_type == 'preprocess':
        _run_preprocess(staging_fs, data_source)
    elif job_type == 'extract_and_load':
        _run_extract_and_load(staging_fs, resources_fs, data_source)
    elif job_type == 'plan_grid_point_backfill':
        _run_plan_grid_point_backfill(resources_fs)
    elif job_type == 'commit_grid_point_backfill':
        _run_commit_grid_point_backfill(resources_fs)
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
