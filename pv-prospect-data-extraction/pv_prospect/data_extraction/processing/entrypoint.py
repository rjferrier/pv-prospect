"""Cloud Run Job entrypoint.

Reads task parameters from environment variables set by the Cloud Workflow
and calls the corresponding core function.

Environment variables
---------------------
JOB_TYPE
    ``preprocess`` or ``extract_and_load``

For **preprocess**:
    DATA_SOURCE — ``pv`` or ``weather`` (optional; defaults to weather)

For **extract_and_load**:
    DATA_SOURCE     — ``pv`` or ``weather`` (optional; defaults to weather)
    PV_SYSTEM_ID    — integer system id (required for PV sources; for weather
                      sources, exactly one of PV_SYSTEM_ID or OPENMETEO_LOCATION
                      must be set)
    OPENMETEO_LOCATION
                    — stringified lat_lon (e.g. ``504900_-35400``); alternative
                      to PV_SYSTEM_ID for weather sources
    START_DATE      — ISO date ``YYYY-MM-DD``
    END_DATE        — ISO date ``YYYY-MM-DD``, exclusive (optional; defaults to
                        START_DATE + 1 day)
    OVERWRITE       — ``true`` or ``false`` (default ``false``)
    DRY_RUN         — ``true`` or ``false`` (default ``false``)
    BY_WEEK         — ``true`` or ``false`` (chunking hint)
"""

import logging
import os
import sys

from pv_prospect.common import (
    build_location_mapping_repo,
    build_pv_site_repo,
    configure_logging,
    get_config,
    get_pv_site_by_system_id,
)
from pv_prospect.common.domain import (
    AnyEntity,
    GridPoint,
    Period,
)
from pv_prospect.data_extraction import (
    DataSource,
    get_extractor,
    supports_multi_date,
)
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.processing import core
from pv_prospect.data_extraction.resources import get_config_dir as get_de_config_dir
from pv_prospect.data_sources import DataSourceType
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


def _run_extract_and_load(
    staging_fs: FileSystem,
    resources_fs: FileSystem,
    data_source: DataSource,
) -> None:
    pv_system_id = _env_int('PV_SYSTEM_ID')
    grid_point_id = os.environ.get('OPENMETEO_LOCATION')
    overwrite = _env_bool('OVERWRITE')
    dry_run = _env_bool('DRY_RUN')
    by_week = _env_bool('BY_WEEK')

    try:
        complete_date_range = build_date_range(
            os.environ['START_DATE'], os.environ.get('END_DATE')
        )
    except DegenerateDateRange as e:
        logger.error('%s', e)
        sys.exit(1)

    resources_extractor = Extractor(resources_fs)
    build_pv_site_repo(resources_extractor.read_file(core.PV_SITES_CSV_FILE))
    build_location_mapping_repo(
        resources_extractor.read_file(core.LOCATION_MAPPING_CSV_FILE)
    )

    # Resolve PV site or weather grid point
    entity: AnyEntity
    if grid_point_id is not None:
        entity = GridPoint.from_id(grid_point_id)
    elif pv_system_id is not None:
        entity = get_pv_site_by_system_id(pv_system_id)
    else:
        raise ValueError('Either PV_SYSTEM_ID or OPENMETEO_LOCATION must be set.')

    logger.info(
        'extract_and_load: %s, %s, %s, by_week=%s',
        data_source,
        entity,
        complete_date_range,
        by_week,
    )

    split_period = Period.WEEK if by_week else Period.DAY
    sub_date_ranges = complete_date_range.split_by(split_period)

    if by_week and not supports_multi_date(data_source):
        final_ranges = []
        for dr in sub_date_ranges:
            final_ranges.extend(dr.split_by(Period.DAY))
    else:
        final_ranges = sub_date_ranges

    if not final_ranges:
        logger.warning(
            'extract_and_load: %s, %s — no dates to process in %s',
            data_source,
            entity,
            complete_date_range,
        )
        return

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
        logger.info('%s: %s', dr, result.type.value)


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
