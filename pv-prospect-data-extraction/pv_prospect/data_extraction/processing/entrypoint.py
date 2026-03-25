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
    PV_SYSTEM_ID    — integer system id
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
from datetime import date, timedelta

from pv_prospect.common import (
    DateRange,
    Period,
    build_location_mapping_repo,
    build_pv_site_repo,
    configure_logging,
    get_config,
    get_pv_site_by_system_id,
)
from pv_prospect.data_extraction import (
    SourceDescriptor,
    get_extractor,
    supports_multi_date,
)
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.processing import core
from pv_prospect.data_extraction.resources import get_config_dir as get_de_config_dir
from pv_prospect.data_sources import DataSource
from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
from pv_prospect.etl import Extractor
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import FileSystem, get_filesystem
from pv_prospect.etl.storage.resolve import resolve_dvc_path

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).lower() in ('true', '1', 'yes')


def _run_preprocess(
    versioned_resources_fs: FileSystem,
    staging_fs: FileSystem,
    dvc_prefix: str,
    source_descriptor: SourceDescriptor,
) -> None:
    logger.info('preprocess: %s', source_descriptor)
    core.preprocess(
        resolve_dvc_path,
        versioned_resources_fs,
        staging_fs,
        dvc_prefix,
        source_descriptor,
    )


def _run_extract_and_load(
    staging_fs: FileSystem,
    source_descriptor: SourceDescriptor,
) -> None:
    pv_system_id = int(os.environ['PV_SYSTEM_ID'])
    start_date = date.fromisoformat(os.environ['START_DATE'])
    end_date_env = os.environ.get('END_DATE')
    end_date = (
        date.fromisoformat(end_date_env)
        if end_date_env
        else start_date + timedelta(days=1)
    )
    overwrite = _env_bool('OVERWRITE')
    dry_run = _env_bool('DRY_RUN')
    by_week = _env_bool('BY_WEEK')

    complete_date_range = DateRange(start_date, end_date)
    logger.info(
        'extract_and_load: %s, site=%s, %s, by_week=%s',
        source_descriptor,
        pv_system_id,
        complete_date_range,
        by_week,
    )

    staging_extractor = Extractor(staging_fs)
    build_pv_site_repo(staging_extractor.read_file(core.PV_SITES_CSV_FILE))
    build_location_mapping_repo(
        staging_extractor.read_file(core.LOCATION_MAPPING_CSV_FILE)
    )

    split_period = Period.WEEK if by_week else Period.DAY
    sub_date_ranges = complete_date_range.split_by(split_period)

    if by_week and not supports_multi_date(source_descriptor):
        # Fall back to days if the source doesn't support multi-date extraction
        final_ranges = []
        for dr in sub_date_ranges:
            final_ranges.extend(dr.split_by(Period.DAY))
    else:
        final_ranges = sub_date_ranges

    for dr in final_ranges:
        result = core.extract_and_load(
            get_pv_site_by_system_id,
            get_extractor,
            source_descriptor,
            staging_fs,
            pv_system_id,
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
    if source_env:
        source_descriptor = config.data_sources.get_descriptor(DataSource(source_env))
    else:
        source_descriptor = config.data_sources.get_descriptor(DataSource.WEATHER)

    # Cloud Run always uses GCS — resolve storage backends once.
    staging_fs = get_filesystem(config.staged_raw_data_storage)
    versioned_resources_fs = get_filesystem(config.versioned_resources_storage)
    tracking = config.versioned_resources_storage.tracking
    dvc_prefix = tracking.prefix if tracking else ''

    if job_type == 'preprocess':
        _run_preprocess(
            versioned_resources_fs, staging_fs, dvc_prefix, source_descriptor
        )
    elif job_type == 'extract_and_load':
        _run_extract_and_load(staging_fs, source_descriptor)
    else:
        logger.error('unknown JOB_TYPE=%r', job_type)
        sys.exit(1)


if __name__ == '__main__':
    configure_logging()
    main()
