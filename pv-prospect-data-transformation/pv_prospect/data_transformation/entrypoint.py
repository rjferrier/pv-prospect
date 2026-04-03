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

from pv_prospect.common import (
    build_location_mapping_repo,
    build_pv_site_repo,
    configure_logging,
    get_config,
    get_location_by_pv_system_id,
    get_pv_site_by_system_id,
)
from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
from pv_prospect.data_sources import resolve_grid_point
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
from pv_prospect.etl.storage import FileSystem, get_filesystem

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

    if extractor.file_exists('location_mapping.csv'):
        build_location_mapping_repo(extractor.read_file('location_mapping.csv'))


def main() -> None:
    transformation = Transformation(os.environ.get('TRANSFORM_STEP', ''))
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

    config = get_config(
        DataTransformationConfig,
        base_config_dirs=[
            get_etl_config_dir(),
            get_ds_config_dir(),
            get_dt_config_dir(),
        ],
    )
    resources_fs = get_filesystem(config.resources_storage)
    raw_fs = get_filesystem(config.staged_raw_data_storage)
    cleaned_fs = get_filesystem(config.staged_cleaned_data_storage)
    batches_fs = get_filesystem(config.staged_prepared_batches_data_storage)
    prepared_fs = get_filesystem(config.staged_prepared_data_storage)

    _load_resources(resources_fs)

    if transformation in TRANSFORMATIONS_NEEDING_PV_SITE and pv_system_id is None:
        raise ValueError('PV_SYSTEM_ID must be set for PV steps.')

    logger.info('Starting %s for %s, split_by=%s', transformation, date_range, split_by)

    if transformation is Transformation.CLEAN_WEATHER:
        grid_point = resolve_grid_point(
            get_location_by_pv_system_id,
            pv_system_id=pv_system_id,
            location_str=location_str,
        )
        run_clean_weather(
            raw_fs,
            cleaned_fs,
            config.data_sources.weather,
            grid_point,
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
        grid_point = resolve_grid_point(
            get_location_by_pv_system_id,
            pv_system_id=pv_system_id,
            location_str=location_str,
        )
        run_prepare_weather(
            cleaned_fs,
            batches_fs,
            config.data_sources.weather,
            grid_point,
            date_range,
        )

    elif transformation is Transformation.PREPARE_PV:
        pv_site = get_pv_site_by_system_id(pv_system_id)  # type: ignore[arg-type]
        grid_point = resolve_grid_point(
            get_location_by_pv_system_id,
            pv_system_id=pv_system_id,
            location_str=location_str,
        )
        run_prepare_pv(
            cleaned_fs,
            batches_fs,
            config.data_sources.pv,
            config.data_sources.weather,
            pv_site,
            grid_point,
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


if __name__ == '__main__':
    configure_logging()
    try:
        main()
    except Exception:
        logger.exception('Unhandled exception')
        sys.exit(1)
