"""Cloud Run Job entrypoint for Data Transformation.

Reads task parameters from environment variables set by the Cloud Workflow
and calls the corresponding clean or prepare function.

Environment variables
---------------------
TRANSFORM_STEP
    ``clean_weather``, ``clean_pv``, ``prepare_weather``, or ``prepare_pv``
DATE
    ISO date ``YYYY-MM-DD`` to process
PV_SYSTEM_ID
    (Optional) integer system id; required for pv steps. For weather steps,
    accepted as an alternative to ``OPENMETEO_LOCATION`` — the location is
    derived via the location mapping repo.
OPENMETEO_LOCATION
    (Optional) stringified lat_lon; required for weather steps unless
    ``PV_SYSTEM_ID`` is provided instead. Exactly one of the two must be set.
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
)
from pv_prospect.data_sources import (
    OpenMeteoTimeSeriesDescriptor,
    PVOutputTimeSeriesDescriptor,
)
from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
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
from pv_prospect.etl import Extractor
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import FileSystem, get_filesystem

logger = logging.getLogger(__name__)


def _load_resources(resources_fs: FileSystem) -> None:
    """Load the PV site and location mapping repos from the raw data bucket."""
    extractor = Extractor(resources_fs)

    if extractor.file_exists('pv_sites.csv'):
        build_pv_site_repo(extractor.read_file('pv_sites.csv'))

    if extractor.file_exists('location_mapping.csv'):
        build_location_mapping_repo(extractor.read_file('location_mapping.csv'))


def _get_weather_ts_descriptor(pv_system_id: int) -> OpenMeteoTimeSeriesDescriptor:
    """Derive the weather grid point descriptor from a PV system ID."""
    loc = get_location_by_pv_system_id(pv_system_id)
    return OpenMeteoTimeSeriesDescriptor.from_coordinates(loc.latitude, loc.longitude)


def _resolve_pv_system_id() -> int:
    """Resolve the PV system ID from env vars for a PV step."""
    pv_system_id_str = os.environ.get('PV_SYSTEM_ID')
    if not pv_system_id_str:
        raise ValueError('A PV step requires PV_SYSTEM_ID to be set.')
    return int(pv_system_id_str)


def _resolve_weather_ts_descriptor() -> OpenMeteoTimeSeriesDescriptor:
    """Resolve the weather time series descriptor from env vars for a weather step.

    Accepts either OPENMETEO_LOCATION or PV_SYSTEM_ID, but not both.
    """
    location_str = os.environ.get('OPENMETEO_LOCATION')
    pv_system_id_str = os.environ.get('PV_SYSTEM_ID')

    if location_str and pv_system_id_str:
        raise ValueError(
            'Ambiguous input: both OPENMETEO_LOCATION and PV_SYSTEM_ID are set '
            'for a weather step. Provide exactly one.'
        )
    if location_str:
        return OpenMeteoTimeSeriesDescriptor.from_str(location_str)
    if pv_system_id_str:
        return _get_weather_ts_descriptor(int(pv_system_id_str))
    raise ValueError(
        'A weather step requires either OPENMETEO_LOCATION or PV_SYSTEM_ID to be set.'
    )


def main() -> None:
    step = os.environ.get('TRANSFORM_STEP', '')
    target_date = os.environ['DATE']
    date_str = target_date.replace('-', '')

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
    pv_descriptor = config.data_sources.pv
    weather_descriptor = config.data_sources.weather

    _load_resources(resources_fs)

    logger.info('Starting %s for date %s', step, target_date)

    if step == 'clean_weather':
        location = _resolve_weather_ts_descriptor()
        run_clean_weather(raw_fs, cleaned_fs, weather_descriptor, location, date_str)
    elif step == 'clean_pv':
        pv_system_id = _resolve_pv_system_id()
        pv_ts_descriptor = PVOutputTimeSeriesDescriptor(pv_system_id)
        run_clean_pv(raw_fs, cleaned_fs, pv_descriptor, pv_ts_descriptor, date_str)
    elif step == 'prepare_weather':
        location = _resolve_weather_ts_descriptor()
        run_prepare_weather(
            cleaned_fs, batches_fs, weather_descriptor, location, date_str
        )
    elif step == 'prepare_pv':
        pv_system_id = _resolve_pv_system_id()
        pv_ts_descriptor = PVOutputTimeSeriesDescriptor(pv_system_id)
        weather_ts_descriptor = _get_weather_ts_descriptor(pv_system_id)
        run_prepare_pv(
            cleaned_fs,
            batches_fs,
            pv_descriptor,
            weather_descriptor,
            pv_ts_descriptor,
            weather_ts_descriptor,
            date_str,
        )
    elif step == 'assemble_weather':
        assemble_prepared_weather(batches_fs, prepared_fs)
    elif step == 'assemble_pv':
        pv_system_id = _resolve_pv_system_id()
        assemble_prepared_pv(batches_fs, prepared_fs, pv_system_id)
    else:
        logger.error('unknown TRANSFORM_STEP=%s', step)
        sys.exit(1)


if __name__ == '__main__':
    configure_logging()
    try:
        main()
    except Exception:
        logger.exception('Unhandled exception')
        sys.exit(1)
