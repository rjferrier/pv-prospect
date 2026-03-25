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
    (Optional) integer system id, required for pv steps
OPENMETEO_LOCATION
    (Optional) stringified lat_lon, required for weather steps
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


def _load_resources(raw_fs: FileSystem) -> None:
    """Load the PV site and location mapping repos from the raw data bucket."""
    extractor = Extractor(raw_fs)

    if extractor.file_exists('pv_sites.csv'):
        build_pv_site_repo(extractor.read_file('pv_sites.csv'))

    if extractor.file_exists('location_mapping.csv'):
        build_location_mapping_repo(extractor.read_file('location_mapping.csv'))


def _get_weather_location(pv_system_id: int) -> OpenMeteoTimeSeriesDescriptor:
    """Derive the weather grid point descriptor from a PV system ID."""
    loc = get_location_by_pv_system_id(pv_system_id)
    return OpenMeteoTimeSeriesDescriptor.from_coordinates(loc.latitude, loc.longitude)


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
        location = OpenMeteoTimeSeriesDescriptor.from_str(
            os.environ['OPENMETEO_LOCATION']
        )
        run_clean_weather(raw_fs, cleaned_fs, weather_descriptor, location, date_str)
    elif step == 'clean_pv':
        pv_system_id = int(os.environ['PV_SYSTEM_ID'])
        pv_ts = PVOutputTimeSeriesDescriptor(pv_system_id)
        run_clean_pv(raw_fs, cleaned_fs, pv_descriptor, pv_ts, date_str)
    elif step == 'prepare_weather':
        location = OpenMeteoTimeSeriesDescriptor.from_str(
            os.environ['OPENMETEO_LOCATION']
        )
        run_prepare_weather(
            cleaned_fs, batches_fs, weather_descriptor, location, date_str
        )
    elif step == 'prepare_pv':
        pv_system_id = int(os.environ['PV_SYSTEM_ID'])
        pv_ts = PVOutputTimeSeriesDescriptor(pv_system_id)
        weather_location = _get_weather_location(pv_system_id)
        run_prepare_pv(
            cleaned_fs,
            batches_fs,
            pv_descriptor,
            weather_descriptor,
            pv_ts,
            weather_location,
            date_str,
        )
    elif step == 'assemble_weather':
        assemble_prepared_weather(batches_fs, prepared_fs)
    elif step == 'assemble_pv':
        pv_system_id = int(os.environ['PV_SYSTEM_ID'])
        assemble_prepared_pv(batches_fs, prepared_fs, pv_system_id)
    else:
        logger.error('unknown TRANSFORM_STEP=%s', step)
        sys.exit(1)


if __name__ == '__main__':
    configure_logging()
    main()
