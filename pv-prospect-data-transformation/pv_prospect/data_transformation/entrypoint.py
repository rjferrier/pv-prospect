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
"""

import os
import sys

from pv_prospect.common import (
    build_location_mapping_repo,
    build_pv_site_repo,
    get_config,
)
from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
from pv_prospect.data_transformation.config import DataTransformationConfig
from pv_prospect.data_transformation.core import (
    run_clean_pv,
    run_clean_weather,
    run_prepare_pv,
    run_prepare_weather,
)
from pv_prospect.etl import Extractor
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import FileSystem, get_filesystem


def _load_resources(raw_fs: FileSystem) -> None:
    """Load the PV site and location mapping repos from the raw data bucket."""
    extractor = Extractor(raw_fs)

    if extractor.file_exists('pv_sites.csv'):
        build_pv_site_repo(extractor.read_file('pv_sites.csv'))

    if extractor.file_exists('location_mapping.csv'):
        build_location_mapping_repo(extractor.read_file('location_mapping.csv'))


def main() -> None:
    step = os.environ.get('TRANSFORM_STEP', '')
    target_date = os.environ['DATE']
    date_str = target_date.replace('-', '')

    config = get_config(
        DataTransformationConfig,
        base_config_dirs=[get_etl_config_dir(), get_ds_config_dir()],
    )
    raw_fs = get_filesystem(config.staged_raw_data_storage)
    intermediate_fs = get_filesystem(config.intermediate_data_storage)
    model_fs = get_filesystem(config.staged_model_data_storage)
    pv_descriptor = config.data_sources.pv
    weather_descriptor = config.data_sources.weather

    _load_resources(raw_fs)

    print(f'[entrypoint] Starting {step} for date {target_date}')

    if step == 'clean_weather':
        run_clean_weather(raw_fs, intermediate_fs, weather_descriptor, date_str)
    elif step == 'clean_pv':
        pv_system_id = int(os.environ['PV_SYSTEM_ID'])
        run_clean_pv(raw_fs, intermediate_fs, pv_descriptor, pv_system_id, date_str)
    elif step == 'prepare_weather':
        run_prepare_weather(intermediate_fs, model_fs, weather_descriptor, date_str)
    elif step == 'prepare_pv':
        pv_system_id = int(os.environ['PV_SYSTEM_ID'])
        run_prepare_pv(
            intermediate_fs,
            model_fs,
            pv_descriptor,
            weather_descriptor,
            pv_system_id,
            date_str,
        )
    else:
        print(
            f'[entrypoint] ERROR: unknown TRANSFORM_STEP={step}',
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == '__main__':
    main()
