"""Cloud Run Job entrypoint for Data Versioning.

Reads task parameters from environment variables set by the Cloud Workflow
and calls the core versioning function.

Environment variables
---------------------
VERSION_DATE
    ISO date ``YYYY-MM-DD`` (optional; defaults to today).
GITHUB_DEPLOY_KEY
    SSH private key for pushing to the instance repo.
    Injected by Cloud Run from Secret Manager.
"""

import logging
import os
import sys
from datetime import date

from pv_prospect.common import configure_logging, get_config
from pv_prospect.data_versioner.config import DataVersionerConfig
from pv_prospect.data_versioner.core import version_data
from pv_prospect.data_versioner.resources import get_config_dir as get_dv_config_dir
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import get_filesystem

logger = logging.getLogger(__name__)


def main() -> None:
    version_date_str = os.environ.get('VERSION_DATE')
    version_date = (
        date.fromisoformat(version_date_str) if version_date_str else date.today()
    )

    deploy_key = os.environ.get('GITHUB_DEPLOY_KEY', '')
    if not deploy_key:
        raise ValueError('GITHUB_DEPLOY_KEY must be set.')

    config = get_config(
        DataVersionerConfig,
        base_config_dirs=[
            get_etl_config_dir(),
            get_dv_config_dir(),
        ],
    )

    prepared_fs = get_filesystem(config.staged_prepared_data_storage)
    cleaned_fs = get_filesystem(config.staged_cleaned_data_storage)
    batches_fs = get_filesystem(config.staged_prepared_batches_data_storage)

    version_data(prepared_fs, cleaned_fs, batches_fs, config, deploy_key, version_date)


if __name__ == '__main__':
    configure_logging()
    try:
        main()
    except Exception:
        logger.exception('Unhandled exception')
        sys.exit(1)
