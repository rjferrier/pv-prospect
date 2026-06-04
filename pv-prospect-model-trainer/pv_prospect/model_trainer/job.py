"""Cloud Run Job entrypoint for the model trainer.

Reads ``DATA_VERSION`` from the environment (injected by the version Cloud
Workflow) and runs the full bootstrap → gate → promote pipeline.

Environment variables
---------------------
DATA_VERSION
    ISO date ``YYYY-MM-DD`` of the ``data-v<date>`` tag to train from.
    Injected by the version Cloud Workflow after the versioner completes.
GITHUB_DEPLOY_KEY
    SSH private key for cloning and pushing the instance repo.
    Injected from Secret Manager.
RUNTIME_ENV
    Config environment overlay (default ``default``).
"""

from __future__ import annotations

import logging
import os
import sys

from pv_prospect.common import configure_logging, get_config
from pv_prospect.model_trainer.config import ModelTrainerConfig
from pv_prospect.model_trainer.resources import get_config_dir
from pv_prospect.model_trainer.trainer import run_trainer_job

logger = logging.getLogger(__name__)


def main() -> None:
    data_version = os.environ.get('DATA_VERSION')
    if not data_version:
        raise ValueError('DATA_VERSION must be set')

    deploy_key = os.environ.get('GITHUB_DEPLOY_KEY', '')
    if not deploy_key:
        raise ValueError('GITHUB_DEPLOY_KEY must be set')

    config = get_config(ModelTrainerConfig, base_config_dirs=[get_config_dir()])
    promoted = run_trainer_job(data_version, config, deploy_key)

    if promoted:
        logger.info('Trainer job complete: model promoted for data-v%s', data_version)
    else:
        logger.info('Trainer job complete: gate rejected, incumbent model unchanged')


if __name__ == '__main__':
    configure_logging()
    try:
        main()
    except Exception:
        logger.exception('Model trainer job failed')
        sys.exit(1)
    sys.exit(0)
