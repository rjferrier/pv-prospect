"""One-time seed for the validation window artifact.

Reads the locally-pulled DVC prepared corpus and writes a 90-day window
to the configured validation_window_storage. Run this once at launch before
enabling the daily maintain_validation_window job.

Usage:
    poetry run python scripts/seed_validation_window.py \\
        --prepared-dir /path/to/instance-repo/data/prepared \\
        [--days 90]

The window storage destination is taken from the standard config
(validation_window_storage in config-default.yaml / config-local.yaml),
so RUNTIME_ENV and CONFIG_DIR are honoured as usual.
"""

import argparse
import logging
import sys

from pv_prospect.common.config_parser import get_config
from pv_prospect.data_transformation.config import DataTransformationConfig
from pv_prospect.data_transformation.processing import seed_validation_window
from pv_prospect.data_transformation.resources import (
    get_config_dir as get_dt_config_dir,
)
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import get_filesystem
from pv_prospect.etl.storage.backends.local import LocalStorageConfig

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--prepared-dir',
        required=True,
        metavar='DIR',
        help='Local path to the prepared data directory (e.g. data/prepared).',
    )
    parser.add_argument(
        '--days',
        type=int,
        default=90,
        metavar='N',
        help='Window size in days (default: 90).',
    )
    args = parser.parse_args()

    config = get_config(
        DataTransformationConfig,
        base_config_dirs=[get_etl_config_dir(), get_dt_config_dir()],
    )
    if config.validation_window_storage is None:
        print(
            'ERROR: validation_window_storage is not configured.',
            file=sys.stderr,
        )
        sys.exit(1)

    prepared_fs = get_filesystem(LocalStorageConfig(base_dir=args.prepared_dir))
    window_fs = get_filesystem(config.validation_window_storage)

    seed_validation_window(prepared_fs, window_fs, args.days)


if __name__ == '__main__':
    main()
