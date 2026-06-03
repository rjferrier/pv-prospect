"""CLI entrypoint for the model trainer.

Subcommands
-----------
bootstrap
    Clone the instance repo at a ``data-v<date>`` tag, pull prepared data,
    train PV and weather models, and write the promoted-artifact store to a
    local directory. Requires ``GITHUB_DEPLOY_KEY`` (or a local-path
    ``instance_repo_url`` in config-local.yaml that needs no SSH key).

Environment variables
---------------------
GITHUB_DEPLOY_KEY
    SSH private key for cloning the instance repo. Required for remote URLs;
    omit or leave empty when ``instance_repo_url`` is a local ``file://`` path.
RUNTIME_ENV
    Config environment (default: ``local``).  Set to ``default`` or a custom
    value to load the matching ``config-{env}.yaml`` overlay.
CONFIG_DIR
    Optional path to an extra config directory overlaid on top of the
    package-bundled ``config-default.yaml``.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from pv_prospect.common import configure_logging, get_config
from pv_prospect.model_trainer.bootstrap import bootstrap_models
from pv_prospect.model_trainer.config import ModelTrainerConfig
from pv_prospect.model_trainer.resources import get_config_dir

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='pv_prospect.model_trainer',
        description='PV Prospect model trainer',
    )
    sub = parser.add_subparsers(dest='command', required=True)

    bootstrap = sub.add_parser(
        'bootstrap',
        help='Train models locally from a versioned corpus snapshot',
    )
    bootstrap.add_argument(
        '--data-version',
        required=True,
        help='ISO date of the data-v<date> tag to clone (e.g. 2026-05-31)',
    )
    bootstrap.add_argument(
        '--output-dir',
        required=True,
        type=Path,
        help='Directory to write the promoted-artifact store into',
    )

    return parser


def _cmd_bootstrap(args: argparse.Namespace) -> None:
    deploy_key = os.environ.get('GITHUB_DEPLOY_KEY', '')

    config = get_config(ModelTrainerConfig, base_config_dirs=[get_config_dir()])

    bootstrap_models(
        data_version=args.data_version,
        output_dir=args.output_dir,
        config=config,
        deploy_key=deploy_key,
    )

    print(f'\nArtifact store written to {args.output_dir}')
    print('  promoted/pv/      ← PV model artifact')
    print('  promoted/weather/ ← weather model artifact')
    print('  current.json      ← metadata pointer')


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == 'bootstrap':
        _cmd_bootstrap(args)


if __name__ == '__main__':
    configure_logging()
    main()
