"""CLI entrypoint: train-pv and (stub) train-weather."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pv_prospect.model.domain import TrainingConfig
from pv_prospect.model.persistence import save_artifact
from pv_prospect.model.training.pv import train_pv


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='pv_prospect.model',
        description='PV Prospect model trainer',
    )
    sub = parser.add_subparsers(dest='command', required=True)

    pv = sub.add_parser('train-pv', help='Train the PV capacity-factor model')
    pv.add_argument(
        '--data-root',
        required=True,
        type=Path,
        help='Root of prepared data (contains pv/ subdirectory)',
    )
    pv.add_argument(
        '--pv-sites-csv',
        required=True,
        type=Path,
        help='Path to pv_sites.csv',
    )
    pv.add_argument(
        '--output-dir',
        required=True,
        type=Path,
        help='Directory to write the artifact into',
    )
    pv.add_argument(
        '--system-ids',
        type=lambda s: [int(x) for x in s.split(',')],
        default=None,
        help='Comma-separated PV system IDs (default: all sites in pv_sites.csv)',
    )
    pv.add_argument(
        '--censoring-margin', type=float, default=TrainingConfig.censoring_margin
    )
    pv.add_argument(
        '--cutoff-quantile', type=float, default=TrainingConfig.cutoff_quantile
    )
    pv.add_argument('--val-fraction', type=float, default=TrainingConfig.val_fraction)
    pv.add_argument('--patience', type=int, default=TrainingConfig.patience)
    pv.add_argument('--num-epochs', type=int, default=TrainingConfig.num_epochs)
    pv.add_argument('--batch-size', type=int, default=TrainingConfig.batch_size)
    pv.add_argument('--learning-rate', type=float, default=TrainingConfig.learning_rate)

    sub.add_parser('train-weather', help='(stub) Train the weather model')

    return parser


def _cmd_train_pv(args: argparse.Namespace) -> None:
    config = TrainingConfig(
        censoring_margin=args.censoring_margin,
        cutoff_quantile=args.cutoff_quantile,
        val_fraction=args.val_fraction,
        patience=args.patience,
        num_epochs=args.num_epochs,
        batch_size=args.batch_size,
        learning_rate=args.learning_rate,
    )
    artifact = train_pv(
        data_root=args.data_root,
        pv_sites_csv=args.pv_sites_csv,
        config=config,
        system_ids=args.system_ids,
    )
    save_artifact(artifact, args.output_dir)
    print(f'\nArtifact saved to {args.output_dir}')
    print(f'Test R² (capacity-factor): {artifact.eval_report.test_f_space.r2:.4f}')
    print(f'Test R² (clamped-power):   {artifact.eval_report.test_power_space.r2:.4f}')


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == 'train-pv':
        _cmd_train_pv(args)
    elif args.command == 'train-weather':
        print('train-weather not yet implemented', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
