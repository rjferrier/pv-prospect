"""CLI entrypoint: train-pv and train-weather."""

from __future__ import annotations

import argparse
import dataclasses
import json
from pathlib import Path

from pv_prospect.model.domain import TrainingConfig, WeatherTrainingConfig
from pv_prospect.model.persistence import save_artifact, save_weather_artifact
from pv_prospect.model.training.loso import loso_eval
from pv_prospect.model.training.pv import train_pv
from pv_prospect.model.training.weather import train_weather


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

    loso = sub.add_parser(
        'loso-pv',
        help='Run the leave-one-site-out cross-site eval (offline; one training per site)',
    )
    loso.add_argument('--data-root', required=True, type=Path)
    loso.add_argument('--pv-sites-csv', required=True, type=Path)
    loso.add_argument(
        '--system-ids',
        type=lambda s: [int(x) for x in s.split(',')],
        default=None,
        help='Comma-separated PV system IDs (default: all sites in pv_sites.csv)',
    )
    loso.add_argument(
        '--seed',
        type=int,
        default=None,
        help='Seed set before each fold (reproducible)',
    )
    loso.add_argument(
        '--output-json',
        type=Path,
        default=None,
        help='Optional path to write the LosoReport as JSON',
    )
    loso.add_argument('--num-epochs', type=int, default=TrainingConfig.num_epochs)

    weather = sub.add_parser(
        'train-weather', help='Train the weather climatology model'
    )
    weather.add_argument(
        '--data-root',
        required=True,
        type=Path,
        help='Root of prepared data (contains weather/ subdirectory)',
    )
    weather.add_argument(
        '--output-dir',
        required=True,
        type=Path,
        help='Directory to write the artifact into',
    )
    weather.add_argument(
        '--cutoff-quantile',
        type=float,
        default=WeatherTrainingConfig.cutoff_quantile,
    )
    weather.add_argument(
        '--val-fraction', type=float, default=WeatherTrainingConfig.val_fraction
    )
    weather.add_argument('--patience', type=int, default=WeatherTrainingConfig.patience)
    weather.add_argument(
        '--num-epochs', type=int, default=WeatherTrainingConfig.num_epochs
    )
    weather.add_argument(
        '--batch-size', type=int, default=WeatherTrainingConfig.batch_size
    )
    weather.add_argument(
        '--learning-rate', type=float, default=WeatherTrainingConfig.learning_rate
    )

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


def _cmd_loso_pv(args: argparse.Namespace) -> None:
    config = TrainingConfig(num_epochs=args.num_epochs)
    report = loso_eval(
        data_root=args.data_root,
        pv_sites_csv=args.pv_sites_csv,
        config=config,
        system_ids=args.system_ids,
        seed=args.seed,
    )
    print(f'\n{"site":>6} {"n":>5} {"pwR2":>7} {"level":>7} {"norm":>7}')
    for m in report.per_site:
        norm = m.level_ratio / report.level_mean
        print(
            f'{m.system_id:>6} {m.n:>5} {m.power_r2:>7.3f} '
            f'{m.level_ratio:>7.3f} {norm:>7.3f}'
        )
    print(f'\nPooled power R²:        {report.pooled_power_r2:.4f}')
    print(f'Out-of-sample mean level: {report.level_mean:.4f}')
    print(
        f'Prospect band (1σ):     ±{100 * report.level_band_1sigma:.1f}%  '
        f'(2σ ±{200 * report.level_band_1sigma:.1f}%)'
    )
    if args.output_json is not None:
        with open(args.output_json, 'w') as f:
            json.dump(dataclasses.asdict(report), f, indent=2)
        print(f'\nLosoReport written to {args.output_json}')


def _cmd_train_weather(args: argparse.Namespace) -> None:
    config = WeatherTrainingConfig(
        cutoff_quantile=args.cutoff_quantile,
        val_fraction=args.val_fraction,
        patience=args.patience,
        num_epochs=args.num_epochs,
        batch_size=args.batch_size,
        learning_rate=args.learning_rate,
    )
    artifact = train_weather(data_root=args.data_root, config=config)
    save_weather_artifact(artifact, args.output_dir)
    print(f'\nArtifact saved to {args.output_dir}')
    for m in artifact.eval_report.temporal_test:
        print(f'  Temporal test R² ({m.target}): {m.r2:.4f}  RMSE: {m.rmse:.4f}')
    print()
    for m in artifact.eval_report.block_clim_model:
        idw = next(
            i for i in artifact.eval_report.block_clim_idw if i.target == m.target
        )
        lift_pct = 100.0 * (idw.rmse - m.rmse) / idw.rmse
        print(
            f'  Block-clim RMSE ({m.target}): model={m.rmse:.4f}  '
            f'IDW={idw.rmse:.4f}  lift={lift_pct:+.1f}%'
        )


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == 'train-pv':
        _cmd_train_pv(args)
    elif args.command == 'loso-pv':
        _cmd_loso_pv(args)
    elif args.command == 'train-weather':
        _cmd_train_weather(args)


if __name__ == '__main__':
    main()
