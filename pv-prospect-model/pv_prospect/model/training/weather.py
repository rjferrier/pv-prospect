"""Weather model training pipeline: features → split → scale → train → evaluate."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import torch
from torch.utils.data import DataLoader, TensorDataset

from pv_prospect.model.domain import (
    WeatherFeatureSpec,
    WeatherModelArtifact,
    WeatherTrainingConfig,
)
from pv_prospect.model.evaluation import build_weather_eval_report
from pv_prospect.model.features.weather import (
    FEATURE_COLUMNS,
    TARGET_COLUMNS,
    build_weather_features,
)
from pv_prospect.model.nets.weather import WeatherNet
from pv_prospect.model.splits import fit_scaler, scale_features, temporal_holdout_split
from pv_prospect.model.training.loop import run_train_loop


def train_weather(
    data_root: Path,
    config: WeatherTrainingConfig | None = None,
) -> WeatherModelArtifact:
    """Run the full weather training pipeline and return a WeatherModelArtifact.

    Steps:
    1. Load and featurise prepared weather CSVs (monthly downsample, cyclic
       day_of_year encoding, elevation join).
    2. Temporal hold-out split (80th-percentile cutoff by default).
    3. Fit feature and target StandardScalers on train only; scale both splits.
    4. Carve a temporal val slice from the tail of train for early stopping.
    5. Train WeatherNet with Adam + MSE loss on standardised targets.
    6. Evaluate: per-target temporal metrics + block-climatology metrics vs IDW.
    7. Return WeatherModelArtifact with best-checkpoint model loaded.
    """
    if config is None:
        config = WeatherTrainingConfig()

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f'device: {device}')

    df = build_weather_features(data_root)

    train_df, test_df, cutoff = temporal_holdout_split(
        df, cutoff_quantile=config.cutoff_quantile
    )
    print(
        f'Temporal split at {cutoff.date()}: '
        f'{len(train_df)} train rows / {len(test_df)} test rows'
    )

    feature_scaler = fit_scaler(train_df, list(FEATURE_COLUMNS))
    target_scaler = fit_scaler(train_df, list(TARGET_COLUMNS))

    train_features = scale_features(train_df, feature_scaler, list(FEATURE_COLUMNS), [])
    test_features = scale_features(test_df, feature_scaler, list(FEATURE_COLUMNS), [])
    train_targets_scaled = scale_features(
        train_df, target_scaler, list(TARGET_COLUMNS), []
    )

    feature_spec = WeatherFeatureSpec(
        feature_columns=tuple(FEATURE_COLUMNS),
        target_columns=tuple(TARGET_COLUMNS),
        feature_scaler_mean=tuple(float(v) for v in feature_scaler.mean_),
        feature_scaler_scale=tuple(float(v) for v in feature_scaler.scale_),
        target_scaler_mean=tuple(float(v) for v in target_scaler.mean_),
        target_scaler_scale=tuple(float(v) for v in target_scaler.scale_),
    )

    n_train_total = len(train_features)
    n_val = int(n_train_total * config.val_fraction)
    n_train = n_train_total - n_val

    train_X = torch.FloatTensor(train_features.values[:n_train])
    train_y = torch.FloatTensor(train_targets_scaled.values[:n_train])
    val_X = torch.FloatTensor(train_features.values[n_train:])
    val_y = torch.FloatTensor(train_targets_scaled.values[n_train:])

    train_loader = DataLoader(
        TensorDataset(train_X, train_y),
        batch_size=config.batch_size,
        shuffle=True,
    )

    model = WeatherNet(feature_spec.input_size).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=config.learning_rate)

    result = run_train_loop(
        model=model,
        optimizer=optimizer,
        train_loader=train_loader,
        val_X=val_X,
        val_y=val_y,
        num_epochs=config.num_epochs,
        patience=config.patience,
        device=device,
    )
    print(
        f'Training stopped at epoch {result.best_epoch + 1} '
        f'(best val loss: {result.val_losses[result.best_epoch]:.6f})'
    )

    model.load_state_dict(result.best_state)
    model.eval()

    with torch.no_grad():
        train_pred_scaled = (
            model(torch.FloatTensor(train_features.values).to(device)).cpu().numpy()
        )
        test_pred_scaled = (
            model(torch.FloatTensor(test_features.values).to(device)).cpu().numpy()
        )

    target_scale = np.array(feature_spec.target_scaler_scale)
    target_mean = np.array(feature_spec.target_scaler_mean)
    train_pred_raw = train_pred_scaled * target_scale + target_mean
    test_pred_raw = test_pred_scaled * target_scale + target_mean

    eval_report = build_weather_eval_report(
        train_targets_raw=train_df[list(TARGET_COLUMNS)].values,
        test_targets_raw=test_df[list(TARGET_COLUMNS)].values,
        train_pred_raw=train_pred_raw,
        test_pred_raw=test_pred_raw,
        train_df=train_df,
        test_df=test_df,
        target_columns=list(TARGET_COLUMNS),
        cutoff=str(cutoff.date()),
    )

    return WeatherModelArtifact(
        model=model,
        feature_scaler=feature_scaler,
        target_scaler=target_scaler,
        feature_spec=feature_spec,
        training_config=config,
        eval_report=eval_report,
        cutoff=cutoff,
    )
