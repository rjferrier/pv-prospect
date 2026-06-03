"""PV model training pipeline: features → split → scale → train → evaluate."""

from __future__ import annotations

from pathlib import Path

import torch
from torch.utils.data import DataLoader, TensorDataset

from pv_prospect.model.domain import (
    FeatureSpec,
    ModelArtifact,
    TrainingConfig,
)
from pv_prospect.model.evaluation import build_eval_report
from pv_prospect.model.features import (
    BINARY_FEATURES,
    CONTINUOUS_FEATURES,
    TARGET_COLUMN,
    build_pv_features,
)
from pv_prospect.model.inference import _run_pv_forward
from pv_prospect.model.nets import CapacityFactorNet
from pv_prospect.model.splits import fit_scaler, scale_features, temporal_holdout_split
from pv_prospect.model.training.loop import run_train_loop


def train_pv(
    data_root: Path,
    pv_sites_csv: Path,
    config: TrainingConfig | None = None,
    system_ids: list[int] | None = None,
) -> ModelArtifact:
    """Run the full PV training pipeline and return a ModelArtifact.

    Steps:
    1. Load and featurise per-site prepared CSVs (censoring, augmentation).
    2. Temporal hold-out split.
    3. Fit StandardScaler on train; scale both splits.
    4. Carve a temporal val slice from the tail of train (early stopping).
    5. Train CapacityFactorNet with Adam + MSE loss.
    6. Evaluate in capacity-factor space and clamped-power space.
    7. Return ModelArtifact with best-checkpoint model loaded.
    """
    if config is None:
        config = TrainingConfig()

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f'device: {device}')

    df = build_pv_features(
        data_root=data_root,
        pv_sites_csv=pv_sites_csv,
        system_ids=system_ids,
        censoring_margin=config.censoring_margin,
    )

    train_df, test_df, cutoff = temporal_holdout_split(
        df, cutoff_quantile=config.cutoff_quantile
    )
    print(
        f'Temporal split at {cutoff.date()}: '
        f'{len(train_df)} train rows / {len(test_df)} test rows'
    )

    scaler = fit_scaler(train_df, list(CONTINUOUS_FEATURES))
    train_features = scale_features(
        train_df, scaler, list(CONTINUOUS_FEATURES), list(BINARY_FEATURES)
    )

    feature_spec = FeatureSpec(
        continuous_features=tuple(CONTINUOUS_FEATURES),
        binary_features=tuple(BINARY_FEATURES),
        target_column=TARGET_COLUMN,
        scaler_mean=tuple(float(v) for v in scaler.mean_),
        scaler_scale=tuple(float(v) for v in scaler.scale_),
    )

    n_train_total = len(train_features)
    n_val = int(n_train_total * config.val_fraction)
    n_train = n_train_total - n_val

    train_X = torch.FloatTensor(train_features.values[:n_train])
    train_y = torch.FloatTensor(train_df[TARGET_COLUMN].values[:n_train]).reshape(-1, 1)
    val_X = torch.FloatTensor(train_features.values[n_train:])
    val_y = torch.FloatTensor(train_df[TARGET_COLUMN].values[n_train:]).reshape(-1, 1)

    train_loader = DataLoader(
        TensorDataset(train_X, train_y),
        batch_size=config.batch_size,
        shuffle=True,
    )

    model = CapacityFactorNet(feature_spec.input_size).to(device)
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

    train_pred_cf = _run_pv_forward(model, scaler, feature_spec, train_df)
    test_pred_cf = _run_pv_forward(model, scaler, feature_spec, test_df)

    eval_report = build_eval_report(
        train_target=train_df[TARGET_COLUMN].values,
        test_target=test_df[TARGET_COLUMN].values,
        train_pred_cf=train_pred_cf,
        test_pred_cf=test_pred_cf,
        train_power=train_df['power'].values,
        test_power=test_df['power'].values,
        train_panel_capacity=train_df['panels_capacity'].values,
        test_panel_capacity=test_df['panels_capacity'].values,
        train_inverter_capacity=train_df['inverter_capacity'].values,
        test_inverter_capacity=test_df['inverter_capacity'].values,
        train_system_ids=train_df['system_id'].values,
        test_system_ids=test_df['system_id'].values,
        cutoff=str(cutoff.date()),
    )

    return ModelArtifact(
        model=model,
        scaler=scaler,
        feature_spec=feature_spec,
        training_config=config,
        eval_report=eval_report,
        cutoff=cutoff,
    )
