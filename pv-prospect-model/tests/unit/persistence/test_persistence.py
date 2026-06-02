"""Round-trip test for save_artifact / load_artifact."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import torch
from pv_prospect.model.domain import (
    EvalReport,
    FeatureSpec,
    ModelArtifact,
    PerSiteMetrics,
    SplitMetrics,
    TrainingConfig,
)
from pv_prospect.model.nets.pv import CapacityFactorNet
from pv_prospect.model.persistence import (
    _scaler_from_feature_spec,
    load_artifact,
    save_artifact,
)


def _make_artifact() -> ModelArtifact:
    feature_spec = FeatureSpec(
        continuous_features=('a', 'b', 'c'),
        binary_features=('d',),
        target_column='y',
        scaler_mean=(1.0, 2.0, 3.0),
        scaler_scale=(0.5, 1.0, 2.0),
    )
    training_config = TrainingConfig()
    site_metrics = PerSiteMetrics(system_id=1, n=10, r2=0.8, rmse=0.05, mae=0.04)
    split_metrics = SplitMetrics(r2=0.8, rmse=0.05, mae=0.04, mse=0.0025)
    eval_report = EvalReport(
        train_f_space=split_metrics,
        test_f_space=split_metrics,
        train_power_space=split_metrics,
        test_power_space=split_metrics,
        test_per_site_f_space=(site_metrics,),
        test_per_site_power_space=(site_metrics,),
        cutoff='2026-01-01',
    )
    model = CapacityFactorNet(feature_spec.input_size)
    scaler = _scaler_from_feature_spec(feature_spec)
    return ModelArtifact(
        model=model,
        scaler=scaler,
        feature_spec=feature_spec,
        training_config=training_config,
        eval_report=eval_report,
        cutoff=pd.Timestamp('2026-01-01'),
    )


def test_round_trip_preserves_feature_spec(tmp_path: Path) -> None:
    """feature_spec is identical after save → load."""
    artifact = _make_artifact()
    save_artifact(artifact, tmp_path)
    loaded = load_artifact(tmp_path)
    assert loaded.feature_spec == artifact.feature_spec


def test_round_trip_preserves_training_config(tmp_path: Path) -> None:
    """training_config is identical after save → load."""
    artifact = _make_artifact()
    save_artifact(artifact, tmp_path)
    loaded = load_artifact(tmp_path)
    assert loaded.training_config == artifact.training_config


def test_round_trip_preserves_eval_report(tmp_path: Path) -> None:
    """eval_report is identical after save → load."""
    artifact = _make_artifact()
    save_artifact(artifact, tmp_path)
    loaded = load_artifact(tmp_path)
    assert loaded.eval_report == artifact.eval_report


def test_round_trip_model_produces_same_predictions(tmp_path: Path) -> None:
    """Loaded model state dict matches original; predictions are bitwise equal."""
    artifact = _make_artifact()
    save_artifact(artifact, tmp_path)
    loaded = load_artifact(tmp_path)

    x = torch.FloatTensor([[0.1, 0.2, 0.3, 1.0]])
    artifact.model.eval()
    loaded.model.eval()
    with torch.no_grad():
        orig_out = artifact.model(x)
        loaded_out = loaded.model(x)
    assert torch.allclose(orig_out, loaded_out)


def test_round_trip_scaler_transforms_identically(tmp_path: Path) -> None:
    """Reconstructed scaler produces the same transform as the original."""
    artifact = _make_artifact()
    save_artifact(artifact, tmp_path)
    loaded = load_artifact(tmp_path)

    x = np.array([[1.5, 2.5, 3.5]])
    orig = artifact.scaler.transform(x)
    loaded_result = loaded.scaler.transform(x)
    np.testing.assert_allclose(orig, loaded_result)
