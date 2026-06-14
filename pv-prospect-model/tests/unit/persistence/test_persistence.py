"""Round-trip test for save_artifact / load_artifact."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import torch
from pv_prospect.model.domain import (
    EvalReport,
    FeatureSpec,
    LosoReport,
    LosoSiteMetrics,
    ModelArtifact,
    PerSiteMetrics,
    SplitMetrics,
    TrainingConfig,
)
from pv_prospect.model.nets.pv import CapacityFactorNet
from pv_prospect.model.persistence import (
    _scaler_from_params,
    load_artifact,
    save_artifact,
)


def _make_artifact() -> ModelArtifact:
    feature_spec = FeatureSpec(
        continuous_features=('a', 'b', 'c'),
        binary_features=(),
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
    # r is reconstructed from training_config on load, so the in-memory net must
    # be built with the same r for the round-trip to be prediction-identical.
    model = CapacityFactorNet(feature_spec.input_size, r=training_config.r_fixed)
    scaler = _scaler_from_params(
        feature_spec.scaler_mean,
        feature_spec.scaler_scale,
        n_features=len(feature_spec.continuous_features),
    )
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


def test_round_trip_preserves_populated_loso(tmp_path: Path) -> None:
    """A populated eval_report.loso survives save → load identically."""
    loso = LosoReport(
        per_site=(
            LosoSiteMetrics(
                system_id=1, n=100, power_r2=0.8, power_mape=0.2, level_ratio=0.9
            ),
            LosoSiteMetrics(
                system_id=2, n=120, power_r2=0.7, power_mape=0.3, level_ratio=1.1
            ),
        ),
        pooled_power_r2=0.84,
        level_mean=1.0,
        level_band_1sigma=0.17,
    )
    artifact = _make_artifact()
    artifact.eval_report = dataclasses.replace(artifact.eval_report, loso=loso)
    save_artifact(artifact, tmp_path)
    loaded = load_artifact(tmp_path)
    assert loaded.eval_report.loso == loso


def test_round_trip_absent_loso_stays_none(tmp_path: Path) -> None:
    """A pre-LOSO artifact (loso=None) round-trips to None, not an error."""
    artifact = _make_artifact()
    assert artifact.eval_report.loso is None
    save_artifact(artifact, tmp_path)
    loaded = load_artifact(tmp_path)
    assert loaded.eval_report.loso is None


def test_round_trip_preserves_degradation_rate(tmp_path: Path) -> None:
    """The fixed degradation rate r survives save → load (via training_config)."""
    artifact = _make_artifact()
    save_artifact(artifact, tmp_path)
    loaded = load_artifact(tmp_path)
    loaded_model = loaded.model
    original_model = artifact.model
    assert isinstance(loaded_model, CapacityFactorNet)
    assert isinstance(original_model, CapacityFactorNet)
    assert loaded_model.r == original_model.r
    assert loaded_model.r == pytest.approx(artifact.training_config.r_fixed)
