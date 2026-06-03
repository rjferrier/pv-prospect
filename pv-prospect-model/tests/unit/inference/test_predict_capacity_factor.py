"""Tests for predict_capacity_factor."""

from __future__ import annotations

import numpy as np
import pandas as pd
import torch
from pv_prospect.model import predict_capacity_factor
from pv_prospect.model.domain import (
    EvalReport,
    FeatureSpec,
    ModelArtifact,
    PerSiteMetrics,
    SplitMetrics,
    TrainingConfig,
)
from pv_prospect.model.nets.pv import CapacityFactorNet
from pv_prospect.model.persistence import _scaler_from_params
from pv_prospect.model.splits import scale_features


def _make_artifact() -> tuple[ModelArtifact, pd.DataFrame]:
    feature_spec = FeatureSpec(
        continuous_features=('f1', 'f2', 'f3'),
        binary_features=('b1',),
        target_column='y',
        scaler_mean=(1.0, 2.0, 3.0),
        scaler_scale=(0.5, 1.0, 2.0),
    )
    scaler = _scaler_from_params(
        feature_spec.scaler_mean, feature_spec.scaler_scale, n_features=3
    )
    model = CapacityFactorNet(feature_spec.input_size)
    model.eval()
    split = SplitMetrics(r2=0.8, rmse=0.05, mae=0.04, mse=0.0025)
    site = PerSiteMetrics(system_id=1, n=10, r2=0.8, rmse=0.05, mae=0.04)
    eval_report = EvalReport(
        train_f_space=split,
        test_f_space=split,
        train_power_space=split,
        test_power_space=split,
        test_per_site_f_space=(site,),
        test_per_site_power_space=(site,),
        cutoff='2026-01-01',
    )
    artifact = ModelArtifact(
        model=model,
        scaler=scaler,
        feature_spec=feature_spec,
        training_config=TrainingConfig(),
        eval_report=eval_report,
        cutoff=pd.Timestamp('2026-01-01'),
    )
    df = pd.DataFrame(
        {'f1': [1.0, 2.0], 'f2': [2.0, 3.0], 'f3': [3.0, 4.0], 'b1': [1, 0]}
    )
    return artifact, df


def test_returns_1d_array_with_one_value_per_row() -> None:
    artifact, df = _make_artifact()
    result = predict_capacity_factor(artifact, df)
    assert isinstance(result, np.ndarray)
    assert result.shape == (len(df),)


def test_values_match_manual_forward_pass() -> None:
    artifact, df = _make_artifact()
    result = predict_capacity_factor(artifact, df)

    scaled = scale_features(df, artifact.scaler, ['f1', 'f2', 'f3'], ['b1'])
    with torch.no_grad():
        expected = artifact.model(torch.FloatTensor(scaled.values)).numpy().flatten()

    np.testing.assert_allclose(result, expected)
