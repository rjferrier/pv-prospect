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

_WEATHER = ['day_of_year', 'temperature', 'plane_of_array_irradiance']


def _make_artifact(r: float = 0.01) -> tuple[ModelArtifact, pd.DataFrame]:
    feature_spec = FeatureSpec(
        continuous_features=tuple(_WEATHER),
        binary_features=(),
        target_column='capacity_factor',
        scaler_mean=(180.0, 12.0, 150.0),
        scaler_scale=(100.0, 5.0, 80.0),
    )
    scaler = _scaler_from_params(
        feature_spec.scaler_mean, feature_spec.scaler_scale, n_features=3
    )
    model = CapacityFactorNet(feature_spec.input_size, r=r)
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
        {
            'day_of_year': [60.0, 200.0],
            'temperature': [8.0, 18.0],
            'plane_of_array_irradiance': [90.0, 220.0],
            'age_years': [0.0, 10.0],
        }
    )
    return artifact, df


def test_returns_1d_array_with_one_value_per_row() -> None:
    artifact, df = _make_artifact()
    result = predict_capacity_factor(artifact, df)
    assert isinstance(result, np.ndarray)
    assert result.shape == (len(df),)


def test_values_equal_head_times_degradation_factor() -> None:
    """Prediction == head(scaled weather) * (1 - r*age), age read from the df."""
    r = 0.01
    artifact, df = _make_artifact(r=r)
    result = predict_capacity_factor(artifact, df)

    model = artifact.model
    assert isinstance(model, CapacityFactorNet)
    scaled = scale_features(df, artifact.scaler, _WEATHER, [])
    age = df['age_years'].to_numpy().reshape(-1, 1)
    with torch.no_grad():
        head = model.network(torch.FloatTensor(scaled.values)).numpy()
    expected = (head * (1.0 - r * age)).flatten()

    np.testing.assert_allclose(result, expected, rtol=1e-5, atol=1e-6)
