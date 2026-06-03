"""Tests for predict_weather."""

from __future__ import annotations

import numpy as np
import pandas as pd
import torch
from pv_prospect.model import predict_weather
from pv_prospect.model.domain import (
    WeatherEvalReport,
    WeatherFeatureSpec,
    WeatherModelArtifact,
    WeatherTargetMetrics,
    WeatherTrainingConfig,
)
from pv_prospect.model.nets.weather import WeatherNet
from pv_prospect.model.persistence import _scaler_from_params
from pv_prospect.model.splits import scale_features


def _make_artifact() -> tuple[WeatherModelArtifact, pd.DataFrame]:
    feature_spec = WeatherFeatureSpec(
        feature_columns=(
            'latitude',
            'longitude',
            'elevation',
            'day_of_year_sin',
            'day_of_year_cos',
        ),
        target_columns=('temperature', 'direct_normal_irradiance', 'diffuse_radiation'),
        feature_scaler_mean=(52.0, -1.5, 50.0, 0.0, 0.0),
        feature_scaler_scale=(2.0, 2.0, 100.0, 1.0, 1.0),
        target_scaler_mean=(10.0, 200.0, 80.0),
        target_scaler_scale=(5.0, 150.0, 50.0),
    )
    feature_scaler = _scaler_from_params(
        feature_spec.feature_scaler_mean,
        feature_spec.feature_scaler_scale,
        n_features=5,
    )
    target_scaler = _scaler_from_params(
        feature_spec.target_scaler_mean, feature_spec.target_scaler_scale, n_features=3
    )
    model = WeatherNet(feature_spec.input_size)
    model.eval()
    target_metrics = tuple(
        WeatherTargetMetrics(target=t, r2=0.5, rmse=10.0, mae=8.0, bias=0.1)
        for t in feature_spec.target_columns
    )
    eval_report = WeatherEvalReport(
        temporal_test=target_metrics,
        block_clim_model=target_metrics,
        block_clim_idw=target_metrics,
        cutoff='2026-01-01',
    )
    artifact = WeatherModelArtifact(
        model=model,
        feature_scaler=feature_scaler,
        target_scaler=target_scaler,
        feature_spec=feature_spec,
        training_config=WeatherTrainingConfig(),
        eval_report=eval_report,
        cutoff=pd.Timestamp('2026-01-01'),
    )
    df = pd.DataFrame(
        {
            'latitude': [51.5, 53.0],
            'longitude': [-0.1, -2.5],
            'elevation': [10.0, 50.0],
            'day_of_year_sin': [0.5, -0.5],
            'day_of_year_cos': [0.866, 0.866],
        }
    )
    return artifact, df


def test_returns_dataframe_with_correct_shape() -> None:
    artifact, df = _make_artifact()
    result = predict_weather(artifact, df)
    assert isinstance(result, pd.DataFrame)
    assert result.shape == (len(df), len(artifact.feature_spec.target_columns))


def test_returns_dataframe_with_correct_columns() -> None:
    artifact, df = _make_artifact()
    result = predict_weather(artifact, df)
    assert list(result.columns) == list(artifact.feature_spec.target_columns)


def test_values_match_manual_forward_pass() -> None:
    artifact, df = _make_artifact()
    result = predict_weather(artifact, df)

    scaled = scale_features(
        df, artifact.feature_scaler, list(artifact.feature_spec.feature_columns), []
    )
    with torch.no_grad():
        out_scaled = artifact.model(torch.FloatTensor(scaled.values)).numpy()
    target_scale = np.array(artifact.feature_spec.target_scaler_scale)
    target_mean = np.array(artifact.feature_spec.target_scaler_mean)
    expected = out_scaled * target_scale + target_mean

    np.testing.assert_allclose(result.values, expected)
