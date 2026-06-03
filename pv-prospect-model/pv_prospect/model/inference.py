"""Inference helpers: scale → forward → (inverse-transform).

Private core functions take raw artifact components so training can call them
before the artifact dataclass is constructed. Public ``predict_*`` functions
are thin wrappers that unpack a fully-built artifact.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import torch
from sklearn.preprocessing import StandardScaler

from pv_prospect.model.domain import (
    FeatureSpec,
    ModelArtifact,
    WeatherFeatureSpec,
    WeatherModelArtifact,
)
from pv_prospect.model.splits import scale_features


def _run_pv_forward(
    model: torch.nn.Module,
    scaler: StandardScaler,
    feature_spec: FeatureSpec,
    df: pd.DataFrame,
) -> np.ndarray:
    """Scale ``df``, run ``CapacityFactorNet``, return capacity-factor predictions.

    Device is derived from the model's own parameters so this works correctly
    whether the model is on CPU or GPU.
    """
    scaled = scale_features(
        df,
        scaler,
        list(feature_spec.continuous_features),
        list(feature_spec.binary_features),
    )
    device = next(model.parameters()).device
    with torch.no_grad():
        return (
            model(torch.FloatTensor(scaled.values).to(device)).cpu().numpy().flatten()
        )


def predict_capacity_factor(artifact: ModelArtifact, df: pd.DataFrame) -> np.ndarray:
    """Run PV inference on ``df``.

    ``df`` must contain the continuous and binary feature columns declared in
    ``artifact.feature_spec``. Returns a 1-D array of capacity-factor
    predictions, one per row of ``df``.
    """
    return _run_pv_forward(artifact.model, artifact.scaler, artifact.feature_spec, df)


def _run_weather_forward(
    model: torch.nn.Module,
    feature_scaler: StandardScaler,
    feature_spec: WeatherFeatureSpec,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Scale ``df``, run ``WeatherNet``, inverse-transform targets.

    Returns a DataFrame with columns ``feature_spec.target_columns``
    (temperature, direct_normal_irradiance, diffuse_radiation) in the
    natural physical units of the training data.
    """
    scaled = scale_features(df, feature_scaler, list(feature_spec.feature_columns), [])
    device = next(model.parameters()).device
    with torch.no_grad():
        out_scaled = model(torch.FloatTensor(scaled.values).to(device)).cpu().numpy()
    target_scale = np.array(feature_spec.target_scaler_scale)
    target_mean = np.array(feature_spec.target_scaler_mean)
    out_raw = out_scaled * target_scale + target_mean
    return pd.DataFrame(
        out_raw, columns=list(feature_spec.target_columns), index=df.index
    )


def predict_weather(artifact: WeatherModelArtifact, df: pd.DataFrame) -> pd.DataFrame:
    """Run weather inference on ``df``.

    ``df`` must contain the feature columns declared in
    ``artifact.feature_spec``. Returns a DataFrame with columns
    ``artifact.feature_spec.target_columns``
    (``temperature``, ``direct_normal_irradiance``, ``diffuse_radiation``).
    """
    return _run_weather_forward(
        artifact.model, artifact.feature_scaler, artifact.feature_spec, df
    )
