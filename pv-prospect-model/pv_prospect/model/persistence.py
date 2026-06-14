"""Save and load model artifacts to/from a directory.

PV artifact layout::

    <output_dir>/
        model.pt            # torch state dict
        feature_spec.json   # FeatureSpec (includes scaler parameters)
        training_config.json
        eval_report.json

Weather artifact layout (same filenames, different JSON schemas)::

    <output_dir>/
        model.pt
        feature_spec.json   # WeatherFeatureSpec (feature + target scaler params)
        training_config.json
        eval_report.json
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from sklearn.preprocessing import StandardScaler

from pv_prospect.model.domain import (
    EvalReport,
    FeatureSpec,
    ModelArtifact,
    PerSiteMetrics,
    SplitMetrics,
    TrainingConfig,
    WeatherEvalReport,
    WeatherFeatureSpec,
    WeatherModelArtifact,
    WeatherTargetMetrics,
    WeatherTrainingConfig,
)
from pv_prospect.model.nets import CapacityFactorNet, WeatherNet


def save_artifact(artifact: ModelArtifact, output_dir: Path) -> None:
    """Persist a ModelArtifact to ``output_dir`` (created if absent)."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    torch.save(artifact.model.state_dict(), output_dir / 'model.pt')

    with open(output_dir / 'feature_spec.json', 'w') as f:
        json.dump(dataclasses.asdict(artifact.feature_spec), f, indent=2)

    with open(output_dir / 'training_config.json', 'w') as f:
        json.dump(dataclasses.asdict(artifact.training_config), f, indent=2)

    with open(output_dir / 'eval_report.json', 'w') as f:
        json.dump(dataclasses.asdict(artifact.eval_report), f, indent=2)


def load_artifact(artifact_dir: Path) -> ModelArtifact:
    """Load a ModelArtifact saved by ``save_artifact``."""
    artifact_dir = Path(artifact_dir)

    with open(artifact_dir / 'feature_spec.json') as f:
        spec_dict = json.load(f)
    feature_spec = FeatureSpec(
        continuous_features=tuple(spec_dict['continuous_features']),
        binary_features=tuple(spec_dict['binary_features']),
        target_column=spec_dict['target_column'],
        scaler_mean=tuple(spec_dict['scaler_mean']),
        scaler_scale=tuple(spec_dict['scaler_scale']),
    )

    with open(artifact_dir / 'training_config.json') as f:
        config_dict = json.load(f)
    training_config = TrainingConfig(**config_dict)

    with open(artifact_dir / 'eval_report.json') as f:
        report_dict = json.load(f)
    eval_report = _eval_report_from_dict(report_dict)

    scaler = _scaler_from_params(
        feature_spec.scaler_mean,
        feature_spec.scaler_scale,
        n_features=len(feature_spec.continuous_features),
    )

    # r is a non-persistent buffer: sourced from the training config, not the
    # state dict. An old-basis artifact (age was a head feature → larger
    # input_size) will fail loudly here on a state-dict shape mismatch.
    model = CapacityFactorNet(feature_spec.input_size, r=training_config.r_fixed)
    state_dict = torch.load(artifact_dir / 'model.pt', weights_only=True)
    model.load_state_dict(state_dict)
    model.eval()

    return ModelArtifact(
        model=model,
        scaler=scaler,
        feature_spec=feature_spec,
        training_config=training_config,
        eval_report=eval_report,
        cutoff=pd.Timestamp(eval_report.cutoff),
    )


def _eval_report_from_dict(d: dict) -> EvalReport:  # type: ignore[type-arg]
    return EvalReport(
        train_f_space=SplitMetrics(**d['train_f_space']),
        test_f_space=SplitMetrics(**d['test_f_space']),
        train_power_space=SplitMetrics(**d['train_power_space']),
        test_power_space=SplitMetrics(**d['test_power_space']),
        test_per_site_f_space=tuple(
            PerSiteMetrics(**m) for m in d['test_per_site_f_space']
        ),
        test_per_site_power_space=tuple(
            PerSiteMetrics(**m) for m in d['test_per_site_power_space']
        ),
        cutoff=d['cutoff'],
    )


# ---------------------------------------------------------------------------
# Weather artifact
# ---------------------------------------------------------------------------


def save_weather_artifact(artifact: WeatherModelArtifact, output_dir: Path) -> None:
    """Persist a WeatherModelArtifact to ``output_dir`` (created if absent)."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    torch.save(artifact.model.state_dict(), output_dir / 'model.pt')

    with open(output_dir / 'feature_spec.json', 'w') as f:
        json.dump(dataclasses.asdict(artifact.feature_spec), f, indent=2)

    with open(output_dir / 'training_config.json', 'w') as f:
        json.dump(dataclasses.asdict(artifact.training_config), f, indent=2)

    with open(output_dir / 'eval_report.json', 'w') as f:
        json.dump(dataclasses.asdict(artifact.eval_report), f, indent=2)


def load_weather_artifact(artifact_dir: Path) -> WeatherModelArtifact:
    """Load a WeatherModelArtifact saved by ``save_weather_artifact``."""
    artifact_dir = Path(artifact_dir)

    with open(artifact_dir / 'feature_spec.json') as f:
        spec_dict = json.load(f)
    feature_spec = WeatherFeatureSpec(
        feature_columns=tuple(spec_dict['feature_columns']),
        target_columns=tuple(spec_dict['target_columns']),
        feature_scaler_mean=tuple(spec_dict['feature_scaler_mean']),
        feature_scaler_scale=tuple(spec_dict['feature_scaler_scale']),
        target_scaler_mean=tuple(spec_dict['target_scaler_mean']),
        target_scaler_scale=tuple(spec_dict['target_scaler_scale']),
    )

    with open(artifact_dir / 'training_config.json') as f:
        config_dict = json.load(f)
    training_config = WeatherTrainingConfig(**config_dict)

    with open(artifact_dir / 'eval_report.json') as f:
        report_dict = json.load(f)
    eval_report = _weather_eval_report_from_dict(report_dict)

    feature_scaler = _scaler_from_params(
        feature_spec.feature_scaler_mean,
        feature_spec.feature_scaler_scale,
        n_features=len(feature_spec.feature_columns),
    )
    target_scaler = _scaler_from_params(
        feature_spec.target_scaler_mean,
        feature_spec.target_scaler_scale,
        n_features=len(feature_spec.target_columns),
    )

    model = WeatherNet(feature_spec.input_size)
    state_dict = torch.load(artifact_dir / 'model.pt', weights_only=True)
    model.load_state_dict(state_dict)
    model.eval()

    return WeatherModelArtifact(
        model=model,
        feature_scaler=feature_scaler,
        target_scaler=target_scaler,
        feature_spec=feature_spec,
        training_config=training_config,
        eval_report=eval_report,
        cutoff=pd.Timestamp(eval_report.cutoff),
    )


def _scaler_from_params(
    mean: tuple[float, ...],
    scale: tuple[float, ...],
    n_features: int,
) -> StandardScaler:
    scaler = StandardScaler()
    scaler.mean_ = np.array(mean, dtype=np.float64)
    scaler.scale_ = np.array(scale, dtype=np.float64)
    scaler.var_ = scaler.scale_**2
    scaler.n_features_in_ = n_features
    scaler.n_samples_seen_ = 1
    return scaler


def _weather_eval_report_from_dict(d: dict) -> WeatherEvalReport:  # type: ignore[type-arg]
    return WeatherEvalReport(
        temporal_test=tuple(WeatherTargetMetrics(**m) for m in d['temporal_test']),
        block_clim_model=tuple(
            WeatherTargetMetrics(**m) for m in d['block_clim_model']
        ),
        block_clim_idw=tuple(WeatherTargetMetrics(**m) for m in d['block_clim_idw']),
        cutoff=d['cutoff'],
    )
