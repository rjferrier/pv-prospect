"""Save and load ModelArtifact to/from a directory.

Artifact layout::

    <output_dir>/
        model.pt            # torch state dict
        feature_spec.json   # FeatureSpec (includes scaler parameters)
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
)
from pv_prospect.model.nets import CapacityFactorNet


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

    scaler = _scaler_from_feature_spec(feature_spec)

    model = CapacityFactorNet(feature_spec.input_size)
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


def _scaler_from_feature_spec(feature_spec: FeatureSpec) -> StandardScaler:
    scaler = StandardScaler()
    scaler.mean_ = np.array(feature_spec.scaler_mean, dtype=np.float64)
    scaler.scale_ = np.array(feature_spec.scaler_scale, dtype=np.float64)
    scaler.var_ = scaler.scale_**2
    scaler.n_features_in_ = len(feature_spec.continuous_features)
    scaler.n_samples_seen_ = 1
    return scaler


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
