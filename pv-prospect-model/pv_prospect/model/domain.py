"""Domain types for the PV model trainer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    import torch.nn as nn
    from sklearn.preprocessing import StandardScaler


@dataclass(frozen=True)
class FeatureSpec:
    """Feature schema and fitted scaler parameters.

    Carries enough information to reconstruct the StandardScaler without
    pickling the sklearn object, and to enforce the correct column order
    at inference time.
    """

    continuous_features: tuple[str, ...]
    binary_features: tuple[str, ...]
    target_column: str
    scaler_mean: tuple[float, ...]
    scaler_scale: tuple[float, ...]

    @property
    def all_features(self) -> tuple[str, ...]:
        return self.continuous_features + self.binary_features

    @property
    def input_size(self) -> int:
        return len(self.continuous_features) + len(self.binary_features)


@dataclass(frozen=True)
class TrainingConfig:
    """Hyperparameters and data-splitting settings for PV model training."""

    censoring_margin: float = 0.01
    cutoff_quantile: float = 0.8
    val_fraction: float = 0.1
    patience: int = 10
    num_epochs: int = 100
    batch_size: int = 32
    learning_rate: float = 1e-3


@dataclass(frozen=True)
class SplitMetrics:
    """Regression metrics for a single evaluation split."""

    r2: float
    rmse: float
    mae: float
    mse: float


@dataclass(frozen=True)
class PerSiteMetrics:
    """Per-site metrics for one evaluation surface."""

    system_id: int
    n: int
    r2: float
    rmse: float
    mae: float


@dataclass(frozen=True)
class EvalReport:
    """Metrics in both capacity-factor space and clamped-power space.

    Computed on train and test splits overall, plus per-site breakdowns
    on the test split only (the train split per-site breakdown is rarely
    diagnostically useful and would clutter the report).
    """

    train_f_space: SplitMetrics
    test_f_space: SplitMetrics
    train_power_space: SplitMetrics
    test_power_space: SplitMetrics
    test_per_site_f_space: tuple[PerSiteMetrics, ...]
    test_per_site_power_space: tuple[PerSiteMetrics, ...]
    cutoff: str


@dataclass
class ModelArtifact:
    """In-memory bundle produced by the training pipeline.

    ``model`` is the best-checkpoint PyTorch module (state dict loaded).
    ``scaler`` is the fitted StandardScaler.
    ``feature_spec`` carries the scaler parameters redundantly so the
    artifact can be reconstructed without pickling sklearn objects.
    """

    model: nn.Module
    scaler: StandardScaler
    feature_spec: FeatureSpec
    training_config: TrainingConfig
    eval_report: EvalReport
    cutoff: pd.Timestamp
