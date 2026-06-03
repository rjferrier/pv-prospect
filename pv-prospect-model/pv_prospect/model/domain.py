"""Domain types shared by the PV and weather model trainers."""

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


# ---------------------------------------------------------------------------
# Weather model types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WeatherTrainingConfig:
    """Hyperparameters and data-splitting settings for weather model training."""

    cutoff_quantile: float = 0.8
    val_fraction: float = 0.1
    patience: int = 10
    num_epochs: int = 100
    batch_size: int = 256
    learning_rate: float = 1e-3


@dataclass(frozen=True)
class WeatherFeatureSpec:
    """Feature/target schema and fitted scaler parameters for the weather model.

    Both the feature scaler and the target scaler are stored here so the
    artifact can be fully reconstructed from JSON without pickling sklearn
    objects. The target scaler is required because temperature (°C), DNI
    (W/m²), and DHI (W/m²) have very different natural scales; without
    target scaling the MSE loss is dominated by DNI.
    """

    feature_columns: tuple[str, ...]
    target_columns: tuple[str, ...]
    feature_scaler_mean: tuple[float, ...]
    feature_scaler_scale: tuple[float, ...]
    target_scaler_mean: tuple[float, ...]
    target_scaler_scale: tuple[float, ...]

    @property
    def input_size(self) -> int:
        return len(self.feature_columns)


@dataclass(frozen=True)
class WeatherTargetMetrics:
    """Regression metrics for a single weather target on a single split."""

    target: str
    r2: float
    rmse: float
    mae: float
    bias: float


@dataclass(frozen=True)
class WeatherEvalReport:
    """Evaluation metrics for the weather model.

    ``temporal_test`` — per-row metrics on the temporal hold-out test set.
    Training smoke-check only: temporal-holdout R² reflects interannual
    noise fitting, not spatial generalisation.

    ``block_clim_model`` — block-level climatology RMSE comparing model
    predictions to observed block climatologies, evaluated on the temporal
    test set.

    ``block_clim_idw`` — same blocks, same observed reference, but
    predictions come from IDW of training-set block climatologies. Provides
    the natural interpolation baseline.

    Both block metrics use the temporal hold-out test rows as the reference,
    so they measure climatology prediction quality on genuinely unseen data.
    The spatial-fold evaluation (with geographic hold-out) is performed
    separately in data-exploration and is the validation of record for
    spatial generalisation.
    """

    temporal_test: tuple[WeatherTargetMetrics, ...]
    block_clim_model: tuple[WeatherTargetMetrics, ...]
    block_clim_idw: tuple[WeatherTargetMetrics, ...]
    cutoff: str


@dataclass
class WeatherModelArtifact:
    """In-memory bundle produced by the weather training pipeline."""

    model: nn.Module
    feature_scaler: StandardScaler
    target_scaler: StandardScaler
    feature_spec: WeatherFeatureSpec
    training_config: WeatherTrainingConfig
    eval_report: WeatherEvalReport
    cutoff: pd.Timestamp
