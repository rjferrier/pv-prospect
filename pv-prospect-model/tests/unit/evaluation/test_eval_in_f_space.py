"""Tests for eval_in_f_space."""

import numpy as np
import pytest
from pv_prospect.model.evaluation import eval_in_f_space


def test_perfect_predictions_give_r2_of_one() -> None:
    """When predictions equal targets, R² = 1 and error metrics are zero."""
    y = np.array([0.1, 0.2, 0.15, 0.3])
    metrics = eval_in_f_space(y, y)
    assert metrics.r2 == pytest.approx(1.0)
    assert metrics.rmse == pytest.approx(0.0, abs=1e-12)
    assert metrics.mae == pytest.approx(0.0, abs=1e-12)
    assert metrics.mse == pytest.approx(0.0, abs=1e-12)


def test_metrics_are_consistent() -> None:
    """rmse == sqrt(mse) and rmse >= mae for any inputs."""
    y_true = np.array([0.1, 0.2, 0.3, 0.4])
    y_pred = np.array([0.12, 0.18, 0.35, 0.38])
    metrics = eval_in_f_space(y_true, y_pred)
    assert metrics.rmse == pytest.approx(np.sqrt(metrics.mse))
    assert metrics.rmse >= metrics.mae


def test_constant_prediction_gives_r2_of_zero_or_less() -> None:
    """Predicting the mean always gives R² = 0; constant != mean gives R² < 0."""
    y_true = np.array([0.1, 0.2, 0.3])
    mean_pred = np.full_like(y_true, y_true.mean())
    metrics = eval_in_f_space(y_true, mean_pred)
    assert metrics.r2 == pytest.approx(0.0, abs=1e-10)
