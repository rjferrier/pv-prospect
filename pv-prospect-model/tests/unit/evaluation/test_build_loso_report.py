"""Tests for build_loso_report."""

import numpy as np
import pytest
from pv_prospect.model.domain import LosoSiteMetrics
from pv_prospect.model.evaluation import build_loso_report


def _site(system_id: int, level_ratio: float) -> LosoSiteMetrics:
    return LosoSiteMetrics(
        system_id=system_id,
        n=100,
        power_r2=0.8,
        power_mape=0.2,
        level_ratio=level_ratio,
    )


def test_level_mean_and_band_match_normalised_sample_sd() -> None:
    """level_mean is the mean ratio; band is the ddof=1 SD of normalised ratios."""
    per_site = (_site(1, 0.8), _site(2, 1.0), _site(3, 1.2))
    report = build_loso_report(
        per_site,
        pooled_power_true=np.array([1.0, 2.0]),
        pooled_power_pred=np.array([1.0, 2.0]),
    )
    assert report.level_mean == pytest.approx(1.0)
    # normalised = [0.8, 1.0, 1.2]; sample SD (ddof=1) = sqrt(0.08 / 2) = 0.2
    assert report.level_band_1sigma == pytest.approx(0.2)


def test_band_is_scale_invariant_to_the_mean() -> None:
    """Doubling every level ratio leaves the normalised band unchanged."""
    base = build_loso_report(
        (_site(1, 0.8), _site(2, 1.0), _site(3, 1.2)),
        pooled_power_true=np.array([1.0, 2.0]),
        pooled_power_pred=np.array([1.0, 2.0]),
    )
    scaled = build_loso_report(
        (_site(1, 1.6), _site(2, 2.0), _site(3, 2.4)),
        pooled_power_true=np.array([1.0, 2.0]),
        pooled_power_pred=np.array([1.0, 2.0]),
    )
    assert scaled.level_band_1sigma == pytest.approx(base.level_band_1sigma)


def test_pooled_power_r2_uses_concatenated_arrays() -> None:
    report = build_loso_report(
        (_site(1, 1.0),),
        pooled_power_true=np.array([1.0, 2.0, 3.0, 4.0]),
        pooled_power_pred=np.array([1.0, 2.0, 3.0, 4.0]),
    )
    assert report.pooled_power_r2 == pytest.approx(1.0)


def test_single_site_band_is_zero() -> None:
    """A single fold has no spread; the band must not raise on ddof=1."""
    report = build_loso_report(
        (_site(1, 1.0),),
        pooled_power_true=np.array([1.0, 2.0]),
        pooled_power_pred=np.array([1.1, 1.9]),
    )
    assert report.level_band_1sigma == 0.0
