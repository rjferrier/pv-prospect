"""Unit tests for the prospect uncertainty band builder."""

from pv_prospect.app.main import (
    PROSPECT_BAND_1SIGMA_FRAC,
    prospect_uncertainty_band,
)


def test_band_brackets_expected_by_the_default_sigma_fraction() -> None:
    band = prospect_uncertainty_band(1000.0)
    assert band.sigma_frac == PROSPECT_BAND_1SIGMA_FRAC
    assert band.annual_kwh_low == round(1000.0 * (1 - PROSPECT_BAND_1SIGMA_FRAC), 1)
    assert band.annual_kwh_high == round(1000.0 * (1 + PROSPECT_BAND_1SIGMA_FRAC), 1)
    assert band.annual_kwh_low < 1000.0 < band.annual_kwh_high


def test_band_is_symmetric_about_the_expected_estimate() -> None:
    band = prospect_uncertainty_band(1000.0)
    assert band.annual_kwh_high - 1000.0 == 1000.0 - band.annual_kwh_low


def test_sigma_fraction_is_overridable() -> None:
    band = prospect_uncertainty_band(1000.0, sigma_frac=0.34)
    assert band.sigma_frac == 0.34
    assert band.annual_kwh_low == 660.0
    assert band.annual_kwh_high == 1340.0
