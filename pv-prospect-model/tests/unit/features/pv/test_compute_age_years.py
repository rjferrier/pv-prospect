"""Tests for compute_age_years."""

import pandas as pd
import pytest
from pv_prospect.model.features.pv import compute_age_years


def test_known_installation_date_produces_correct_age() -> None:
    """Age in years is computed as elapsed days / 365.25."""
    times = pd.Series(pd.to_datetime(['2020-01-01']))
    install_dates = pd.Series(pd.to_datetime(['2015-01-01']))
    age_years, age_known = compute_age_years(times, install_dates)
    expected_days = (pd.Timestamp('2020-01-01') - pd.Timestamp('2015-01-01')).days
    assert age_years.iloc[0] == pytest.approx(expected_days / 365.25)
    assert age_known.iloc[0] == 1


def test_unknown_installation_date_uses_median_of_known_rows() -> None:
    """Sites with missing install_date get median age across known sites."""
    times = pd.Series(pd.to_datetime(['2020-01-01'] * 3))
    install_dates = pd.Series(pd.to_datetime(['2015-01-01', '2010-01-01', pd.NaT]))
    age_years, age_known = compute_age_years(times, install_dates)

    known_ages = age_years[age_known == 1]
    median_age = float(known_ages.median())

    assert age_known.iloc[2] == 0
    assert age_years.iloc[2] == pytest.approx(median_age)


def test_unknown_installation_date_sets_age_known_to_zero() -> None:
    """age_known is 0 wherever installation_date is missing."""
    times = pd.Series(pd.to_datetime(['2020-01-01']))
    install_dates = pd.Series([pd.NaT])
    _, age_known = compute_age_years(times, install_dates)
    assert age_known.iloc[0] == 0


def test_all_unknown_dates_fallback_to_zero() -> None:
    """If all installation dates are missing, imputed age_years is 0.0."""
    times = pd.Series(pd.to_datetime(['2020-01-01', '2021-01-01']))
    install_dates = pd.Series([pd.NaT, pd.NaT])
    age_years, age_known = compute_age_years(times, install_dates)
    assert list(age_years) == [0.0, 0.0]
    assert list(age_known) == [0, 0]


def test_explicit_fill_with_overrides_median() -> None:
    """Passing fill_with uses that value instead of computing the median."""
    times = pd.Series(pd.to_datetime(['2020-01-01']))
    install_dates = pd.Series([pd.NaT])
    age_years, _ = compute_age_years(times, install_dates, fill_with=7.5)
    assert age_years.iloc[0] == pytest.approx(7.5)
