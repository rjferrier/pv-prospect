"""Tests for augment_features."""

import pandas as pd
import pytest
from pv_prospect.model.features.pv import TARGET_COLUMN, augment_features


def _make_df(
    time: str = '2023-06-01',
    power: float = 500.0,
    panels_capacity: float = 2000.0,
    installation_date: str | None = '2018-01-01',
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            'time': pd.to_datetime([time]),
            'power': [power],
            'panels_capacity': [panels_capacity],
            'installation_date': pd.to_datetime(
                [installation_date] if installation_date else [pd.NaT]
            ),
        }
    )


def test_capacity_factor_is_power_over_panels_capacity() -> None:
    """capacity_factor = power / panels_capacity."""
    df = _make_df(power=500.0, panels_capacity=2000.0)
    result = augment_features(df)
    assert result[TARGET_COLUMN].iloc[0] == pytest.approx(0.25)


def test_day_of_year_is_added() -> None:
    """day_of_year is derived from the time column."""
    df = _make_df(time='2023-06-01')
    result = augment_features(df)
    assert result['day_of_year'].iloc[0] == pd.Timestamp('2023-06-01').dayofyear


def test_age_years_and_age_known_are_added() -> None:
    """age_years and age_known columns are present after augmentation."""
    df = _make_df(time='2023-01-01', installation_date='2018-01-01')
    result = augment_features(df)
    assert 'age_years' in result.columns
    assert 'age_known' in result.columns
    assert result['age_known'].iloc[0] == 1


def test_missing_installation_date_yields_age_known_zero() -> None:
    """Sites with no installation_date get age_known = 0."""
    df = _make_df(installation_date=None)
    result = augment_features(df)
    assert result['age_known'].iloc[0] == 0


def test_input_columns_are_preserved() -> None:
    """augment_features does not drop existing columns from the input frame."""
    df = _make_df()
    result = augment_features(df)
    for col in ['time', 'power', 'panels_capacity']:
        assert col in result.columns
