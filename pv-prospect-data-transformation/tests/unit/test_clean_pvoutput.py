"""Tests for clean_pvoutput."""

import pandas as pd
import pytest
from pv_prospect.data_transformation.clean_pvoutput import clean_pvoutput


@pytest.fixture
def raw_pvoutput_df() -> 'pd.DataFrame':
    """Raw PVOutput DataFrame with date, time, power, and extra columns."""
    return pd.DataFrame(
        {
            'date': ['20260115', '20260115', '20260115', '20260115'],
            'time': ['12:00', '12:05', '12:10', '12:15'],
            'power': [1500.0, 1600.0, float('nan'), 1800.0],
            'energy': [100.0, 200.0, 300.0, 400.0],
        }
    )


def test_output_has_only_time_and_power(raw_pvoutput_df: pd.DataFrame) -> None:
    """Result should contain only 'time' and 'power' columns."""
    result = clean_pvoutput(raw_pvoutput_df)

    assert list(result.columns) == ['time', 'power']


def test_drops_nan_power_rows(raw_pvoutput_df: pd.DataFrame) -> None:
    """Rows where power is NaN should be dropped."""
    result = clean_pvoutput(raw_pvoutput_df)

    assert len(result) == 3
    assert not result['power'].isna().any()


def test_time_column_is_datetime(raw_pvoutput_df: pd.DataFrame) -> None:
    """The 'time' column should be of datetime type."""
    result = clean_pvoutput(raw_pvoutput_df)

    assert pd.api.types.is_datetime64_any_dtype(result['time'])


def test_winter_time_is_unchanged() -> None:
    """In winter (no DST), UK time equals UTC so values should be unchanged."""
    df = pd.DataFrame(
        {
            'date': ['20260115'],
            'time': ['12:00'],
            'power': [1500.0],
        }
    )

    result = clean_pvoutput(df)

    expected = pd.Timestamp('2026-01-15 12:00:00')
    assert result['time'].iloc[0] == expected


def test_summer_time_shifts_back_one_hour() -> None:
    """During BST (summer), UK local time should be shifted back by 1 hour to UTC."""
    df = pd.DataFrame(
        {
            'date': ['20260715'],
            'time': ['13:00'],
            'power': [2000.0],
        }
    )

    result = clean_pvoutput(df)

    # BST is UTC+1, so 13:00 BST = 12:00 UTC
    expected = pd.Timestamp('2026-07-15 12:00:00')
    assert result['time'].iloc[0] == expected


def test_does_not_mutate_input() -> None:
    """The original DataFrame should not be modified."""
    df = pd.DataFrame(
        {
            'date': ['20260115'],
            'time': ['12:00'],
            'power': [1500.0],
        }
    )
    original_columns = list(df.columns)

    clean_pvoutput(df)

    assert list(df.columns) == original_columns


def test_preserves_power_values(raw_pvoutput_df: pd.DataFrame) -> None:
    """Power values should be passed through unchanged."""
    result = clean_pvoutput(raw_pvoutput_df)

    assert list(result['power']) == [1500.0, 1600.0, 1800.0]
