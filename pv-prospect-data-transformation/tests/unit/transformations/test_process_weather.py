"""Tests for process_weather."""

import pandas as pd
import pytest
from pv_prospect.data_transformation.transformations import process_weather


@pytest.fixture
def cleaned_weather_df():
    """Cleaned weather DataFrame (as output by clean_weather)."""
    return pd.DataFrame(
        {
            'time': pd.to_datetime(
                [
                    '2026-01-15 01:00:00',
                    '2026-01-15 02:00:00',
                    '2026-01-15 03:00:00',
                ]
            ),
            'temperature': [5.0, 6.0, 7.0],
            'direct_normal_irradiance': [100.0, 200.0, 300.0],
            'diffuse_radiation': [50.0, 60.0, 70.0],
            'cloud_cover': [80.0, 60.0, 40.0],
            'wind_speed_80m': [10.0, 12.0, 14.0],
        }
    )


def test_selects_default_columns(cleaned_weather_df):
    """Should retain only time + default keep columns."""
    result = process_weather(cleaned_weather_df, timescale_days=None)

    assert list(result.columns) == [
        'time',
        'temperature',
        'direct_normal_irradiance',
        'diffuse_radiation',
    ]


def test_drops_non_selected_columns(cleaned_weather_df):
    """Columns not in keep_columns should be dropped."""
    result = process_weather(cleaned_weather_df, timescale_days=None)

    assert 'cloud_cover' not in result.columns
    assert 'wind_speed_80m' not in result.columns


def test_custom_keep_columns(cleaned_weather_df):
    """Should retain only the columns specified in keep_columns."""
    result = process_weather(
        cleaned_weather_df,
        keep_columns=('temperature', 'cloud_cover'),
        timescale_days=None,
    )

    assert list(result.columns) == ['time', 'temperature', 'cloud_cover']


def test_ignores_missing_keep_columns(cleaned_weather_df):
    """Columns in keep_columns that don't exist in the data should be silently skipped."""
    result = process_weather(
        cleaned_weather_df,
        keep_columns=('temperature', 'nonexistent_column'),
        timescale_days=None,
    )

    assert list(result.columns) == ['time', 'temperature']


def test_preserves_values(cleaned_weather_df):
    """Column values should be unchanged when no downsampling is applied."""
    result = process_weather(cleaned_weather_df, timescale_days=None)

    assert list(result['temperature']) == [5.0, 6.0, 7.0]
    assert list(result['direct_normal_irradiance']) == [100.0, 200.0, 300.0]


def test_no_downsampling_preserves_row_count(cleaned_weather_df):
    """With timescale_days=None, row count should be unchanged."""
    result = process_weather(cleaned_weather_df, timescale_days=None)

    assert len(result) == 3


def test_always_includes_time_column(cleaned_weather_df):
    """Result should always contain 'time' as the first column."""
    result = process_weather(
        cleaned_weather_df,
        keep_columns=('temperature',),
        timescale_days=None,
    )

    assert result.columns[0] == 'time'
