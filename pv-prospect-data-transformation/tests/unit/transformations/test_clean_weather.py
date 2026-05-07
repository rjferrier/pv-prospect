"""Tests for clean_weather."""

import pandas as pd
import pytest
from pv_prospect.data_transformation.transformations import clean_weather


@pytest.fixture
def raw_weather_df():
    """Raw OpenMeteo weather DataFrame with model-suffixed columns."""
    return pd.DataFrame(
        {
            'time': pd.to_datetime(
                [
                    '2026-01-15 01:00:00',
                    '2026-01-15 02:00:00',
                    '2026-01-15 03:00:00',
                ]
            ),
            'temperature_best_match': [5.0, 6.0, 7.0],
            'direct_normal_irradiance_best_match': [100.0, 200.0, 300.0],
            'diffuse_radiation_best_match': [50.0, 60.0, 70.0],
            'pressure_msl_best_match': [1013.0, 1014.0, 1015.0],
            'wind_direction_80m_best_match': [180, 190, 200],
            'wind_direction_180m_best_match': [185, 195, 205],
            'cloud_cover_best_match': [80.0, 60.0, 40.0],
            'temperature_ukmo_seamless': [4.5, 5.5, 6.5],
            'direct_normal_irradiance_ukmo_seamless': [90.0, 190.0, 290.0],
        }
    )


def test_selects_columns_for_given_weather_model(raw_weather_df):
    """Should select only columns matching the weather model suffix."""
    result = clean_weather(raw_weather_df, weather_model='best_match')

    assert 'temperature' in result.columns
    assert 'direct_normal_irradiance' in result.columns
    assert 'diffuse_radiation' in result.columns
    assert 'cloud_cover' in result.columns
    assert 'time' in result.columns

    # ukmo_seamless columns should NOT appear
    assert 'temperature_ukmo_seamless' not in result.columns
    assert 'direct_normal_irradiance_ukmo_seamless' not in result.columns


def test_strips_model_suffix_from_column_names(raw_weather_df):
    """Column names should have the model suffix removed."""
    result = clean_weather(raw_weather_df, weather_model='best_match')

    # No column should still have the suffix
    for col in result.columns:
        assert not col.endswith('_best_match')


def test_drops_default_excluded_columns(raw_weather_df):
    """Should drop pressure_msl, wind_direction_80m, wind_direction_180m by default."""
    result = clean_weather(raw_weather_df, weather_model='best_match')

    assert 'pressure_msl' not in result.columns
    assert 'wind_direction_80m' not in result.columns
    assert 'wind_direction_180m' not in result.columns


def test_custom_excluded_columns(raw_weather_df):
    """Should drop only the specified excluded columns."""
    result = clean_weather(
        raw_weather_df,
        weather_model='best_match',
        excluded_columns={'cloud_cover'},
    )

    assert 'cloud_cover' not in result.columns
    # Defaults should NOT be excluded when custom set is provided
    assert 'pressure_msl' in result.columns


def test_preserves_values(raw_weather_df):
    """Column values should match the original suffixed columns."""
    result = clean_weather(raw_weather_df, weather_model='best_match')

    assert list(result['temperature']) == [5.0, 6.0, 7.0]
    assert list(result['direct_normal_irradiance']) == [100.0, 200.0, 300.0]


def test_selects_alternative_weather_model(raw_weather_df):
    """Should work with a non-default weather model."""
    result = clean_weather(raw_weather_df, weather_model='ukmo_seamless')

    assert 'temperature' in result.columns
    assert list(result['temperature']) == [4.5, 5.5, 6.5]

    # best_match columns should NOT appear
    assert 'cloud_cover' not in result.columns


def test_no_downsampling_by_default(raw_weather_df):
    """With timescale_days=None, row count should be unchanged."""
    result = clean_weather(
        raw_weather_df, weather_model='best_match', timescale_days=None
    )

    assert len(result) == 3


def test_always_includes_time_column(raw_weather_df):
    """Result should always contain 'time' as the first column."""
    result = clean_weather(raw_weather_df, weather_model='best_match')

    assert result.columns[0] == 'time'


def test_strips_altitude_suffix_from_openmeteo_column_names():
    """temperature_2m_best_match should produce a column named 'temperature'."""
    df = pd.DataFrame(
        {
            'time': pd.to_datetime(['2026-05-05 00:00:00']),
            'temperature_2m_best_match': [10.2],
            'wind_speed_180m_best_match': [20.6],
            'direct_normal_irradiance_best_match': [0.0],
            'diffuse_radiation_best_match': [0.0],
        }
    )
    result = clean_weather(df, weather_model='best_match')

    assert 'temperature' in result.columns
    assert 'temperature_2m' not in result.columns
    assert 'wind_speed' in result.columns
    assert 'wind_speed_180m' not in result.columns
