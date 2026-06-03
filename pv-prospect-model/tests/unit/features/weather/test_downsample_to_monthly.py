"""Tests for downsample_to_monthly."""

import pandas as pd
import pytest
from pv_prospect.model.features.weather import downsample_to_monthly


def _make_df(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df['time'] = pd.to_datetime(df['time'])
    return df


def test_daily_rows_collapse_to_one_monthly_row_per_grid_point() -> None:
    df = _make_df([
        {'latitude': 51.5, 'longitude': -1.0, 'time': '2023-01-05',
         'elevation': 100.0, 'temperature': 4.0,
         'direct_normal_irradiance': 50.0, 'diffuse_radiation': 20.0},
        {'latitude': 51.5, 'longitude': -1.0, 'time': '2023-01-15',
         'elevation': 100.0, 'temperature': 6.0,
         'direct_normal_irradiance': 70.0, 'diffuse_radiation': 30.0},
    ])
    result = downsample_to_monthly(df)
    assert len(result) == 1
    assert result.loc[0, 'temperature'] == pytest.approx(5.0)
    assert result.loc[0, 'direct_normal_irradiance'] == pytest.approx(60.0)


def test_two_grid_points_produce_separate_monthly_rows() -> None:
    df = _make_df([
        {'latitude': 51.5, 'longitude': -1.0, 'time': '2023-01-10',
         'elevation': 100.0, 'temperature': 4.0,
         'direct_normal_irradiance': 50.0, 'diffuse_radiation': 20.0},
        {'latitude': 53.0, 'longitude': -2.0, 'time': '2023-01-10',
         'elevation': 200.0, 'temperature': 2.0,
         'direct_normal_irradiance': 40.0, 'diffuse_radiation': 15.0},
    ])
    result = downsample_to_monthly(df)
    assert len(result) == 2


def test_duplicate_rows_are_deduplicated_before_averaging() -> None:
    """Overlapping extract windows produce duplicate (grid, day) rows; they
    must not double-weight a day in the monthly mean."""
    df = _make_df([
        {'latitude': 51.5, 'longitude': -1.0, 'time': '2023-01-10',
         'elevation': 100.0, 'temperature': 4.0,
         'direct_normal_irradiance': 50.0, 'diffuse_radiation': 20.0},
        {'latitude': 51.5, 'longitude': -1.0, 'time': '2023-01-10',
         'elevation': 100.0, 'temperature': 4.0,
         'direct_normal_irradiance': 50.0, 'diffuse_radiation': 20.0},
        {'latitude': 51.5, 'longitude': -1.0, 'time': '2023-01-20',
         'elevation': 100.0, 'temperature': 8.0,
         'direct_normal_irradiance': 90.0, 'diffuse_radiation': 40.0},
    ])
    result = downsample_to_monthly(df)
    assert len(result) == 1
    # mean of [4, 8] not [4, 4, 8]
    assert result.loc[0, 'temperature'] == pytest.approx(6.0)


def test_representative_time_is_month_midpoint() -> None:
    """time in the output should be the 15th of the month (day 14 after start)."""
    df = _make_df([
        {'latitude': 51.5, 'longitude': -1.0, 'time': '2023-03-05',
         'elevation': 100.0, 'temperature': 8.0,
         'direct_normal_irradiance': 60.0, 'diffuse_radiation': 25.0},
    ])
    result = downsample_to_monthly(df)
    assert result.loc[0, 'time'] == pd.Timestamp('2023-03-15')


def test_elevation_is_carried_through() -> None:
    df = _make_df([
        {'latitude': 51.5, 'longitude': -1.0, 'time': '2023-06-10',
         'elevation': 350.0, 'temperature': 15.0,
         'direct_normal_irradiance': 200.0, 'diffuse_radiation': 80.0},
    ])
    result = downsample_to_monthly(df)
    assert result.loc[0, 'elevation'] == pytest.approx(350.0)
