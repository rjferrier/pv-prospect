"""Tests for downsample_by_days."""

import pandas as pd
import pytest
from pv_prospect.data_transformation.helpers import downsample_by_days


def _hourly_df(start: str, periods: int, value: float = 10.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            'time': pd.date_range(start, periods=periods, freq='h'),
            'value': [value] * periods,
        }
    )


def test_single_day_produces_one_row() -> None:
    df = _hourly_df('2026-05-07 00:00:00', periods=24)

    result = downsample_by_days(df, timescale_days=1)

    assert len(result) == 1


def test_single_day_is_labelled_by_its_own_date() -> None:
    """A daily aggregate of 2026-05-07's hourly data must be labelled
    2026-05-07, not 2026-05-08 (the right edge of the aggregation interval)."""
    df = _hourly_df('2026-05-07 00:00:00', periods=24)

    result = downsample_by_days(df, timescale_days=1)

    assert result['time'].iloc[0] == pd.Timestamp('2026-05-07 00:00:00')


def test_multi_day_rows_are_labelled_by_their_own_dates() -> None:
    df = _hourly_df('2026-05-06 00:00:00', periods=48)

    result = downsample_by_days(df, timescale_days=1)

    assert list(result['time']) == [
        pd.Timestamp('2026-05-06 00:00:00'),
        pd.Timestamp('2026-05-07 00:00:00'),
    ]


def test_constant_values_are_preserved() -> None:
    df = _hourly_df('2026-05-07 00:00:00', periods=24, value=42.0)

    result = downsample_by_days(df, timescale_days=1)

    assert result['value'].iloc[0] == pytest.approx(42.0)


def test_time_weighted_average_across_a_day() -> None:
    """Each value contributes equally over hourly data, so the daily mean
    equals the arithmetic mean of the values inside the period."""
    df = pd.DataFrame(
        {
            'time': pd.date_range('2026-05-07 00:00:00', periods=24, freq='h'),
            # First value (at 00:00) sits on the boundary and is excluded by
            # reduce_rows; the remaining 23 (1.0 .. 23.0) are averaged.
            'value': [float(i) for i in range(24)],
        }
    )

    result = downsample_by_days(df, timescale_days=1)

    assert result['value'].iloc[0] == pytest.approx(sum(range(1, 24)) / 23)


def test_empty_input_returns_empty_output() -> None:
    df = pd.DataFrame({'time': pd.to_datetime([]), 'value': []})

    result = downsample_by_days(df, timescale_days=1)

    assert result.empty


def test_multi_day_period_labels_by_period_start() -> None:
    """A 7-day aggregate of 14 days of hourly data is labelled by the
    start of each 7-day period."""
    df = _hourly_df('2026-05-04 00:00:00', periods=24 * 14)

    result = downsample_by_days(df, timescale_days=7)

    assert list(result['time']) == [
        pd.Timestamp('2026-05-04 00:00:00'),
        pd.Timestamp('2026-05-11 00:00:00'),
    ]
