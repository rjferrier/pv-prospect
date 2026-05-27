"""Tests for initial_weather_grid_backfill_cursor."""

from datetime import date, timedelta

from pv_prospect.etl import (
    BackfillScope,
    default_window_days,
    initial_weather_grid_backfill_cursor,
)


def test_initial_cursor_ends_one_window_before_today() -> None:
    today = date(2026, 4, 3)
    window_days = default_window_days(BackfillScope.WEATHER_GRID)

    cursor = initial_weather_grid_backfill_cursor(today)

    assert cursor.next_end_date == today - timedelta(days=window_days)


def test_initial_cursor_sample_offset_is_one() -> None:
    cursor = initial_weather_grid_backfill_cursor(date(2026, 4, 3))

    assert cursor.next_sample_offset == 1
