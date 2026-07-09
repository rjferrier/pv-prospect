"""Tests for initial_weather_grid_backfill_cursor."""

from datetime import date, timedelta

from pv_prospect.etl import (
    BackfillScope,
    default_window_days,
    initial_weather_grid_backfill_cursor,
)

_WINDOW_DAYS = default_window_days(BackfillScope.WEATHER_GRID)


def test_initial_cursor_ends_one_window_before_today() -> None:
    # 2026-04-03 sits on the window grid, so no snapping is needed.
    today = date(2026, 4, 3)

    cursor = initial_weather_grid_backfill_cursor(today)

    assert cursor.next_end_date == today - timedelta(days=_WINDOW_DAYS)


def test_initial_cursor_snaps_onto_the_window_grid() -> None:
    on_grid = date(2026, 4, 3)

    for offset in range(1, _WINDOW_DAYS):
        cursor = initial_weather_grid_backfill_cursor(on_grid + timedelta(days=offset))

        assert cursor.next_end_date == on_grid - timedelta(days=_WINDOW_DAYS)


def test_initial_cursor_leaves_the_trailing_window_to_step_2() -> None:
    for offset in range(_WINDOW_DAYS):
        today = date(2026, 4, 3) + timedelta(days=offset)

        cursor = initial_weather_grid_backfill_cursor(today)

        assert cursor.next_end_date <= today - timedelta(days=_WINDOW_DAYS)


def test_initial_cursor_starts_at_the_first_density_pass() -> None:
    cursor = initial_weather_grid_backfill_cursor(date(2026, 4, 3))

    assert cursor.density_pass == 0
