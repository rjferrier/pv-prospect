"""Tests for weather_grid_schedule."""

from datetime import date, timedelta

from pv_prospect.etl import (
    BackfillScope,
    WeatherGridBackfillCursor,
    default_window_days,
    initial_weather_grid_backfill_cursor,
    weather_grid_schedule,
)

_TODAY = date(2026, 4, 3)
_NUM_SAMPLE_FILES = 32
_EPOCH = date(1970, 1, 1)
_WINDOW_DAYS = default_window_days(BackfillScope.WEATHER_GRID)


def _epoch_days(d: date) -> int:
    return (d - _EPOCH).days


def test_first_slice_uses_todays_modulus() -> None:
    cursor = initial_weather_grid_backfill_cursor(_TODAY)
    slices, _ = weather_grid_schedule(_TODAY, _NUM_SAMPLE_FILES, cursor)

    expected_index = _epoch_days(_TODAY) % _NUM_SAMPLE_FILES
    assert slices[0].grid_point_sample_index == expected_index


def test_first_slice_covers_trailing_window() -> None:
    cursor = initial_weather_grid_backfill_cursor(_TODAY)
    slices, _ = weather_grid_schedule(_TODAY, _NUM_SAMPLE_FILES, cursor)

    assert slices[0].end_date == _TODAY
    assert slices[0].start_date == _TODAY - timedelta(days=_WINDOW_DAYS)


def test_total_slice_count_is_step2_plus_step3() -> None:
    cursor = initial_weather_grid_backfill_cursor(_TODAY)
    slices, _ = weather_grid_schedule(
        _TODAY, _NUM_SAMPLE_FILES, cursor, step3_batch_count=5
    )

    assert len(slices) == 1 + 5


def test_default_step3_batch_count_is_8() -> None:
    cursor = initial_weather_grid_backfill_cursor(_TODAY)
    slices, _ = weather_grid_schedule(_TODAY, _NUM_SAMPLE_FILES, cursor)

    assert len(slices) == 1 + 8


def test_step3_slices_march_backwards_in_window_lengths() -> None:
    cursor = initial_weather_grid_backfill_cursor(_TODAY)
    slices, _ = weather_grid_schedule(_TODAY, _NUM_SAMPLE_FILES, cursor)

    step3_slices = slices[1:]
    for i, weather_slice in enumerate(step3_slices):
        assert weather_slice.end_date - weather_slice.start_date == timedelta(
            days=_WINDOW_DAYS
        )
        if i > 0:
            assert weather_slice.end_date == step3_slices[i - 1].start_date


def test_first_step3_slice_starts_where_cursor_points() -> None:
    cursor = initial_weather_grid_backfill_cursor(_TODAY)
    slices, _ = weather_grid_schedule(_TODAY, _NUM_SAMPLE_FILES, cursor)

    assert slices[1].end_date == cursor.next_end_date


def test_step3_slices_use_distinct_sample_files() -> None:
    cursor = initial_weather_grid_backfill_cursor(_TODAY)
    slices, _ = weather_grid_schedule(_TODAY, _NUM_SAMPLE_FILES, cursor)

    indices = [s.grid_point_sample_index for s in slices[1:]]
    assert len(set(indices)) == len(indices)


def test_step3_sample_indices_differ_from_step2() -> None:
    cursor = initial_weather_grid_backfill_cursor(_TODAY)
    slices, _ = weather_grid_schedule(_TODAY, _NUM_SAMPLE_FILES, cursor)

    step2_index = slices[0].grid_point_sample_index
    step3_indices = {s.grid_point_sample_index for s in slices[1:]}
    assert step2_index not in step3_indices


def test_updated_cursor_continues_from_last_slice() -> None:
    cursor = initial_weather_grid_backfill_cursor(_TODAY)
    slices, updated_cursor = weather_grid_schedule(_TODAY, _NUM_SAMPLE_FILES, cursor)

    assert updated_cursor.next_end_date == slices[-1].start_date


def test_consecutive_days_produce_contiguous_step3_slices() -> None:
    day1 = _TODAY
    cursor = initial_weather_grid_backfill_cursor(day1)
    slices1, cursor_after_day1 = weather_grid_schedule(day1, _NUM_SAMPLE_FILES, cursor)

    day2 = day1 + timedelta(days=1)
    slices2, _ = weather_grid_schedule(day2, _NUM_SAMPLE_FILES, cursor_after_day1)

    # Day 2's first Step 3 slice should start where day 1 left off.
    assert slices2[1].end_date == slices1[-1].start_date


def test_sample_file_indices_wrap_around() -> None:
    num_files = 4
    cursor = WeatherGridBackfillCursor(
        next_end_date=_TODAY - timedelta(days=_WINDOW_DAYS),
        next_sample_offset=1,
    )
    slices, _ = weather_grid_schedule(_TODAY, num_files, cursor, step3_batch_count=6)

    step3_indices = [s.grid_point_sample_index for s in slices[1:]]
    assert all(0 <= i < num_files for i in step3_indices)
