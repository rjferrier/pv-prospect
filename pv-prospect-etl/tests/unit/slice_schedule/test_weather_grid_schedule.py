"""Tests for weather_grid_schedule."""

from datetime import date, timedelta

from pv_prospect.etl import (
    BackfillScope,
    WeatherGridBackfillCursor,
    default_window_days,
    initial_weather_grid_backfill_cursor,
    weather_grid_schedule,
)
from pv_prospect.etl.slice_schedule import TARGET_DENSITY_PASSES

_TODAY = date(2026, 4, 3)
_NUM_SAMPLE_FILES = 32
_EPOCH = date(1970, 1, 1)
_WINDOW_DAYS = default_window_days(BackfillScope.WEATHER_GRID)

# The bottom of the fixed window grid: the first window boundary above the
# 2016-01-01 archive floor. Restated here so the tests pin the grid's phase
# independently of the module under test.
_MIN_ARCHIVE_DATE = date(2016, 1, 1)
_BOTTOM_WINDOW_END = date(2016, 1, 8)


def _epoch_days(d: date) -> int:
    return (d - _EPOCH).days


def _window_end(index: int) -> date:
    """Exclusive end of the grid window at *index*, counting up from 0."""
    return _BOTTOM_WINDOW_END + timedelta(days=index * _WINDOW_DAYS)


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


def test_step3_windows_never_overlap_the_step2_window() -> None:
    # Step 3's sample file is no longer chosen to dodge Step 2's, so what
    # keeps a grid point from being drawn twice over overlapping dates is
    # that the historical march stays strictly below the trailing window.
    for offset in range(_WINDOW_DAYS):
        today = _TODAY + timedelta(days=offset)
        cursor = initial_weather_grid_backfill_cursor(today)
        slices, _ = weather_grid_schedule(today, _NUM_SAMPLE_FILES, cursor)

        step2_slice = slices[0]
        for step3_slice in slices[1:]:
            assert step3_slice.end_date <= step2_slice.start_date


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
        next_end_date=_window_end(20),
        density_pass=0,
    )
    slices, _ = weather_grid_schedule(_TODAY, num_files, cursor, step3_batch_count=6)

    step3_indices = [s.grid_point_sample_index for s in slices[1:]]
    assert all(0 <= i < num_files for i in step3_indices)


# ---------------------------------------------------------------------------
# The archive floor
# ---------------------------------------------------------------------------


def test_bottom_window_is_truncated_to_the_archive_floor() -> None:
    cursor = WeatherGridBackfillCursor(next_end_date=_window_end(0), density_pass=0)
    slices, _ = weather_grid_schedule(
        _TODAY, _NUM_SAMPLE_FILES, cursor, step3_batch_count=1
    )

    assert slices[1].start_date == _MIN_ARCHIVE_DATE
    assert slices[1].end_date == _BOTTOM_WINDOW_END


def test_march_never_plans_a_window_below_the_archive_floor() -> None:
    cursor = WeatherGridBackfillCursor(next_end_date=_window_end(2), density_pass=0)
    slices, _ = weather_grid_schedule(
        _TODAY, _NUM_SAMPLE_FILES, cursor, step3_batch_count=8
    )

    for weather_slice in slices[1:]:
        assert weather_slice.start_date >= _MIN_ARCHIVE_DATE
        assert weather_slice.start_date < weather_slice.end_date


def test_reaching_the_floor_rolls_to_the_next_density_pass() -> None:
    cursor = WeatherGridBackfillCursor(next_end_date=_window_end(0), density_pass=0)
    slices, updated_cursor = weather_grid_schedule(
        _TODAY, _NUM_SAMPLE_FILES, cursor, step3_batch_count=2
    )

    assert slices[1].end_date == _BOTTOM_WINDOW_END
    # The next pass restarts at the top of the grid.
    assert slices[2].end_date == _TODAY - timedelta(days=_WINDOW_DAYS)
    assert updated_cursor.density_pass == 1


def test_cursor_left_below_the_floor_rolls_without_planning_backwards() -> None:
    # The shape a pre-densifier cursor arrives in: run off the bottom of the
    # archive, having already swept the grid once.
    cursor = WeatherGridBackfillCursor(next_end_date=date(2010, 5, 21), density_pass=0)
    slices, updated_cursor = weather_grid_schedule(
        _TODAY, _NUM_SAMPLE_FILES, cursor, step3_batch_count=3
    )

    assert updated_cursor.density_pass == 1
    assert slices[1].end_date == _TODAY - timedelta(days=_WINDOW_DAYS)
    for weather_slice in slices[1:]:
        assert weather_slice.start_date >= _MIN_ARCHIVE_DATE


# ---------------------------------------------------------------------------
# Spatial densification
# ---------------------------------------------------------------------------


def test_each_density_pass_assigns_a_different_sample_file_to_a_window() -> None:
    window_end = _window_end(20)

    indices = set()
    for density_pass in range(TARGET_DENSITY_PASSES):
        cursor = WeatherGridBackfillCursor(
            next_end_date=window_end, density_pass=density_pass
        )
        slices, _ = weather_grid_schedule(
            _TODAY, _NUM_SAMPLE_FILES, cursor, step3_batch_count=1
        )
        indices.add(slices[1].grid_point_sample_index)

    assert len(indices) == TARGET_DENSITY_PASSES


def test_step3_sample_file_does_not_depend_on_today() -> None:
    cursor = WeatherGridBackfillCursor(next_end_date=_window_end(20), density_pass=1)

    slices_a, _ = weather_grid_schedule(_TODAY, _NUM_SAMPLE_FILES, cursor)
    slices_b, _ = weather_grid_schedule(
        _TODAY + timedelta(days=_WINDOW_DAYS), _NUM_SAMPLE_FILES, cursor
    )

    assert [s.grid_point_sample_index for s in slices_a[1:]] == [
        s.grid_point_sample_index for s in slices_b[1:]
    ]


def test_march_goes_quiescent_at_the_target_density() -> None:
    cursor = WeatherGridBackfillCursor(
        next_end_date=_window_end(0), density_pass=TARGET_DENSITY_PASSES - 1
    )
    slices, updated_cursor = weather_grid_schedule(
        _TODAY, _NUM_SAMPLE_FILES, cursor, step3_batch_count=8
    )

    # The last window of the last pass, and then nothing.
    assert len(slices) == 1 + 1
    assert updated_cursor.density_pass == TARGET_DENSITY_PASSES


def test_quiescent_cursor_emits_no_step3_slices_and_does_not_move() -> None:
    cursor = WeatherGridBackfillCursor(
        next_end_date=_MIN_ARCHIVE_DATE, density_pass=TARGET_DENSITY_PASSES
    )
    slices, updated_cursor = weather_grid_schedule(_TODAY, _NUM_SAMPLE_FILES, cursor)

    assert len(slices) == 1
    assert updated_cursor == cursor


def test_step2_keeps_sampling_after_the_march_goes_quiescent() -> None:
    cursor = WeatherGridBackfillCursor(
        next_end_date=_MIN_ARCHIVE_DATE, density_pass=TARGET_DENSITY_PASSES
    )
    slices, _ = weather_grid_schedule(_TODAY, _NUM_SAMPLE_FILES, cursor)

    assert slices[0].end_date == _TODAY
    assert slices[0].start_date == _TODAY - timedelta(days=_WINDOW_DAYS)
