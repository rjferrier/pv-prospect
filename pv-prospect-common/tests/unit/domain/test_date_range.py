"""Tests for DateRange domain model."""

from datetime import date

from pv_prospect.common.domain.date_range import DateRange, Period


def test_str_formats_as_date_range():
    dr = DateRange(date(2025, 1, 1), date(2025, 1, 8))
    assert str(dr) == '2025-01-01 to 2025-01-08'


def test_of_single_day_creates_range_with_next_day_as_end():
    dr = DateRange.of_single_day(date(2025, 3, 15))
    assert dr.start == date(2025, 3, 15)
    assert dr.end == date(2025, 3, 16)


def test_of_single_day_end_of_month_rolls_over():
    dr = DateRange.of_single_day(date(2025, 1, 31))
    assert dr.end == date(2025, 2, 1)


def test_split_by_day_single_day_range():
    dr = DateRange.of_single_day(date(2025, 6, 10))
    result = dr.split_by(Period.DAY)
    assert len(result) == 1
    assert result[0] == dr


def test_split_by_day_multi_day_range():
    dr = DateRange(date(2025, 1, 1), date(2025, 1, 4))
    result = dr.split_by(Period.DAY)
    assert len(result) == 3
    assert result[0] == DateRange(date(2025, 1, 1), date(2025, 1, 2))
    assert result[1] == DateRange(date(2025, 1, 2), date(2025, 1, 3))
    assert result[2] == DateRange(date(2025, 1, 3), date(2025, 1, 4))


def test_split_by_day_zero_day_range_returns_empty():
    dr = DateRange(date(2025, 1, 5), date(2025, 1, 5))
    assert dr.split_by(Period.DAY) == []


def test_split_by_week_exact_monday_to_sunday():
    # 2025-01-06 is a Monday, 2025-01-13 is the next Monday (exclusive end)
    dr = DateRange(date(2025, 1, 6), date(2025, 1, 13))
    result = dr.split_by(Period.WEEK)
    assert len(result) == 1
    assert result[0] == DateRange(date(2025, 1, 6), date(2025, 1, 13))


def test_split_by_week_two_full_weeks():
    dr = DateRange(date(2025, 1, 6), date(2025, 1, 20))
    result = dr.split_by(Period.WEEK)
    assert len(result) == 2
    assert result[0] == DateRange(date(2025, 1, 6), date(2025, 1, 13))
    assert result[1] == DateRange(date(2025, 1, 13), date(2025, 1, 20))


def test_split_by_week_partial_leading_days_excluded():
    # Start on Wednesday, only the following full week is included
    dr = DateRange(date(2025, 1, 8), date(2025, 1, 20))
    result = dr.split_by(Period.WEEK)
    assert len(result) == 1
    assert result[0].start == date(2025, 1, 13)


def test_split_by_week_partial_trailing_days_excluded():
    # End on Wednesday — the partial trailing week is excluded
    dr = DateRange(date(2025, 1, 6), date(2025, 1, 15))
    result = dr.split_by(Period.WEEK)
    assert len(result) == 1
    assert result[0] == DateRange(date(2025, 1, 6), date(2025, 1, 13))


def test_split_by_week_less_than_a_week_returns_empty():
    dr = DateRange(date(2025, 1, 7), date(2025, 1, 12))
    assert dr.split_by(Period.WEEK) == []


def test_split_by_week_within_single_week_not_starting_monday():
    dr = DateRange(date(2025, 1, 8), date(2025, 1, 11))
    assert dr.split_by(Period.WEEK) == []
