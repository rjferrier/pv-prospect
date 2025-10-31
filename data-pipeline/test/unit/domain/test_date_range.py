"""Module-style tests for DateRange domain logic."""

import pytest
from datetime import date

from domain.date_range import DateRange, Period


def test_of_single_day():
    d = date(2024, 1, 15)
    date_range = DateRange.of_single_day(d)

    assert date_range.start == date(2024, 1, 15)
    assert date_range.end == date(2024, 1, 16)


def test_split_by_day_single_day():
    date_range = DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))
    result = date_range.split_by(Period.DAY)
    # Should return a single-day DateRange with exclusive end
    assert result == [DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))]


def test_split_by_day_multiple_days():
    date_range = DateRange(start=date(2024, 1, 15), end=date(2024, 1, 18))
    result = date_range.split_by(Period.DAY)

    assert len(result) == 3
    assert result[0] == DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))
    assert result[1] == DateRange(start=date(2024, 1, 16), end=date(2024, 1, 17))
    assert result[2] == DateRange(start=date(2024, 1, 17), end=date(2024, 1, 18))


def test_split_by_week_no_complete_weeks():
    date_range = DateRange(start=date(2024, 1, 3), end=date(2024, 1, 5))
    result = date_range.split_by(Period.WEEK)
    assert result == []


def test_split_by_week_one_complete_week():
    date_range = DateRange(start=date(2024, 1, 1), end=date(2024, 1, 8))
    result = date_range.split_by(Period.WEEK)

    assert len(result) == 1
    assert result[0] == DateRange(start=date(2024, 1, 1), end=date(2024, 1, 8))


def test_split_by_week_multiple_weeks():
    date_range = DateRange(start=date(2024, 1, 1), end=date(2024, 1, 22))
    result = date_range.split_by(Period.WEEK)

    assert len(result) == 3
    assert result[0] == DateRange(start=date(2024, 1, 1), end=date(2024, 1, 8))
    assert result[1] == DateRange(start=date(2024, 1, 8), end=date(2024, 1, 15))
    assert result[2] == DateRange(start=date(2024, 1, 15), end=date(2024, 1, 22))


def test_split_by_week_starts_mid_week():
    date_range = DateRange(start=date(2024, 1, 3), end=date(2024, 1, 22))
    result = date_range.split_by(Period.WEEK)

    assert len(result) == 2
    assert result[0] == DateRange(start=date(2024, 1, 8), end=date(2024, 1, 15))
    assert result[1] == DateRange(start=date(2024, 1, 15), end=date(2024, 1, 22))


def test_split_by_week_ends_mid_week():
    date_range = DateRange(start=date(2024, 1, 1), end=date(2024, 1, 17))
    result = date_range.split_by(Period.WEEK)

    assert len(result) == 2
    assert result[0] == DateRange(start=date(2024, 1, 1), end=date(2024, 1, 8))
    assert result[1] == DateRange(start=date(2024, 1, 8), end=date(2024, 1, 15))


def test_frozen_dataclass():
    date_range = DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))
    with pytest.raises(Exception):
        date_range.start = date(2024, 1, 16)
