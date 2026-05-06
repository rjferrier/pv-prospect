"""Tests for to_inclusive_datetime_bounds."""

from datetime import date, datetime

from pv_prospect.data_extraction.extractors import to_inclusive_datetime_bounds
from pv_prospect.data_extraction.extractors.openmeteo import MAX_TIME, MIN_TIME


def test_single_day_via_exclusive_end_yields_one_inclusive_day() -> None:
    start_dt, end_dt = to_inclusive_datetime_bounds(date(2026, 5, 5), date(2026, 5, 6))

    assert start_dt == datetime.combine(date(2026, 5, 5), MIN_TIME)
    assert end_dt == datetime.combine(date(2026, 5, 5), MAX_TIME)


def test_multi_day_window_subtracts_one_day_from_exclusive_end() -> None:
    start_dt, end_dt = to_inclusive_datetime_bounds(date(2026, 4, 22), date(2026, 5, 6))

    assert start_dt == datetime.combine(date(2026, 4, 22), MIN_TIME)
    assert end_dt == datetime.combine(date(2026, 5, 5), MAX_TIME)


def test_omitted_end_date_covers_only_start_date() -> None:
    start_dt, end_dt = to_inclusive_datetime_bounds(date(2026, 5, 5))

    assert start_dt == datetime.combine(date(2026, 5, 5), MIN_TIME)
    assert end_dt == datetime.combine(date(2026, 5, 5), MAX_TIME)
