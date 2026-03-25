from datetime import date, timedelta

import pytest
from pv_prospect.common import DateRange
from pv_prospect.etl import DegenerateDateRange, build_date_range


def test_no_args_defaults_to_yesterday():
    result = build_date_range()
    yesterday = date.today() - timedelta(days=1)
    assert result == DateRange(yesterday, date.today())


def test_iso_start_only_defaults_end_to_next_day():
    result = build_date_range('2025-06-15')
    assert result == DateRange(date(2025, 6, 15), date(2025, 6, 16))


def test_iso_start_and_end():
    result = build_date_range('2025-06-01', '2025-06-30')
    assert result == DateRange(date(2025, 6, 1), date(2025, 6, 30))


def test_month_start_defaults_end_to_first_of_next_month():
    result = build_date_range('2025-06')
    assert result == DateRange(date(2025, 6, 1), date(2025, 7, 1))


def test_month_start_december_rolls_over_to_january():
    result = build_date_range('2024-12')
    assert result == DateRange(date(2024, 12, 1), date(2025, 1, 1))


def test_month_end_exclusive_covers_full_month():
    result = build_date_range('2025-06-01', '2025-06')
    assert result == DateRange(date(2025, 6, 1), date(2025, 7, 1))


def test_month_start_and_end_covers_both_months():
    result = build_date_range('2025-05', '2025-06')
    assert result == DateRange(date(2025, 5, 1), date(2025, 7, 1))


def test_degenerate_range_raises() -> None:
    with pytest.raises(DegenerateDateRange):
        build_date_range('2025-06-15', '2025-06-15')


def test_end_before_start_raises() -> None:
    with pytest.raises(DegenerateDateRange):
        build_date_range('2025-06-15', '2025-06-01')
