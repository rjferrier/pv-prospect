from datetime import date

from pv_prospect.data_sources import format_date


def test_formats_standard_date():
    assert format_date(date(2025, 6, 24)) == '20250624'


def test_zero_pads_month_and_day():
    assert format_date(date(2025, 1, 5)) == '20250105'
