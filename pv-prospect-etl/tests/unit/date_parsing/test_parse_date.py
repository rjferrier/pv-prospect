from datetime import date, timedelta

import pytest
from pv_prospect.etl import parse_date


def test_today():
    assert parse_date('today') == date.today()


def test_today_case_insensitive():
    assert parse_date('TODAY') == date.today()


def test_yesterday():
    assert parse_date('yesterday') == date.today() - timedelta(days=1)


def test_yesterday_case_insensitive():
    assert parse_date('Yesterday') == date.today() - timedelta(days=1)


def test_iso_date():
    assert parse_date('2025-06-15') == date(2025, 6, 15)


def test_month_format_returns_first_of_month():
    assert parse_date('2025-06') == date(2025, 6, 1)


def test_month_format_december():
    assert parse_date('2024-12') == date(2024, 12, 1)


def test_invalid_string_raises():
    with pytest.raises(ValueError):
        parse_date('not-a-date')
