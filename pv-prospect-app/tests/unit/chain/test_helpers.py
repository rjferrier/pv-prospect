"""Tests for pure helper functions in chain.py."""

from __future__ import annotations

import datetime

from pv_prospect.app.chain import (
    OutsideUKDomainError,
    _days_in_month_within_range,
    _months_in_range,
    check_uk_domain,
)


def test_check_uk_domain_accepts_central_uk() -> None:
    check_uk_domain(52.0, -1.0)


def test_check_uk_domain_rejects_outside_uk() -> None:
    import pytest

    with pytest.raises(OutsideUKDomainError):
        check_uk_domain(48.0, 2.5)


def test_months_in_range_full_year() -> None:
    months = _months_in_range(datetime.date(2025, 1, 1), datetime.date(2025, 12, 31))
    assert len(months) == 12
    assert months[0] == datetime.date(2025, 1, 1)
    assert months[-1] == datetime.date(2025, 12, 1)


def test_months_in_range_single_month() -> None:
    months = _months_in_range(datetime.date(2025, 6, 1), datetime.date(2025, 6, 30))
    assert months == [datetime.date(2025, 6, 1)]


def test_months_in_range_crosses_year_boundary() -> None:
    months = _months_in_range(datetime.date(2025, 11, 1), datetime.date(2026, 2, 28))
    assert len(months) == 4
    assert months[-1] == datetime.date(2026, 2, 1)


def test_days_in_month_within_range_full_month() -> None:
    month_start = datetime.date(2025, 6, 1)
    days = _days_in_month_within_range(
        month_start, datetime.date(2025, 1, 1), datetime.date(2025, 12, 31)
    )
    assert len(days) == 30
    assert days[0] == datetime.date(2025, 6, 1)
    assert days[-1] == datetime.date(2025, 6, 30)


def test_days_in_month_within_range_partial_start() -> None:
    month_start = datetime.date(2025, 6, 1)
    days = _days_in_month_within_range(
        month_start, datetime.date(2025, 6, 15), datetime.date(2025, 12, 31)
    )
    assert days[0] == datetime.date(2025, 6, 15)
    assert len(days) == 16
