"""Tests for pv_sites_plan."""

from datetime import date, timedelta

from pv_prospect.etl import (
    BackfillCursor,
    BackfillScope,
    default_window_days,
    pv_sites_plan,
)


def test_default_window_matches_pv_sites_scope() -> None:
    today = date(2026, 5, 6)
    cursor = BackfillCursor(next_end_date=today)

    plan, _ = pv_sites_plan(cursor)

    assert plan.end_date == today
    assert plan.start_date == today - timedelta(
        days=default_window_days(BackfillScope.PV_SITES)
    )


def test_explicit_window_days_overrides_default() -> None:
    today = date(2026, 5, 6)
    cursor = BackfillCursor(next_end_date=today)

    plan, _ = pv_sites_plan(cursor, window_days=7)

    assert plan.end_date - plan.start_date == timedelta(days=7)


def test_next_cursor_starts_at_plan_start() -> None:
    today = date(2026, 5, 6)
    cursor = BackfillCursor(next_end_date=today)

    plan, next_cursor = pv_sites_plan(cursor)

    assert next_cursor.next_end_date == plan.start_date
