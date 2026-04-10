"""Tests for build_pv_backfill_plan."""

from datetime import date, timedelta

from pv_prospect.data_extraction.processing.pv_backfill import (
    PV_BACKFILL_WINDOW_DAYS,
    PvBackfillCursor,
    PvBackfillPlan,
    build_pv_backfill_plan,
    initial_pv_backfill_cursor,
)


def test_initial_cursor_starts_at_today() -> None:
    today = date(2026, 4, 10)
    cursor = initial_pv_backfill_cursor(today)

    assert cursor.next_end_date == today


def test_plan_covers_window_days_before_cursor_end() -> None:
    cursor = PvBackfillCursor(next_end_date=date(2026, 4, 10))

    plan, _ = build_pv_backfill_plan(cursor)

    assert plan.start_date == date(2026, 4, 10) - timedelta(
        days=PV_BACKFILL_WINDOW_DAYS
    )
    assert plan.end_date == date(2026, 4, 10)


def test_next_cursor_advances_by_window_days() -> None:
    cursor = PvBackfillCursor(next_end_date=date(2026, 4, 10))

    _, next_cursor = build_pv_backfill_plan(cursor)

    assert next_cursor.next_end_date == date(2026, 4, 10) - timedelta(
        days=PV_BACKFILL_WINDOW_DAYS
    )


def test_successive_plans_do_not_overlap() -> None:
    cursor = PvBackfillCursor(next_end_date=date(2026, 4, 10))

    plan_1, cursor_1 = build_pv_backfill_plan(cursor)
    plan_2, _ = build_pv_backfill_plan(cursor_1)

    assert plan_2.end_date == plan_1.start_date


def test_successive_plans_cover_contiguous_dates() -> None:
    cursor = PvBackfillCursor(next_end_date=date(2026, 4, 10))

    plan_1, cursor_1 = build_pv_backfill_plan(cursor)
    plan_2, cursor_2 = build_pv_backfill_plan(cursor_1)
    plan_3, _ = build_pv_backfill_plan(cursor_2)

    total_days = (plan_1.end_date - plan_3.start_date).days
    assert total_days == 3 * PV_BACKFILL_WINDOW_DAYS


def test_custom_window_days() -> None:
    cursor = PvBackfillCursor(next_end_date=date(2026, 4, 10))

    plan, next_cursor = build_pv_backfill_plan(cursor, window_days=7)

    assert plan.start_date == date(2026, 4, 3)
    assert plan.end_date == date(2026, 4, 10)
    assert next_cursor.next_end_date == date(2026, 4, 3)


def test_first_run_from_initial_cursor() -> None:
    today = date(2026, 4, 10)
    cursor = initial_pv_backfill_cursor(today)

    plan, next_cursor = build_pv_backfill_plan(cursor)

    assert plan == PvBackfillPlan(
        start_date=today - timedelta(days=PV_BACKFILL_WINDOW_DAYS),
        end_date=today,
    )
    assert next_cursor.next_end_date == today - timedelta(days=PV_BACKFILL_WINDOW_DAYS)
