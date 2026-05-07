"""Tests for build_backfill_plan."""

from datetime import date, timedelta

from pv_prospect.etl import (
    BackfillCursor,
    BackfillPlan,
    build_backfill_plan,
    initial_backfill_cursor,
)


def test_initial_cursor_starts_at_today() -> None:
    today = date(2026, 5, 6)

    cursor = initial_backfill_cursor(today)

    assert cursor.next_end_date == today


def test_plan_covers_window_days_before_cursor_end() -> None:
    cursor = BackfillCursor(next_end_date=date(2026, 5, 6))

    plan, _ = build_backfill_plan(cursor, window_days=14)

    assert plan.start_date == date(2026, 5, 6) - timedelta(days=14)
    assert plan.end_date == date(2026, 5, 6)


def test_next_cursor_advances_by_window_days() -> None:
    cursor = BackfillCursor(next_end_date=date(2026, 5, 6))

    _, next_cursor = build_backfill_plan(cursor, window_days=28)

    assert next_cursor.next_end_date == date(2026, 5, 6) - timedelta(days=28)


def test_successive_plans_do_not_overlap() -> None:
    cursor = BackfillCursor(next_end_date=date(2026, 5, 6))

    plan_1, cursor_1 = build_backfill_plan(cursor, window_days=28)
    plan_2, _ = build_backfill_plan(cursor_1, window_days=28)

    assert plan_2.end_date == plan_1.start_date


def test_successive_plans_cover_contiguous_dates() -> None:
    cursor = BackfillCursor(next_end_date=date(2026, 5, 6))

    plan_1, cursor_1 = build_backfill_plan(cursor, window_days=28)
    plan_2, cursor_2 = build_backfill_plan(cursor_1, window_days=28)
    plan_3, _ = build_backfill_plan(cursor_2, window_days=28)

    total_days = (plan_1.end_date - plan_3.start_date).days
    assert total_days == 3 * 28


def test_first_run_from_initial_cursor() -> None:
    today = date(2026, 5, 6)
    cursor = initial_backfill_cursor(today)

    plan, next_cursor = build_backfill_plan(cursor, window_days=28)

    assert plan == BackfillPlan(
        start_date=today - timedelta(days=28),
        end_date=today,
    )
    assert next_cursor.next_end_date == today - timedelta(days=28)
