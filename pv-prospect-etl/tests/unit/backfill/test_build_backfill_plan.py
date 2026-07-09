"""Tests for build_backfill_plan."""

from datetime import date, timedelta

from pv_prospect.etl import (
    BackfillCursor,
    BackfillPlan,
    build_backfill_plan,
    initial_backfill_cursor,
)
from pv_prospect.etl.backfill import MIN_ARCHIVE_DATE


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


def test_default_floor_is_the_archive_start() -> None:
    cursor = BackfillCursor(next_end_date=MIN_ARCHIVE_DATE + timedelta(days=1))

    plan, _ = build_backfill_plan(cursor, window_days=28)

    assert plan.start_date == MIN_ARCHIVE_DATE


def test_final_window_is_truncated_at_the_floor() -> None:
    floor = date(2016, 1, 1)
    cursor = BackfillCursor(next_end_date=date(2016, 1, 15))

    plan, next_cursor = build_backfill_plan(cursor, window_days=28, floor=floor)

    assert plan == BackfillPlan(start_date=floor, end_date=date(2016, 1, 15))
    assert next_cursor.next_end_date == floor


def test_cursor_at_the_floor_plans_an_empty_window() -> None:
    floor = date(2016, 1, 1)
    cursor = BackfillCursor(next_end_date=floor)

    plan, next_cursor = build_backfill_plan(cursor, window_days=28, floor=floor)

    assert plan.start_date == plan.end_date == floor
    assert next_cursor.next_end_date == floor


def test_cursor_below_the_floor_never_plans_an_inverted_window() -> None:
    floor = date(2016, 1, 1)
    cursor = BackfillCursor(next_end_date=date(2010, 5, 21))

    plan, next_cursor = build_backfill_plan(cursor, window_days=28, floor=floor)

    assert plan.start_date == plan.end_date == floor
    assert next_cursor.next_end_date == floor


def test_halted_cursor_is_a_fixed_point() -> None:
    floor = date(2016, 1, 1)
    cursor = BackfillCursor(next_end_date=date(2016, 1, 15))

    _, cursor_1 = build_backfill_plan(cursor, window_days=28, floor=floor)
    _, cursor_2 = build_backfill_plan(cursor_1, window_days=28, floor=floor)

    assert cursor_2 == cursor_1
