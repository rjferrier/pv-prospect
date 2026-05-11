"""Tests for backfill cursor and plan persistence."""

from datetime import date, timedelta

from pv_prospect.etl import (
    BackfillCursor,
    BackfillPlan,
    commit_backfill,
    cursor_filename,
    deserialize_cursor,
    deserialize_plan,
    load_cursor,
    manifest_filename,
    plan_backfill,
    serialize_cursor,
    serialize_plan,
)

from tests.unit.helpers import FakeFileSystem

_WF = 'test-backfill'
_RUN_DATE = '2026-05-06'
_PLAN = BackfillPlan(
    start_date=date(2026, 4, 8),
    end_date=date(2026, 5, 6),
)
_NEXT_CURSOR = BackfillCursor(next_end_date=date(2026, 4, 8))


def test_serialize_cursor_roundtrip() -> None:
    cursor = BackfillCursor(next_end_date=date(2026, 5, 6))

    text = serialize_cursor(cursor)
    result = deserialize_cursor(text)

    assert result == cursor


def test_serialize_plan_roundtrip() -> None:
    text = serialize_plan(_PLAN, _NEXT_CURSOR)
    plan, next_cursor = deserialize_plan(text)

    assert plan == _PLAN
    assert next_cursor == _NEXT_CURSOR


def test_load_cursor_returns_initial_when_no_file() -> None:
    cursors_fs = FakeFileSystem()
    today = date(2026, 5, 6)

    cursor = load_cursor(cursors_fs, _WF, today)

    assert cursor.next_end_date == today


def test_load_cursor_returns_persisted_cursor() -> None:
    cursors_fs = FakeFileSystem()
    cursors_fs.write_text(
        cursor_filename(_WF),
        serialize_cursor(BackfillCursor(next_end_date=date(2026, 3, 1))),
    )

    cursor = load_cursor(cursors_fs, _WF, date(2026, 5, 6))

    assert cursor.next_end_date == date(2026, 3, 1)


def test_plan_backfill_writes_manifest() -> None:
    cursors_fs = FakeFileSystem()
    manifests_fs = FakeFileSystem()

    plan_backfill(
        cursors_fs,
        manifests_fs,
        _WF,
        _RUN_DATE,
        today=date(2026, 5, 6),
        window_days=28,
    )

    assert manifests_fs.exists(manifest_filename(_WF, _RUN_DATE))


def test_plan_backfill_does_not_advance_live_cursor() -> None:
    cursors_fs = FakeFileSystem()
    manifests_fs = FakeFileSystem()

    plan_backfill(
        cursors_fs,
        manifests_fs,
        _WF,
        _RUN_DATE,
        today=date(2026, 5, 6),
        window_days=28,
    )

    assert not cursors_fs.exists(cursor_filename(_WF))


def test_plan_backfill_returns_plan_consistent_with_file() -> None:
    cursors_fs = FakeFileSystem()
    manifests_fs = FakeFileSystem()

    returned = plan_backfill(
        cursors_fs,
        manifests_fs,
        _WF,
        _RUN_DATE,
        today=date(2026, 5, 6),
        window_days=28,
    )
    persisted, _ = deserialize_plan(
        manifests_fs.read_text(manifest_filename(_WF, _RUN_DATE))
    )

    assert returned == persisted


def test_commit_backfill_promotes_next_cursor() -> None:
    cursors_fs = FakeFileSystem()
    manifests_fs = FakeFileSystem()
    manifests_fs.write_text(
        manifest_filename(_WF, _RUN_DATE), serialize_plan(_PLAN, _NEXT_CURSOR)
    )

    committed = commit_backfill(cursors_fs, manifests_fs, _WF, _RUN_DATE)

    assert committed == _NEXT_CURSOR
    assert load_cursor(cursors_fs, _WF, date(2026, 5, 6)) == _NEXT_CURSOR


def test_plan_then_commit_advances_live_cursor() -> None:
    cursors_fs = FakeFileSystem()
    manifests_fs = FakeFileSystem()
    today = date(2026, 5, 6)

    plan_backfill(cursors_fs, manifests_fs, _WF, _RUN_DATE, today=today, window_days=28)
    commit_backfill(cursors_fs, manifests_fs, _WF, _RUN_DATE)

    cursor = load_cursor(cursors_fs, _WF, today)
    assert cursor.next_end_date == today - timedelta(days=28)


def test_distinct_workflows_isolate_their_cursors() -> None:
    cursors_fs = FakeFileSystem()
    manifests_fs = FakeFileSystem()
    today = date(2026, 5, 6)

    plan_backfill(
        cursors_fs, manifests_fs, 'wf-a', _RUN_DATE, today=today, window_days=28
    )
    commit_backfill(cursors_fs, manifests_fs, 'wf-a', _RUN_DATE)

    cursor_a = load_cursor(cursors_fs, 'wf-a', today)
    cursor_b = load_cursor(cursors_fs, 'wf-b', today)

    assert cursor_a.next_end_date == today - timedelta(days=28)
    assert cursor_b.next_end_date == today
