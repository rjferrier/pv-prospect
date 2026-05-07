"""Tests for backfill cursor and plan persistence."""

from datetime import date, timedelta

from pv_prospect.etl import (
    BackfillCursor,
    BackfillPaths,
    BackfillPlan,
    commit_backfill,
    deserialize_cursor,
    deserialize_plan,
    load_cursor,
    plan_backfill,
    serialize_cursor,
    serialize_plan,
)

from tests.unit.helpers import FakeFileSystem

_PATHS = BackfillPaths(
    cursor='manifests/test_backfill_cursor.json',
    manifest='manifests/test_backfill_manifest.json',
)
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
    fs = FakeFileSystem()
    today = date(2026, 5, 6)

    cursor = load_cursor(fs, _PATHS, today)

    assert cursor.next_end_date == today


def test_load_cursor_returns_persisted_cursor() -> None:
    fs = FakeFileSystem()
    fs.write_text(
        _PATHS.cursor,
        serialize_cursor(BackfillCursor(next_end_date=date(2026, 3, 1))),
    )

    cursor = load_cursor(fs, _PATHS, date(2026, 5, 6))

    assert cursor.next_end_date == date(2026, 3, 1)


def test_plan_backfill_writes_manifest() -> None:
    fs = FakeFileSystem()

    plan_backfill(fs, _PATHS, today=date(2026, 5, 6), window_days=28)

    assert fs.exists(_PATHS.manifest)


def test_plan_backfill_does_not_advance_live_cursor() -> None:
    fs = FakeFileSystem()

    plan_backfill(fs, _PATHS, today=date(2026, 5, 6), window_days=28)

    assert not fs.exists(_PATHS.cursor)


def test_plan_backfill_returns_plan_consistent_with_file() -> None:
    fs = FakeFileSystem()

    returned = plan_backfill(fs, _PATHS, today=date(2026, 5, 6), window_days=28)
    persisted, _ = deserialize_plan(fs.read_text(_PATHS.manifest))

    assert returned == persisted


def test_commit_backfill_promotes_next_cursor() -> None:
    fs = FakeFileSystem()
    fs.write_text(_PATHS.manifest, serialize_plan(_PLAN, _NEXT_CURSOR))

    committed = commit_backfill(fs, _PATHS)

    assert committed == _NEXT_CURSOR
    assert load_cursor(fs, _PATHS, date(2026, 5, 6)) == _NEXT_CURSOR


def test_plan_then_commit_advances_live_cursor() -> None:
    fs = FakeFileSystem()
    today = date(2026, 5, 6)

    plan_backfill(fs, _PATHS, today=today, window_days=28)
    commit_backfill(fs, _PATHS)

    cursor = load_cursor(fs, _PATHS, today)
    assert cursor.next_end_date == today - timedelta(days=28)


def test_paths_isolate_concurrent_backfills() -> None:
    fs = FakeFileSystem()
    today = date(2026, 5, 6)
    paths_a = BackfillPaths(cursor='a/cursor.json', manifest='a/manifest.json')
    paths_b = BackfillPaths(cursor='b/cursor.json', manifest='b/manifest.json')

    plan_backfill(fs, paths_a, today=today, window_days=28)
    commit_backfill(fs, paths_a)

    cursor_a = load_cursor(fs, paths_a, today)
    cursor_b = load_cursor(fs, paths_b, today)

    assert cursor_a.next_end_date == today - timedelta(days=28)
    assert cursor_b.next_end_date == today
