"""Tests for PV backfill cursor and plan persistence."""

from datetime import date, timedelta

from pv_prospect.data_extraction.processing.pv_backfill import (
    PV_BACKFILL_CURSOR_PATH,
    PV_BACKFILL_MANIFEST_PATH,
    PvBackfillCursor,
    PvBackfillPlan,
    commit_pv_site_backfill,
    deserialize_cursor,
    deserialize_plan,
    load_cursor,
    plan_pv_site_backfill,
    serialize_cursor,
    serialize_plan,
)


class FakeFileSystem:
    def __init__(self) -> None:
        self.files: dict[str, str] = {}

    def exists(self, path: str) -> bool:
        return path in self.files

    def read_text(self, path: str) -> str:
        return self.files[path]

    def write_text(self, path: str, content: str) -> None:
        self.files[path] = content


_PLAN = PvBackfillPlan(
    start_date=date(2026, 3, 13),
    end_date=date(2026, 4, 10),
)

_NEXT_CURSOR = PvBackfillCursor(next_end_date=date(2026, 3, 13))


def test_serialize_cursor_roundtrip() -> None:
    cursor = PvBackfillCursor(next_end_date=date(2026, 4, 10))

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
    today = date(2026, 4, 10)

    cursor = load_cursor(fs, today)

    assert cursor.next_end_date == today


def test_load_cursor_returns_persisted_cursor() -> None:
    fs = FakeFileSystem()
    fs.files[PV_BACKFILL_CURSOR_PATH] = serialize_cursor(
        PvBackfillCursor(next_end_date=date(2026, 3, 1))
    )

    cursor = load_cursor(fs, date(2026, 4, 10))

    assert cursor.next_end_date == date(2026, 3, 1)


def test_plan_pv_site_backfill_writes_manifest() -> None:
    fs = FakeFileSystem()
    today = date(2026, 4, 10)

    plan_pv_site_backfill(today, fs)

    assert PV_BACKFILL_MANIFEST_PATH in fs.files


def test_plan_pv_site_backfill_does_not_advance_live_cursor() -> None:
    fs = FakeFileSystem()
    today = date(2026, 4, 10)

    plan_pv_site_backfill(today, fs)

    assert PV_BACKFILL_CURSOR_PATH not in fs.files


def test_plan_pv_site_backfill_returns_plan_consistent_with_file() -> None:
    fs = FakeFileSystem()
    today = date(2026, 4, 10)

    returned = plan_pv_site_backfill(today, fs)
    persisted, _ = deserialize_plan(fs.files[PV_BACKFILL_MANIFEST_PATH])

    assert returned == persisted


def test_commit_pv_site_backfill_promotes_next_cursor() -> None:
    fs = FakeFileSystem()
    fs.files[PV_BACKFILL_MANIFEST_PATH] = serialize_plan(_PLAN, _NEXT_CURSOR)

    committed = commit_pv_site_backfill(fs)

    assert committed == _NEXT_CURSOR
    assert load_cursor(fs, date(2026, 4, 10)) == _NEXT_CURSOR


def test_plan_then_commit_advances_live_cursor() -> None:
    fs = FakeFileSystem()
    today = date(2026, 4, 10)

    plan_pv_site_backfill(today, fs)
    commit_pv_site_backfill(fs)

    cursor = load_cursor(fs, today)
    assert cursor.next_end_date == today - timedelta(days=28)
