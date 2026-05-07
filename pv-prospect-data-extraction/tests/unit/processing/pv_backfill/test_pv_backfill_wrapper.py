"""Tests for the PV-site backfill wrapper.

The cursor / plan-commit logic itself is tested in pv-prospect-etl. These
tests cover only what the wrapper adds: the right paths are used and the
PV_SITES default window (28 days) is applied when none is given.
"""

from datetime import date, timedelta

from pv_prospect.data_extraction.processing.pv_backfill import (
    commit_pv_site_backfill,
    plan_pv_site_backfill,
)
from pv_prospect.etl import deserialize_plan, load_cursor
from pv_prospect.etl.backfill import BackfillPaths

_EXPECTED_PATHS = BackfillPaths(
    cursor='manifests/pv_backfill_cursor.json',
    manifest='manifests/todays_pv_backfill_manifest.json',
)


class _FakeFileSystem:
    def __init__(self) -> None:
        self.files: dict[str, str] = {}

    def exists(self, path: str) -> bool:
        return path in self.files

    def read_text(self, path: str) -> str:
        return self.files[path]

    def write_text(self, path: str, content: str) -> None:
        self.files[path] = content


def test_plan_writes_manifest_at_expected_path() -> None:
    fs = _FakeFileSystem()

    plan_pv_site_backfill(date(2026, 5, 6), fs)

    assert _EXPECTED_PATHS.manifest in fs.files


def test_plan_uses_28_day_default_window() -> None:
    fs = _FakeFileSystem()
    today = date(2026, 5, 6)

    plan = plan_pv_site_backfill(today, fs)

    assert plan.start_date == today - timedelta(days=28)
    assert plan.end_date == today


def test_commit_advances_cursor_at_expected_path() -> None:
    fs = _FakeFileSystem()
    today = date(2026, 5, 6)

    plan_pv_site_backfill(today, fs)
    commit_pv_site_backfill(fs)

    assert _EXPECTED_PATHS.cursor in fs.files
    cursor = load_cursor(fs, _EXPECTED_PATHS, today)
    assert cursor.next_end_date == today - timedelta(days=28)


def test_manifest_contents_round_trip() -> None:
    fs = _FakeFileSystem()
    today = date(2026, 5, 6)

    returned = plan_pv_site_backfill(today, fs)
    persisted, _ = deserialize_plan(fs.files[_EXPECTED_PATHS.manifest])

    assert returned == persisted
