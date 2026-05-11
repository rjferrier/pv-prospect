"""Tests for the PV-site backfill wrapper.

The cursor / plan-commit logic itself is tested in pv-prospect-etl. These
tests cover only what the wrapper adds: the right paths are used and the
PV_SITES default window (28 days) is applied when none is given.
"""

from datetime import date, timedelta

from pv_prospect.data_extraction.processing.pv_backfill import (
    WORKFLOW_NAME,
    commit_pv_site_backfill,
    plan_pv_site_backfill,
)
from pv_prospect.etl import (
    cursor_filename,
    deserialize_plan,
    load_cursor,
    manifest_filename,
)

_RUN_DATE = '2026-05-06'
_EXPECTED_CURSOR = cursor_filename(WORKFLOW_NAME)
_EXPECTED_MANIFEST = manifest_filename(WORKFLOW_NAME, _RUN_DATE)


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
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    plan_pv_site_backfill(date(2026, 5, 6), _RUN_DATE, cursors_fs, manifests_fs)

    assert _EXPECTED_MANIFEST in manifests_fs.files


def test_plan_uses_28_day_default_window() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()
    today = date(2026, 5, 6)

    plan = plan_pv_site_backfill(today, _RUN_DATE, cursors_fs, manifests_fs)

    assert plan.start_date == today - timedelta(days=28)
    assert plan.end_date == today


def test_commit_advances_cursor_at_expected_path() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()
    today = date(2026, 5, 6)

    plan_pv_site_backfill(today, _RUN_DATE, cursors_fs, manifests_fs)
    commit_pv_site_backfill(_RUN_DATE, cursors_fs, manifests_fs)

    assert _EXPECTED_CURSOR in cursors_fs.files
    cursor = load_cursor(cursors_fs, WORKFLOW_NAME, today)
    assert cursor.next_end_date == today - timedelta(days=28)


def test_manifest_contents_round_trip() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()
    today = date(2026, 5, 6)

    returned = plan_pv_site_backfill(today, _RUN_DATE, cursors_fs, manifests_fs)
    persisted, _ = deserialize_plan(manifests_fs.files[_EXPECTED_MANIFEST])

    assert returned == persisted
