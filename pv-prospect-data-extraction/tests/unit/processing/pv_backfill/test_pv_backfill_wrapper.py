"""Tests for the PV-site backfill wrapper.

The cursor / plan-commit logic itself is tested in pv-prospect-etl. These
tests cover only what the wrapper adds: the right paths are used, the
PV_SITES default window (28 days) is applied when none is given, and the
persisted manifest carries phased per-task envs (one env per
(pv_system_id, window) pair, with no pre-injected TASK_HASH).
"""

import json
from datetime import date, timedelta

from pv_prospect.data_extraction.processing.pv_backfill import (
    WORKFLOW_NAME,
    commit_pv_site_backfill,
    plan_pv_site_backfill,
)
from pv_prospect.etl import (
    BackfillPlan,
    compute_task_hash,
    cursor_filename,
    deserialize_plan,
    load_cursor,
    manifest_filename,
)

_RUN_DATE = '2026-05-06'
_EXPECTED_CURSOR = cursor_filename(WORKFLOW_NAME)
_EXPECTED_MANIFEST = manifest_filename(WORKFLOW_NAME, _RUN_DATE)
_PV_SYSTEM_IDS = [89665, 12345, 67890]
_PV_DATA_SOURCE = 'pvoutput'
_WEATHER_DATA_SOURCE = 'openmeteo_quarterhourly'
_DRY_RUN = 'false'


class _FakeFileSystem:
    def __init__(self) -> None:
        self.files: dict[str, str] = {}

    def exists(self, path: str) -> bool:
        return path in self.files

    def read_text(self, path: str) -> str:
        return self.files[path]

    def write_text(self, path: str, content: str) -> None:
        self.files[path] = content


def _plan(
    cursors_fs: _FakeFileSystem,
    manifests_fs: _FakeFileSystem,
    today: date = date(2026, 5, 6),
) -> BackfillPlan:
    return plan_pv_site_backfill(
        today,
        _RUN_DATE,
        _PV_SYSTEM_IDS,
        _PV_DATA_SOURCE,
        _WEATHER_DATA_SOURCE,
        _DRY_RUN,
        cursors_fs,
        manifests_fs,
    )


def test_plan_writes_manifest_at_expected_path() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    _plan(cursors_fs, manifests_fs)

    assert _EXPECTED_MANIFEST in manifests_fs.files


def test_plan_uses_28_day_default_window() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()
    today = date(2026, 5, 6)

    plan = _plan(cursors_fs, manifests_fs, today=today)

    assert plan.start_date == today - timedelta(days=28)
    assert plan.end_date == today


def test_commit_advances_cursor_at_expected_path() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()
    today = date(2026, 5, 6)

    _plan(cursors_fs, manifests_fs, today=today)
    commit_pv_site_backfill(_RUN_DATE, cursors_fs, manifests_fs)

    assert _EXPECTED_CURSOR in cursors_fs.files
    cursor = load_cursor(cursors_fs, WORKFLOW_NAME, today)
    assert cursor.next_end_date == today - timedelta(days=28)


def test_manifest_contents_round_trip() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()
    today = date(2026, 5, 6)

    returned = _plan(cursors_fs, manifests_fs, today=today)
    persisted, _ = deserialize_plan(manifests_fs.files[_EXPECTED_MANIFEST])

    assert returned == persisted


def test_manifest_has_two_phases() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    _plan(cursors_fs, manifests_fs)
    data = json.loads(manifests_fs.files[_EXPECTED_MANIFEST])

    assert len(data['phases']) == 2


def test_pv_phase_has_one_task_per_pv_system() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    _plan(cursors_fs, manifests_fs)
    data = json.loads(manifests_fs.files[_EXPECTED_MANIFEST])

    assert len(data['phases'][0]) == len(_PV_SYSTEM_IDS)


def test_weather_phase_has_one_task_per_pv_system() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    _plan(cursors_fs, manifests_fs)
    data = json.loads(manifests_fs.files[_EXPECTED_MANIFEST])

    assert len(data['phases'][1]) == len(_PV_SYSTEM_IDS)


def _env_value(task_env: list[dict[str, str]], name: str) -> str | None:
    return next((e['value'] for e in task_env if e['name'] == name), None)


def test_every_task_carries_a_pre_injected_task_hash() -> None:
    """Each task env carries a ``TASK_HASH`` so the container has a
    stable identity for naming its per-task scratch ledger/log file at
    flush time. Per-*site* hashes recorded in each ledger entry are
    still computed inside the container."""
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    _plan(cursors_fs, manifests_fs)
    data = json.loads(manifests_fs.files[_EXPECTED_MANIFEST])

    for phase in data['phases']:
        for task in phase:
            task_hash = _env_value(task, 'TASK_HASH')
            assert task_hash is not None and len(task_hash) == 64


def test_pv_task_carries_pv_data_source() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    _plan(cursors_fs, manifests_fs)
    data = json.loads(manifests_fs.files[_EXPECTED_MANIFEST])
    pv_task = data['phases'][0][0]

    assert _env_value(pv_task, 'DATA_SOURCE') == _PV_DATA_SOURCE


def test_weather_task_carries_weather_data_source() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    _plan(cursors_fs, manifests_fs)
    data = json.loads(manifests_fs.files[_EXPECTED_MANIFEST])
    weather_task = data['phases'][1][0]

    assert _env_value(weather_task, 'DATA_SOURCE') == _WEATHER_DATA_SOURCE


def test_same_inputs_produce_envs_with_same_computed_hashes() -> None:
    """Container-computed task identity is deterministic given the same
    planning inputs: planning twice yields envs whose
    ``compute_task_hash`` values match position-by-position."""
    fs_a_c, fs_a_m = _FakeFileSystem(), _FakeFileSystem()
    fs_b_c, fs_b_m = _FakeFileSystem(), _FakeFileSystem()

    _plan(fs_a_c, fs_a_m)
    _plan(fs_b_c, fs_b_m)
    data_a = json.loads(fs_a_m.files[_EXPECTED_MANIFEST])
    data_b = json.loads(fs_b_m.files[_EXPECTED_MANIFEST])
    hashes_a = [compute_task_hash(t) for p in data_a['phases'] for t in p]
    hashes_b = [compute_task_hash(t) for p in data_b['phases'] for t in p]

    assert hashes_a == hashes_b
