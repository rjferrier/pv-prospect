"""Tests for plan/commit persistence and the phased-manifest contents."""

import json
from datetime import date, timedelta

from pv_prospect.data_extraction.processing.weather_grid_backfill import (
    WORKFLOW_NAME,
    WeatherGridBackfillCursor,
    build_weather_grid_manifest,
    commit_weather_grid_backfill,
    deserialize_next_cursor,
    initial_weather_grid_backfill_cursor,
    load_cursor,
    plan_weather_grid_backfill,
    serialize_manifest,
)
from pv_prospect.data_extraction.processing.weather_grid_backfill import (
    build_phases as build_weather_grid_phases,
)
from pv_prospect.etl import compute_task_hash

_RUN_DATE = '2026-04-09'
_CURSOR_PATH = f'{WORKFLOW_NAME}.json'
_MANIFEST_PATH = f'{_RUN_DATE}/{WORKFLOW_NAME}.backfill.json'
_DATA_SOURCE = 'openmeteo_quarterhourly'
_DRY_RUN = 'false'
_NUM_SAMPLE_FILES = 32


class _FakeFileSystem:
    def __init__(self, files: dict[str, str] | None = None) -> None:
        self.files: dict[str, str] = dict(files or {})

    def exists(self, path: str) -> bool:
        return path in self.files

    def read_text(self, path: str) -> str:
        return self.files[path]

    def write_text(self, path: str, content: str) -> None:
        self.files[path] = content

    def list_files(
        self, prefix: str, pattern: str = '*', recursive: bool = False
    ) -> list:
        return []


def _resources_fs(num_files: int = _NUM_SAMPLE_FILES) -> _FakeFileSystem:
    """Build a resources FakeFileSystem pre-seeded with sample files.

    Each sample file ``sample_NNN.csv`` carries two grid points so the
    resulting manifest entries have a non-trivial ``LOCATIONS`` array
    that varies by index.
    """
    files: dict[str, str] = {}
    for i in range(num_files):
        # Make the contents index-dependent so different sample files
        # produce different LOCATIONS arrays.
        files[f'point_samples/sample_{i:03d}.csv'] = (
            f'50.{i:02d},-3.{i:02d}\n51.{i:02d},-4.{i:02d}\n'
        )
    return _FakeFileSystem(files)


def _plan(
    cursors_fs: _FakeFileSystem,
    manifests_fs: _FakeFileSystem,
    today: date = date(2026, 4, 9),
    num_sample_files: int = _NUM_SAMPLE_FILES,
    resources_fs: _FakeFileSystem | None = None,
) -> None:
    plan_weather_grid_backfill(
        today,
        _RUN_DATE,
        num_sample_files,
        _DATA_SOURCE,
        _DRY_RUN,
        cursors_fs,
        manifests_fs,
        resources_fs or _resources_fs(num_sample_files),
    )


def test_serialize_manifest_carries_phases_and_next_cursor() -> None:
    today = date(2026, 4, 9)
    cursor = initial_weather_grid_backfill_cursor(today)
    manifest, next_cursor = build_weather_grid_manifest(
        today, _NUM_SAMPLE_FILES, cursor
    )
    phases = build_weather_grid_phases(
        manifest, _resources_fs(), _DATA_SOURCE, _DRY_RUN, _RUN_DATE
    )

    data = json.loads(serialize_manifest(phases, next_cursor))

    assert 'phases' in data
    assert 'next_cursor' in data


def test_deserialize_next_cursor_round_trips() -> None:
    today = date(2026, 4, 9)
    cursor = initial_weather_grid_backfill_cursor(today)
    manifest, expected_next = build_weather_grid_manifest(
        today, _NUM_SAMPLE_FILES, cursor
    )
    phases = build_weather_grid_phases(
        manifest, _resources_fs(), _DATA_SOURCE, _DRY_RUN, _RUN_DATE
    )

    text = serialize_manifest(phases, expected_next)

    assert deserialize_next_cursor(text) == expected_next


def test_plan_weather_grid_backfill_writes_manifest_file() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    _plan(cursors_fs, manifests_fs)

    assert _MANIFEST_PATH in manifests_fs.files


def test_plan_weather_grid_backfill_does_not_advance_live_cursor() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    _plan(cursors_fs, manifests_fs)

    assert _CURSOR_PATH not in cursors_fs.files


def test_commit_weather_grid_backfill_promotes_next_cursor() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()
    today = date(2026, 4, 9)
    _plan(cursors_fs, manifests_fs, today=today)
    expected_next = deserialize_next_cursor(manifests_fs.files[_MANIFEST_PATH])

    committed = commit_weather_grid_backfill(_RUN_DATE, cursors_fs, manifests_fs)

    assert committed == expected_next
    assert load_cursor(cursors_fs, today) == expected_next


def test_plan_then_commit_advances_live_cursor() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()
    today = date(2026, 4, 9)

    _plan(cursors_fs, manifests_fs, today=today)
    commit_weather_grid_backfill(_RUN_DATE, cursors_fs, manifests_fs)

    cursor = load_cursor(cursors_fs, today)
    # With the default 8-batch step 3 plan, the cursor offset advances from
    # 1 to 9 (initial 1 + 8 batches).
    assert cursor.next_sample_offset == 9


def test_plan_weather_grid_backfill_without_prior_cursor_uses_initial() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()
    today = date(2026, 4, 9)

    _plan(cursors_fs, manifests_fs, today=today)
    next_cursor = deserialize_next_cursor(manifests_fs.files[_MANIFEST_PATH])

    # Initial cursor starts at (today - 14). After 8 more 14-day backward
    # steps the next_end_date should be (today - 14 - 8*14) = today - 126.
    assert next_cursor.next_end_date == today - timedelta(days=126)


def _env_value(task_env: list[dict[str, str]], name: str) -> str | None:
    return next((e['value'] for e in task_env if e['name'] == name), None)


def test_phases_have_one_phase_with_all_batches_together() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    _plan(cursors_fs, manifests_fs)
    data = json.loads(manifests_fs.files[_MANIFEST_PATH])

    # One phase (sequential pacing handled by the workflow YAML); 1 step2
    # batch + 8 step3 batches = 9 tasks.
    assert len(data['phases']) == 1
    assert len(data['phases'][0]) == 9


def test_no_task_carries_a_pre_injected_task_hash() -> None:
    """Per-site task identity is computed inside the container via
    ``compute_task_hash`` against the env (and the LOCATIONS array it
    carries), not injected at plan time. Regression guard for the
    predictable-cadence design."""
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    _plan(cursors_fs, manifests_fs)
    data = json.loads(manifests_fs.files[_MANIFEST_PATH])

    for task in data['phases'][0]:
        assert _env_value(task, 'TASK_HASH') is None


def test_task_envs_carry_data_source_and_run_date() -> None:
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    _plan(cursors_fs, manifests_fs)
    data = json.loads(manifests_fs.files[_MANIFEST_PATH])
    first_task = data['phases'][0][0]

    assert _env_value(first_task, 'DATA_SOURCE') == _DATA_SOURCE
    assert _env_value(first_task, 'RUN_DATE') == _RUN_DATE


def test_task_envs_carry_locations_resolved_from_sample_files() -> None:
    """Each batch env carries a LOCATIONS env var (JSON array) whose
    contents are the lat,lon strings read from the batch's sample file,
    rather than a SAMPLE_FILE_INDEX pointer."""
    cursors_fs = _FakeFileSystem()
    manifests_fs = _FakeFileSystem()

    _plan(cursors_fs, manifests_fs)
    data = json.loads(manifests_fs.files[_MANIFEST_PATH])

    for task in data['phases'][0]:
        assert _env_value(task, 'SAMPLE_FILE_INDEX') is None
        locations_raw = _env_value(task, 'LOCATIONS')
        assert locations_raw is not None
        locations = json.loads(locations_raw)
        # Each fixture sample file has 2 rows.
        assert len(locations) == 2
        # Every entry is a "lat,lon" string.
        for loc in locations:
            assert ',' in loc


def test_same_cursor_state_produces_envs_with_same_computed_hashes() -> None:
    """Container-computed task identity is deterministic given the same
    cursor + sample files: planning twice yields envs whose
    ``compute_task_hash`` values match position-by-position."""
    cursor = WeatherGridBackfillCursor(
        next_end_date=date(2026, 3, 26),
        next_sample_offset=1,
    )
    cursor_json = json.dumps(
        {
            'next_end_date': cursor.next_end_date.isoformat(),
            'next_sample_offset': cursor.next_sample_offset,
        }
    )
    fs_a_c = _FakeFileSystem({_CURSOR_PATH: cursor_json})
    fs_a_m = _FakeFileSystem()
    fs_b_c = _FakeFileSystem({_CURSOR_PATH: cursor_json})
    fs_b_m = _FakeFileSystem()

    _plan(fs_a_c, fs_a_m)
    _plan(fs_b_c, fs_b_m)
    hashes_a = [
        compute_task_hash(t)
        for t in json.loads(fs_a_m.files[_MANIFEST_PATH])['phases'][0]
    ]
    hashes_b = [
        compute_task_hash(t)
        for t in json.loads(fs_b_m.files[_MANIFEST_PATH])['phases'][0]
    ]

    assert hashes_a == hashes_b
