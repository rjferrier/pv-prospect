"""Tests for manifest serialization and plan/commit persistence."""

from datetime import date, timedelta

from pv_prospect.data_extraction.processing.manifest import (
    CURSOR_PATH,
    MANIFEST_PATH,
    BackfillCursor,
    Batch,
    Manifest,
    commit_weather_grid_backfill,
    deserialize_manifest,
    load_cursor,
    plan_weather_grid_backfill,
    serialize_manifest,
)

_MANIFEST = Manifest(
    step2_batch=Batch(
        sample_file_index=17,
        start_date=date(2026, 3, 26),
        end_date=date(2026, 4, 9),
    ),
    step3_batches=[
        Batch(
            sample_file_index=16,
            start_date=date(2026, 3, 12),
            end_date=date(2026, 3, 26),
        ),
        Batch(
            sample_file_index=15,
            start_date=date(2026, 2, 26),
            end_date=date(2026, 3, 12),
        ),
    ],
)

_NEXT_CURSOR = BackfillCursor(
    next_end_date=date(2026, 2, 26),
    next_sample_offset=3,
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

    def list_files(
        self, prefix: str, pattern: str = '*', recursive: bool = False
    ) -> list:
        return []


def test_serialize_manifest_roundtrip() -> None:
    text = serialize_manifest(_MANIFEST, _NEXT_CURSOR)
    manifest, next_cursor = deserialize_manifest(text)

    assert manifest == _MANIFEST
    assert next_cursor == _NEXT_CURSOR


def test_serialize_manifest_preserves_step3_order() -> None:
    text = serialize_manifest(_MANIFEST, _NEXT_CURSOR)
    manifest, _ = deserialize_manifest(text)

    assert [b.sample_file_index for b in manifest.step3_batches] == [16, 15]


def test_plan_weather_grid_backfill_writes_manifest_file() -> None:
    fs = FakeFileSystem()
    today = date(2026, 4, 9)

    plan_weather_grid_backfill(today, num_sample_files=32, fs=fs)

    assert MANIFEST_PATH in fs.files


def test_plan_weather_grid_backfill_does_not_advance_live_cursor() -> None:
    fs = FakeFileSystem()
    today = date(2026, 4, 9)

    plan_weather_grid_backfill(today, num_sample_files=32, fs=fs)

    assert CURSOR_PATH not in fs.files


def test_plan_weather_grid_backfill_returns_manifest_consistent_with_file() -> None:
    fs = FakeFileSystem()
    today = date(2026, 4, 9)

    returned = plan_weather_grid_backfill(today, num_sample_files=32, fs=fs)
    persisted, _ = deserialize_manifest(fs.files[MANIFEST_PATH])

    assert returned == persisted


def test_commit_weather_grid_backfill_promotes_next_cursor() -> None:
    fs = FakeFileSystem()
    fs.files[MANIFEST_PATH] = serialize_manifest(_MANIFEST, _NEXT_CURSOR)

    committed = commit_weather_grid_backfill(fs)

    assert committed == _NEXT_CURSOR
    assert load_cursor(fs, date(2026, 4, 9)) == _NEXT_CURSOR


def test_plan_then_commit_advances_live_cursor() -> None:
    fs = FakeFileSystem()
    today = date(2026, 4, 9)

    plan_weather_grid_backfill(today, num_sample_files=32, fs=fs)
    commit_weather_grid_backfill(fs)

    cursor = load_cursor(fs, today)
    # With the default 8-batch step 3 plan, the cursor offset advances from
    # 1 to 9 (initial 1 + 8 batches).
    assert cursor.next_sample_offset == 9


def test_plan_weather_grid_backfill_without_prior_cursor_uses_initial() -> None:
    fs = FakeFileSystem()
    today = date(2026, 4, 9)

    plan_weather_grid_backfill(today, num_sample_files=32, fs=fs)
    _, next_cursor = deserialize_manifest(fs.files[MANIFEST_PATH])

    # Initial cursor starts at (today - 14). After 8 more 14-day backward
    # steps the next_end_date should be (today - 14 - 8*14) = today - 126.
    assert next_cursor.next_end_date == today - timedelta(days=126)
