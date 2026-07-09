"""Tests for cursor persistence."""

from datetime import date

from pv_prospect.data_extraction.processing.weather_grid_backfill import (
    WORKFLOW_NAME,
    deserialize_cursor,
    load_cursor,
    save_cursor,
    serialize_cursor,
)
from pv_prospect.etl import WeatherGridBackfillCursor

_CURSOR_PATH = f'{WORKFLOW_NAME}.json'


class FakeFileSystem:
    def __init__(self) -> None:
        self._files: dict[str, str] = {}

    def exists(self, path: str) -> bool:
        return path in self._files

    def read_text(self, path: str) -> str:
        return self._files[path]

    def write_text(self, path: str, content: str) -> None:
        self._files[path] = content


_CURSOR = WeatherGridBackfillCursor(
    next_end_date=date(2025, 11, 20),
    density_pass=2,
)


def test_serialize_roundtrip() -> None:
    text = serialize_cursor(_CURSOR)
    result = deserialize_cursor(text)

    assert result == _CURSOR


def test_serialize_produces_json_with_iso_date() -> None:
    text = serialize_cursor(_CURSOR)

    assert '2025-11-20' in text
    assert '"density_pass": 2' in text


def test_deserialize_reads_a_pre_densifier_cursor_as_the_first_pass() -> None:
    legacy = '{"next_end_date": "2010-05-21", "next_sample_offset": 417}'

    cursor = deserialize_cursor(legacy)

    assert cursor.next_end_date == date(2010, 5, 21)
    assert cursor.density_pass == 0


def test_load_cursor_returns_initial_when_file_missing() -> None:
    cursors_fs = FakeFileSystem()
    today = date(2026, 4, 3)

    cursor = load_cursor(cursors_fs, today)

    assert cursor.next_end_date == date(2026, 3, 20)
    assert cursor.density_pass == 0


def test_load_cursor_reads_existing_file() -> None:
    cursors_fs = FakeFileSystem()
    cursors_fs._files[_CURSOR_PATH] = serialize_cursor(_CURSOR)

    cursor = load_cursor(cursors_fs, date(2026, 4, 3))

    assert cursor == _CURSOR


def test_save_cursor_writes_to_expected_path() -> None:
    cursors_fs = FakeFileSystem()

    save_cursor(cursors_fs, _CURSOR)

    assert _CURSOR_PATH in cursors_fs._files
    assert deserialize_cursor(cursors_fs._files[_CURSOR_PATH]) == _CURSOR


def test_save_then_load_roundtrip() -> None:
    cursors_fs = FakeFileSystem()

    save_cursor(cursors_fs, _CURSOR)
    result = load_cursor(cursors_fs, date(2026, 4, 3))

    assert result == _CURSOR
