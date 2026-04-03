"""Tests for cursor persistence."""

from datetime import date

from pv_prospect.data_extraction.processing.manifest import (
    CURSOR_PATH,
    BackfillCursor,
    deserialize_cursor,
    load_cursor,
    save_cursor,
    serialize_cursor,
)


class FakeFileSystem:
    def __init__(self) -> None:
        self._files: dict[str, str] = {}

    def exists(self, path: str) -> bool:
        return path in self._files

    def read_text(self, path: str) -> str:
        return self._files[path]

    def write_text(self, path: str, content: str) -> None:
        self._files[path] = content


_CURSOR = BackfillCursor(
    next_end_date=date(2025, 11, 20),
    next_sample_offset=9,
)


def test_serialize_roundtrip() -> None:
    text = serialize_cursor(_CURSOR)
    result = deserialize_cursor(text)

    assert result == _CURSOR


def test_serialize_produces_json_with_iso_date() -> None:
    text = serialize_cursor(_CURSOR)

    assert '2025-11-20' in text
    assert '"next_sample_offset": 9' in text


def test_load_cursor_returns_initial_when_file_missing() -> None:
    fs = FakeFileSystem()
    today = date(2026, 4, 3)

    cursor = load_cursor(fs, today)

    assert cursor.next_end_date == date(2026, 3, 20)
    assert cursor.next_sample_offset == 1


def test_load_cursor_reads_existing_file() -> None:
    fs = FakeFileSystem()
    fs._files[CURSOR_PATH] = serialize_cursor(_CURSOR)

    cursor = load_cursor(fs, date(2026, 4, 3))

    assert cursor == _CURSOR


def test_save_cursor_writes_to_expected_path() -> None:
    fs = FakeFileSystem()

    save_cursor(fs, _CURSOR)

    assert CURSOR_PATH in fs._files
    assert deserialize_cursor(fs._files[CURSOR_PATH]) == _CURSOR


def test_save_then_load_roundtrip() -> None:
    fs = FakeFileSystem()

    save_cursor(fs, _CURSOR)
    result = load_cursor(fs, date(2026, 4, 3))

    assert result == _CURSOR
