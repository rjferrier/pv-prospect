"""Tests for count_sample_files."""

from pv_prospect.data_extraction.processing.sample_file import (
    SAMPLE_FILES_DIR,
    count_sample_files,
)
from pv_prospect.etl.storage import FileEntry


class FakeFileSystem:
    def __init__(self, entries: list[FileEntry]) -> None:
        self._entries = entries
        self.list_calls: list[tuple[str, str]] = []

    def list_files(
        self, prefix: str, pattern: str = '*', recursive: bool = False
    ) -> list[FileEntry]:
        self.list_calls.append((prefix, pattern))
        return self._entries


def _entry(name: str) -> FileEntry:
    return FileEntry(
        id=name,
        name=name,
        path=f'{SAMPLE_FILES_DIR}/{name}',
        parent_path=SAMPLE_FILES_DIR,
    )


def test_counts_entries_returned_by_list_files() -> None:
    fs = FakeFileSystem([_entry(f'sample_{i:03d}.csv') for i in range(32)])

    assert count_sample_files(fs) == 32


def test_lists_files_under_sample_files_dir_with_glob_pattern() -> None:
    fs = FakeFileSystem([])

    count_sample_files(fs)

    assert fs.list_calls == [(SAMPLE_FILES_DIR, 'sample_*.csv')]


def test_returns_zero_when_directory_empty() -> None:
    fs = FakeFileSystem([])

    assert count_sample_files(fs) == 0
