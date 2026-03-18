"""Tests for Extractor."""

import pytest
from pv_prospect.etl import Extractor

from ..helpers import FakeFileSystem


def test_read_file_returns_text_stream() -> None:
    fs = FakeFileSystem({'data.csv': 'a,b\n1,2'})
    extractor = Extractor(fs)

    with extractor.read_file('data.csv') as f:
        content = f.read()

    assert content == 'a,b\n1,2'


def test_read_file_raises_on_missing() -> None:
    fs = FakeFileSystem()
    extractor = Extractor(fs)

    with pytest.raises(FileNotFoundError):
        extractor.read_file('missing.csv')


def test_file_exists_true_when_present() -> None:
    fs = FakeFileSystem({'present.txt': ''})
    extractor = Extractor(fs)

    assert extractor.file_exists('present.txt') is True


def test_file_exists_false_when_absent() -> None:
    fs = FakeFileSystem({'present.txt': ''})
    extractor = Extractor(fs)

    assert extractor.file_exists('absent.txt') is False


def test_list_files_filters_by_folder() -> None:
    fs = FakeFileSystem({'data/a.csv': '', 'data/b.csv': '', 'other/c.csv': ''})
    extractor = Extractor(fs)

    results = extractor.list_files('data')

    names = [r.name for r in results]
    assert sorted(names) == ['a.csv', 'b.csv']


def test_list_files_with_none_folder_lists_all() -> None:
    fs = FakeFileSystem({'a.csv': '', 'b.csv': ''})
    extractor = Extractor(fs)

    results = extractor.list_files(None)

    assert len(results) == 2


def test_read_bytes_returns_binary_content() -> None:
    fs = FakeFileSystem(binary_files={'data.parquet': b'\x00\x01\x02'})
    extractor = Extractor(fs)

    assert extractor.read_bytes('data.parquet') == b'\x00\x01\x02'


def test_read_bytes_raises_on_missing() -> None:
    fs = FakeFileSystem()
    extractor = Extractor(fs)

    with pytest.raises(FileNotFoundError):
        extractor.read_bytes('missing.parquet')
