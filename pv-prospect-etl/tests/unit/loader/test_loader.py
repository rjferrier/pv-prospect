"""Tests for Loader."""

import pytest
from pv_prospect.etl import Loader

from ..helpers import FakeFileSystem


def test_write_csv_serializes_rows() -> None:
    fs = FakeFileSystem()
    loader = Loader(fs)

    loader.write_csv('out.csv', [['a', 'b'], ['1', '2']])

    assert 'out.csv' in fs._files
    assert 'a,b' in fs._files['out.csv']
    assert '1,2' in fs._files['out.csv']


def test_write_csv_raises_on_existing_file() -> None:
    fs = FakeFileSystem({'out.csv': 'old'})
    loader = Loader(fs)

    with pytest.raises(FileExistsError):
        loader.write_csv('out.csv', [['x']])


def test_write_csv_overwrites_when_flag_set() -> None:
    fs = FakeFileSystem({'out.csv': 'old'})
    loader = Loader(fs)

    loader.write_csv('out.csv', [['new']], overwrite=True)

    assert 'new' in fs._files['out.csv']


def test_write_text_writes_content() -> None:
    fs = FakeFileSystem()
    loader = Loader(fs)

    loader.write_text('note.txt', 'hello')

    assert fs._files['note.txt'] == 'hello'


def test_write_text_raises_on_existing_file() -> None:
    fs = FakeFileSystem({'note.txt': 'old'})
    loader = Loader(fs)

    with pytest.raises(FileExistsError):
        loader.write_text('note.txt', 'new')


def test_write_text_overwrites_when_flag_set() -> None:
    fs = FakeFileSystem({'note.txt': 'old'})
    loader = Loader(fs)

    loader.write_text('note.txt', 'new', overwrite=True)

    assert fs._files['note.txt'] == 'new'


def test_create_folder_delegates_mkdir() -> None:
    fs = FakeFileSystem()
    loader = Loader(fs)

    loader.create_folder('data/raw')

    assert 'data/raw' in fs.created_dirs


def test_create_folder_returns_path() -> None:
    fs = FakeFileSystem()
    loader = Loader(fs)

    result = loader.create_folder('data/raw')

    assert result == 'data/raw'


def test_file_exists_true_when_present() -> None:
    fs = FakeFileSystem({'present.txt': ''})
    loader = Loader(fs)

    assert loader.file_exists('present.txt') is True


def test_file_exists_false_when_absent() -> None:
    fs = FakeFileSystem()
    loader = Loader(fs)

    assert loader.file_exists('absent.txt') is False
