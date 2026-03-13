"""Tests for LocalFileSystem."""

from pathlib import Path

import pytest
from pv_prospect.etl.storage.backends.local import LocalFileSystem


def test_exists_returns_false_for_missing_file(tmp_path: Path) -> None:
    fs = LocalFileSystem(str(tmp_path))

    assert fs.exists('missing.txt') is False


def test_exists_returns_true_for_existing_file(tmp_path: Path) -> None:
    (tmp_path / 'present.txt').write_text('hello')
    fs = LocalFileSystem(str(tmp_path))

    assert fs.exists('present.txt') is True


def test_read_text_returns_file_content(tmp_path: Path) -> None:
    (tmp_path / 'data.txt').write_text('content', encoding='utf-8')
    fs = LocalFileSystem(str(tmp_path))

    assert fs.read_text('data.txt') == 'content'


def test_read_text_raises_on_missing_file(tmp_path: Path) -> None:
    fs = LocalFileSystem(str(tmp_path))

    with pytest.raises(FileNotFoundError):
        fs.read_text('missing.txt')


def test_write_text_creates_file(tmp_path: Path) -> None:
    fs = LocalFileSystem(str(tmp_path))

    fs.write_text('output.txt', 'hello')

    assert (tmp_path / 'output.txt').read_text(encoding='utf-8') == 'hello'


def test_write_text_creates_parent_directories(tmp_path: Path) -> None:
    fs = LocalFileSystem(str(tmp_path))

    fs.write_text('sub/dir/output.txt', 'nested')

    assert (tmp_path / 'sub' / 'dir' / 'output.txt').read_text(
        encoding='utf-8'
    ) == 'nested'


def test_mkdir_creates_directory(tmp_path: Path) -> None:
    fs = LocalFileSystem(str(tmp_path))

    fs.mkdir('new/sub')

    assert (tmp_path / 'new' / 'sub').is_dir()


def test_list_files_returns_file_metadata(tmp_path: Path) -> None:
    (tmp_path / 'a.csv').write_text('data')
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('')

    assert len(results) == 1
    assert results[0].name == 'a.csv'
    assert results[0].path == 'a.csv'


def test_list_files_returns_empty_for_missing_dir(tmp_path: Path) -> None:
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('nonexistent')

    assert results == []


def test_list_files_filters_by_prefix(tmp_path: Path) -> None:
    (tmp_path / 'data').mkdir()
    (tmp_path / 'data' / 'a.csv').write_text('1')
    (tmp_path / 'other').mkdir()
    (tmp_path / 'other' / 'b.csv').write_text('2')
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('data')

    names = [r.name for r in results]
    assert names == ['a.csv']


def test_list_files_filters_by_pattern(tmp_path: Path) -> None:
    (tmp_path / 'a.csv').write_text('1')
    (tmp_path / 'b.txt').write_text('2')
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('', pattern='*.csv')

    names = [r.name for r in results]
    assert names == ['a.csv']


def test_list_files_recursive_finds_nested(tmp_path: Path) -> None:
    (tmp_path / 'sub').mkdir()
    (tmp_path / 'sub' / 'nested.csv').write_text('1')
    (tmp_path / 'top.csv').write_text('2')
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('', recursive=True)

    names = sorted(r.name for r in results)
    assert names == ['nested.csv', 'top.csv']


def test_list_files_includes_parent_path(tmp_path: Path) -> None:
    (tmp_path / 'sub').mkdir()
    (tmp_path / 'sub' / 'file.csv').write_text('1')
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('sub')

    assert results[0].parent_path == 'sub'


def test_constructor_creates_base_dir(tmp_path: Path) -> None:
    base = tmp_path / 'new' / 'base'

    LocalFileSystem(str(base))

    assert base.is_dir()
