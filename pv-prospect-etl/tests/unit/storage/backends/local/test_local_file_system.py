"""Tests for LocalFileSystem."""

import pytest
from pv_prospect.etl.storage.backends import LocalFileSystem


def test_exists_returns_false_for_missing_file(tmp_path):
    fs = LocalFileSystem(str(tmp_path))

    assert fs.exists('missing.txt') is False


def test_exists_returns_true_for_existing_file(tmp_path):
    (tmp_path / 'present.txt').write_text('hello')
    fs = LocalFileSystem(str(tmp_path))

    assert fs.exists('present.txt') is True


def test_read_text_returns_file_content(tmp_path):
    (tmp_path / 'data.txt').write_text('content', encoding='utf-8')
    fs = LocalFileSystem(str(tmp_path))

    assert fs.read_text('data.txt') == 'content'


def test_read_text_raises_on_missing_file(tmp_path):
    fs = LocalFileSystem(str(tmp_path))

    with pytest.raises(FileNotFoundError):
        fs.read_text('missing.txt')


def test_write_text_creates_file(tmp_path):
    fs = LocalFileSystem(str(tmp_path))

    fs.write_text('output.txt', 'hello')

    assert (tmp_path / 'output.txt').read_text(encoding='utf-8') == 'hello'


def test_write_text_creates_parent_directories(tmp_path):
    fs = LocalFileSystem(str(tmp_path))

    fs.write_text('sub/dir/output.txt', 'nested')

    assert (tmp_path / 'sub' / 'dir' / 'output.txt').read_text(
        encoding='utf-8'
    ) == 'nested'


def test_append_text_creates_file_when_missing(tmp_path):
    fs = LocalFileSystem(str(tmp_path))

    fs.append_text('ledger.jsonl', 'line\n')

    assert (tmp_path / 'ledger.jsonl').read_text(encoding='utf-8') == 'line\n'


def test_append_text_appends_to_existing_file(tmp_path):
    (tmp_path / 'ledger.jsonl').write_text('first\n', encoding='utf-8')
    fs = LocalFileSystem(str(tmp_path))

    fs.append_text('ledger.jsonl', 'second\n')

    assert (tmp_path / 'ledger.jsonl').read_text(encoding='utf-8') == 'first\nsecond\n'


def test_append_text_creates_parent_directories(tmp_path):
    fs = LocalFileSystem(str(tmp_path))

    fs.append_text('sub/dir/ledger.jsonl', 'line\n')

    assert (tmp_path / 'sub' / 'dir' / 'ledger.jsonl').read_text(
        encoding='utf-8'
    ) == 'line\n'


def test_mkdir_creates_directory(tmp_path):
    fs = LocalFileSystem(str(tmp_path))

    fs.mkdir('new/sub')

    assert (tmp_path / 'new' / 'sub').is_dir()


def test_list_files_returns_file_metadata(tmp_path):
    (tmp_path / 'a.csv').write_text('data')
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('')

    assert len(results) == 1
    assert results[0].name == 'a.csv'
    assert results[0].path == 'a.csv'


def test_list_files_returns_empty_for_missing_dir(tmp_path):
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('nonexistent')

    assert results == []


def test_list_files_filters_by_prefix(tmp_path):
    (tmp_path / 'data').mkdir()
    (tmp_path / 'data' / 'a.csv').write_text('1')
    (tmp_path / 'other').mkdir()
    (tmp_path / 'other' / 'b.csv').write_text('2')
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('data')

    names = [r.name for r in results]
    assert names == ['a.csv']


def test_list_files_filters_by_pattern(tmp_path):
    (tmp_path / 'a.csv').write_text('1')
    (tmp_path / 'b.txt').write_text('2')
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('', pattern='*.csv')

    names = [r.name for r in results]
    assert names == ['a.csv']


def test_list_files_recursive_finds_nested(tmp_path):
    (tmp_path / 'sub').mkdir()
    (tmp_path / 'sub' / 'nested.csv').write_text('1')
    (tmp_path / 'top.csv').write_text('2')
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('', recursive=True)

    names = sorted(r.name for r in results)
    assert names == ['nested.csv', 'top.csv']


def test_list_files_recursive_pattern_matches_at_every_depth(tmp_path):
    """A recursive pattern matches on basename, whatever the depth.

    The GCS backend gets this from a ``**/<pattern>`` matchGlob, whose
    ``**/`` also matches no leading segments at all. The two backends
    must agree, since ``list_consolidated_ledgers`` relies on it.
    """
    (tmp_path / 'sub').mkdir()
    (tmp_path / 'sub' / 'nested.jsonl').write_text('1')
    (tmp_path / 'top.jsonl').write_text('2')
    (tmp_path / 'other.csv').write_text('3')
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('', '*.jsonl', recursive=True)

    assert sorted(r.name for r in results) == ['nested.jsonl', 'top.jsonl']


def test_list_files_start_offset_excludes_lexically_earlier_paths(tmp_path):
    for day in ('2026-05-13', '2026-05-14', '2026-05-15'):
        (tmp_path / day).mkdir()
        (tmp_path / day / f'{day}-ledger.jsonl').write_text('1')
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('', '*.jsonl', recursive=True, start_offset='2026-05-14')

    assert sorted(r.name for r in results) == [
        '2026-05-14-ledger.jsonl',
        '2026-05-15-ledger.jsonl',
    ]


def test_list_files_empty_start_offset_returns_everything(tmp_path):
    (tmp_path / 'a.csv').write_text('1')
    (tmp_path / 'b.csv').write_text('2')
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('', start_offset='')

    assert len(results) == 2


def test_list_files_includes_parent_path(tmp_path):
    (tmp_path / 'sub').mkdir()
    (tmp_path / 'sub' / 'file.csv').write_text('1')
    fs = LocalFileSystem(str(tmp_path))

    results = fs.list_files('sub')

    assert results[0].parent_path == 'sub'


def test_read_bytes_returns_file_content(tmp_path):
    (tmp_path / 'data.bin').write_bytes(b'\x00\x01\x02')
    fs = LocalFileSystem(str(tmp_path))

    assert fs.read_bytes('data.bin') == b'\x00\x01\x02'


def test_read_bytes_raises_on_missing_file(tmp_path):
    fs = LocalFileSystem(str(tmp_path))

    with pytest.raises(FileNotFoundError):
        fs.read_bytes('missing.bin')


def test_write_bytes_creates_file(tmp_path):
    fs = LocalFileSystem(str(tmp_path))

    fs.write_bytes('output.bin', b'\xfe\xff')

    assert (tmp_path / 'output.bin').read_bytes() == b'\xfe\xff'


def test_write_bytes_creates_parent_directories(tmp_path):
    fs = LocalFileSystem(str(tmp_path))

    fs.write_bytes('sub/dir/output.bin', b'\x01')

    assert (tmp_path / 'sub' / 'dir' / 'output.bin').read_bytes() == b'\x01'


def test_constructor_creates_base_dir(tmp_path):
    base = tmp_path / 'new' / 'base'

    LocalFileSystem(str(base))

    assert base.is_dir()
