"""Tests for LoggingFileSystem."""

from datetime import datetime, timezone

from pv_prospect.etl.storage.logging_fs import LoggingFileSystem

from ...helpers import FakeFileSystem

FIXED_TIME = datetime(2025, 6, 24, 10, 30, 15, 123456, tzinfo=timezone.utc)


def _fixed_now() -> datetime:
    return FIXED_TIME


def test_write_text_delegates_to_inner() -> None:
    inner = FakeFileSystem()
    log_fs = FakeFileSystem()
    fs = LoggingFileSystem(inner, log_fs, 'my-workflow', 'raw', now=_fixed_now)

    fs.write_text('data/file.csv', 'content')

    assert inner._files['data/file.csv'] == 'content'


def test_write_text_creates_log_entry() -> None:
    inner = FakeFileSystem()
    log_fs = FakeFileSystem()
    fs = LoggingFileSystem(inner, log_fs, 'my-workflow', 'raw', now=_fixed_now)

    fs.write_text('data/file.csv', 'content')

    expected_path = '2025-06-24/my-workflow/103015123456.txt'
    assert expected_path in log_fs._files
    assert log_fs._files[expected_path] == (
        '2025-06-24T10:30:15.123456+00:00 raw/data/file.csv\n'
    )


def test_write_bytes_delegates_to_inner() -> None:
    inner = FakeFileSystem()
    log_fs = FakeFileSystem()
    fs = LoggingFileSystem(inner, log_fs, 'my-workflow', 'raw', now=_fixed_now)

    fs.write_bytes('data/file.bin', b'\x00\x01')

    assert inner._binary_files['data/file.bin'] == b'\x00\x01'


def test_write_bytes_creates_log_entry() -> None:
    inner = FakeFileSystem()
    log_fs = FakeFileSystem()
    fs = LoggingFileSystem(inner, log_fs, 'my-workflow', 'raw', now=_fixed_now)

    fs.write_bytes('data/file.bin', b'\x00\x01')

    expected_path = '2025-06-24/my-workflow/103015123456.txt'
    assert expected_path in log_fs._files
    assert 'raw/data/file.bin' in log_fs._files[expected_path]


def test_read_operations_do_not_create_log_entries() -> None:
    inner = FakeFileSystem({'existing.txt': 'hello'})
    log_fs = FakeFileSystem()
    fs = LoggingFileSystem(inner, log_fs, 'my-workflow', 'raw', now=_fixed_now)

    fs.read_text('existing.txt')
    fs.exists('existing.txt')

    assert len(log_fs._files) == 0


def test_delete_does_not_create_log_entry() -> None:
    inner = FakeFileSystem({'file.txt': 'data'})
    log_fs = FakeFileSystem()
    fs = LoggingFileSystem(inner, log_fs, 'my-workflow', 'raw', now=_fixed_now)

    fs.delete('file.txt')

    assert len(log_fs._files) == 0


def test_mkdir_does_not_create_log_entry() -> None:
    inner = FakeFileSystem()
    log_fs = FakeFileSystem()
    fs = LoggingFileSystem(inner, log_fs, 'my-workflow', 'raw', now=_fixed_now)

    fs.mkdir('some/dir')

    assert len(log_fs._files) == 0


def test_label_is_prepended_to_logged_path() -> None:
    inner = FakeFileSystem()
    log_fs = FakeFileSystem()
    fs = LoggingFileSystem(inner, log_fs, 'my-workflow', 'cleaned', now=_fixed_now)

    fs.write_text('timeseries/data.csv', 'content')

    expected_path = '2025-06-24/my-workflow/103015123456.txt'
    assert 'cleaned/timeseries/data.csv' in log_fs._files[expected_path]


def test_empty_label_logs_path_without_prefix() -> None:
    inner = FakeFileSystem()
    log_fs = FakeFileSystem()
    fs = LoggingFileSystem(inner, log_fs, 'my-workflow', '', now=_fixed_now)

    fs.write_text('data.csv', 'content')

    expected_path = '2025-06-24/my-workflow/103015123456.txt'
    assert log_fs._files[expected_path] == (
        '2025-06-24T10:30:15.123456+00:00 data.csv\n'
    )


def test_log_entry_contains_iso_timestamp() -> None:
    inner = FakeFileSystem()
    log_fs = FakeFileSystem()
    fs = LoggingFileSystem(inner, log_fs, 'wf', 'raw', now=_fixed_now)

    fs.write_text('file.csv', 'content')

    log_content = next(iter(log_fs._files.values()))
    timestamp_part = log_content.split(' ')[0]
    assert timestamp_part == '2025-06-24T10:30:15.123456+00:00'


def test_log_failure_does_not_prevent_write() -> None:
    inner = FakeFileSystem()
    log_fs = FakeFileSystem()
    fs = LoggingFileSystem(inner, log_fs, 'wf', 'raw', now=_fixed_now)

    def failing_write(path: str, content: str) -> None:
        raise RuntimeError('log write failed')

    log_fs.write_text = failing_write  # type: ignore[assignment]

    fs.write_text('file.csv', 'content')

    assert inner._files['file.csv'] == 'content'


def test_str_delegates_to_inner() -> None:
    inner = FakeFileSystem()
    log_fs = FakeFileSystem()
    fs = LoggingFileSystem(inner, log_fs, 'wf', 'raw', now=_fixed_now)

    assert str(fs) == str(inner)


def test_list_files_delegates_to_inner() -> None:
    inner = FakeFileSystem({'dir/a.csv': 'a', 'dir/b.csv': 'b'})
    log_fs = FakeFileSystem()
    fs = LoggingFileSystem(inner, log_fs, 'wf', 'raw', now=_fixed_now)

    result = fs.list_files('dir', '*.csv')

    assert len(result) == 2


def test_multiple_writes_with_different_timestamps() -> None:
    inner = FakeFileSystem()
    log_fs = FakeFileSystem()

    times = iter(
        [
            datetime(2025, 6, 24, 10, 30, 15, 0, tzinfo=timezone.utc),
            datetime(2025, 6, 24, 10, 30, 15, 500000, tzinfo=timezone.utc),
        ]
    )
    fs = LoggingFileSystem(inner, log_fs, 'wf', 'raw', now=lambda: next(times))

    fs.write_text('a.csv', 'a')
    fs.write_text('b.csv', 'b')

    assert '2025-06-24/wf/103015000000.txt' in log_fs._files
    assert '2025-06-24/wf/103015500000.txt' in log_fs._files
