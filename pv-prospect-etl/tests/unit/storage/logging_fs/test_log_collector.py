"""Tests for LogCollector."""

import threading
from datetime import datetime, timezone

from pv_prospect.etl.storage.logging_fs import LogCollector

from ...helpers import FakeFileSystem

FLUSH_TIME = datetime(2025, 6, 24, 11, 0, 0, tzinfo=timezone.utc)


def _flush_now() -> datetime:
    return FLUSH_TIME


def test_flush_writes_buffered_line_to_consolidated_path() -> None:
    collector = LogCollector('wf', '2025-06-24')
    collector.record('2025-06-24T10:30:15+00:00 CREATED cleaned/a.csv')

    log_fs = FakeFileSystem()
    collector.flush(log_fs, now=_flush_now)

    content = log_fs.read_text('2025-06-24/2025-06-24-110000-wf.txt')
    assert content == '2025-06-24T10:30:15+00:00 CREATED cleaned/a.csv\n'


def test_flush_sorts_lines() -> None:
    collector = LogCollector('wf', '2025-06-24')
    collector.record('2025-06-24T10:30:17+00:00 CREATED cleaned/c.csv')
    collector.record('2025-06-24T10:30:15+00:00 CREATED cleaned/a.csv')
    collector.record('2025-06-24T10:30:16+00:00 CREATED cleaned/b.csv')

    log_fs = FakeFileSystem()
    collector.flush(log_fs, now=_flush_now)

    content = log_fs.read_text('2025-06-24/2025-06-24-110000-wf.txt')
    assert content.splitlines() == [
        '2025-06-24T10:30:15+00:00 CREATED cleaned/a.csv',
        '2025-06-24T10:30:16+00:00 CREATED cleaned/b.csv',
        '2025-06-24T10:30:17+00:00 CREATED cleaned/c.csv',
    ]


def test_flush_is_noop_when_nothing_recorded() -> None:
    collector = LogCollector('wf', '2025-06-24')

    log_fs = FakeFileSystem()
    collector.flush(log_fs, now=_flush_now)

    assert log_fs._files == {}


def test_flush_path_includes_run_label() -> None:
    collector = LogCollector('wf', '2025-06-24', run_label='run1')
    collector.record('2025-06-24T10:30:15+00:00 CREATED cleaned/a.csv')

    log_fs = FakeFileSystem()
    collector.flush(log_fs, now=_flush_now)

    assert '2025-06-24/2025-06-24-110000-run1-wf.txt' in log_fs._files


def test_concurrent_record_buffers_every_line() -> None:
    collector = LogCollector('wf', '2025-06-24')

    def record(index: int) -> None:
        collector.record(f'2025-06-24T10:30:15+00:00 CREATED cleaned/file{index}.csv')

    threads = [threading.Thread(target=record, args=(i,)) for i in range(200)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    log_fs = FakeFileSystem()
    collector.flush(log_fs, now=_flush_now)

    content = log_fs.read_text('2025-06-24/2025-06-24-110000-wf.txt')
    assert len(content.splitlines()) == 200
