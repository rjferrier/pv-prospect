"""Tests for LedgerCollector."""

import json
import threading
from datetime import datetime, timezone

from pv_prospect.etl.storage.ledger import LedgerCollector

from ...helpers import FakeFileSystem

FLUSH_TIME = datetime(2025, 6, 24, 11, 0, 0, tzinfo=timezone.utc)


def _flush_now() -> datetime:
    return FLUSH_TIME


def _entry(
    recorded_at: str, task_hash: str, status: str, error: str | None = None
) -> dict[str, object]:
    entry: dict[str, object] = {
        'recorded_at': recorded_at,
        'run_date': '2025-06-24',
        'workflow': 'wf',
        'task_hash': task_hash,
        'descriptor': {'system_id': '89665'},
        'status': status,
    }
    if error is not None:
        entry['error'] = error
    return entry


def _read_entries(ledger_fs: FakeFileSystem, path: str) -> list[dict]:
    return [json.loads(line) for line in ledger_fs.read_text(path).splitlines() if line]


def test_flush_writes_buffered_entry_to_consolidated_path() -> None:
    collector = LedgerCollector('wf', '2025-06-24')
    collector.record(_entry('2025-06-24T10:30:15+00:00', 'aaa', 'completed'))

    ledger_fs = FakeFileSystem()
    collector.flush(ledger_fs, now=_flush_now)

    entries = _read_entries(ledger_fs, '2025-06-24/2025-06-24-110000-wf.jsonl')
    assert len(entries) == 1
    assert entries[0]['task_hash'] == 'aaa'
    assert entries[0]['status'] == 'completed'


def test_flush_sorts_entries_by_recorded_at() -> None:
    collector = LedgerCollector('wf', '2025-06-24')
    collector.record(_entry('2025-06-24T10:30:17+00:00', 'ccc', 'completed'))
    collector.record(_entry('2025-06-24T10:30:15+00:00', 'aaa', 'completed'))
    collector.record(_entry('2025-06-24T10:30:16+00:00', 'bbb', 'completed'))

    ledger_fs = FakeFileSystem()
    collector.flush(ledger_fs, now=_flush_now)

    entries = _read_entries(ledger_fs, '2025-06-24/2025-06-24-110000-wf.jsonl')
    assert [e['task_hash'] for e in entries] == ['aaa', 'bbb', 'ccc']


def test_completed_is_idempotent() -> None:
    collector = LedgerCollector('wf', '2025-06-24')
    collector.record(_entry('2025-06-24T10:30:15+00:00', 'aaa', 'completed'))
    collector.record(_entry('2025-06-24T10:30:16+00:00', 'aaa', 'completed'))

    ledger_fs = FakeFileSystem()
    collector.flush(ledger_fs, now=_flush_now)

    entries = _read_entries(ledger_fs, '2025-06-24/2025-06-24-110000-wf.jsonl')
    assert len(entries) == 1


def test_failed_entries_accumulate() -> None:
    collector = LedgerCollector('wf', '2025-06-24')
    collector.record(_entry('2025-06-24T10:30:15+00:00', 'aaa', 'failed', 'first'))
    collector.record(_entry('2025-06-24T10:30:16+00:00', 'aaa', 'failed', 'second'))

    ledger_fs = FakeFileSystem()
    collector.flush(ledger_fs, now=_flush_now)

    entries = _read_entries(ledger_fs, '2025-06-24/2025-06-24-110000-wf.jsonl')
    assert [e['error'] for e in entries] == ['first', 'second']


def test_failure_then_completed_keeps_both() -> None:
    collector = LedgerCollector('wf', '2025-06-24')
    collector.record(_entry('2025-06-24T10:30:15+00:00', 'aaa', 'failed', 'boom'))
    collector.record(_entry('2025-06-24T10:30:16+00:00', 'aaa', 'completed'))

    ledger_fs = FakeFileSystem()
    collector.flush(ledger_fs, now=_flush_now)

    entries = _read_entries(ledger_fs, '2025-06-24/2025-06-24-110000-wf.jsonl')
    assert [e['status'] for e in entries] == ['failed', 'completed']


def test_flush_is_noop_when_nothing_recorded() -> None:
    collector = LedgerCollector('wf', '2025-06-24')

    ledger_fs = FakeFileSystem()
    collector.flush(ledger_fs, now=_flush_now)

    assert ledger_fs._files == {}


def test_flush_path_includes_run_label() -> None:
    collector = LedgerCollector('wf', '2025-06-24', run_label='run1')
    collector.record(_entry('2025-06-24T10:30:15+00:00', 'aaa', 'completed'))

    ledger_fs = FakeFileSystem()
    collector.flush(ledger_fs, now=_flush_now)

    assert '2025-06-24/2025-06-24-110000-run1-wf.jsonl' in ledger_fs._files


def test_flush_per_task_writes_to_scratch_path() -> None:
    """The per-task flush mode writes one file containing every buffered
    entry to the scratch directory, named for the *task*'s hash. The
    workflow-end ``consolidate_ledger`` step then merges this file with
    peer tasks' into one daily consolidated file."""
    collector = LedgerCollector('wf', '2025-06-24')
    collector.record(_entry('2025-06-24T10:30:15+00:00', 'site-a', 'completed'))
    collector.record(_entry('2025-06-24T10:30:16+00:00', 'site-b', 'failed', 'boom'))

    ledger_fs = FakeFileSystem()
    collector.flush_per_task(ledger_fs, task_hash='batch-1')

    entries = _read_entries(ledger_fs, '2025-06-24/wf/batch-1.jsonl')
    assert [e['task_hash'] for e in entries] == ['site-a', 'site-b']


def test_flush_per_task_path_includes_run_label() -> None:
    collector = LedgerCollector('wf', '2025-06-24', run_label='run1')
    collector.record(_entry('2025-06-24T10:30:15+00:00', 'site-a', 'completed'))

    ledger_fs = FakeFileSystem()
    collector.flush_per_task(ledger_fs, task_hash='batch-1')

    assert '2025-06-24/wf/run1/batch-1.jsonl' in ledger_fs._files


def test_flush_per_task_is_noop_when_nothing_recorded() -> None:
    collector = LedgerCollector('wf', '2025-06-24')

    ledger_fs = FakeFileSystem()
    collector.flush_per_task(ledger_fs, task_hash='batch-1')

    assert ledger_fs._files == {}


def test_concurrent_record_buffers_every_entry() -> None:
    collector = LedgerCollector('wf', '2025-06-24')

    def record(index: int) -> None:
        collector.record(
            _entry('2025-06-24T10:30:15+00:00', f'task{index}', 'completed')
        )

    threads = [threading.Thread(target=record, args=(i,)) for i in range(200)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    ledger_fs = FakeFileSystem()
    collector.flush(ledger_fs, now=_flush_now)

    entries = _read_entries(ledger_fs, '2025-06-24/2025-06-24-110000-wf.jsonl')
    assert len({e['task_hash'] for e in entries}) == 200
