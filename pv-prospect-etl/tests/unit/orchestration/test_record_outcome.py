"""Tests for WorkflowOrchestrator.record_outcome."""

import json
from datetime import datetime, timezone

from pv_prospect.etl import WorkflowOrchestrator
from pv_prospect.etl.storage import LedgerCollector

from ..helpers import FakeFileSystem

FIXED_TIME = datetime(2025, 6, 24, 10, 30, 15, 123456, tzinfo=timezone.utc)


def _fixed_now() -> datetime:
    return FIXED_TIME


def _read_entries(ledger_fs: FakeFileSystem, path: str) -> list[dict]:
    return [json.loads(line) for line in ledger_fs.read_text(path).splitlines() if line]


def test_appends_completed_entry() -> None:
    ledger_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    orchestrator.record_outcome(
        'abc',
        {'system_id': '89665', 'date': '2025-06-24'},
        'completed',
        now=_fixed_now,
    )

    entries = _read_entries(ledger_fs, '2025-06-24/wf/abc.jsonl')
    assert len(entries) == 1
    assert entries[0] == {
        'recorded_at': '2025-06-24T10:30:15.123456+00:00',
        'run_date': '2025-06-24',
        'workflow': 'wf',
        'task_hash': 'abc',
        'descriptor': {'system_id': '89665', 'date': '2025-06-24'},
        'status': 'completed',
    }


def test_appends_failed_entry_with_error() -> None:
    ledger_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    orchestrator.record_outcome(
        'abc',
        {'system_id': '89665'},
        'failed',
        error='HTTPError(503)',
        now=_fixed_now,
    )

    entries = _read_entries(ledger_fs, '2025-06-24/wf/abc.jsonl')
    assert entries[0]['status'] == 'failed'
    assert entries[0]['error'] == 'HTTPError(503)'


def test_completed_is_idempotent() -> None:
    ledger_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    orchestrator.record_outcome('abc', {}, 'completed', now=_fixed_now)
    orchestrator.record_outcome('abc', {}, 'completed', now=_fixed_now)

    entries = _read_entries(ledger_fs, '2025-06-24/wf/abc.jsonl')
    assert len(entries) == 1


def test_failure_then_completed_appends_both() -> None:
    ledger_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    orchestrator.record_outcome('abc', {}, 'failed', error='boom', now=_fixed_now)
    orchestrator.record_outcome('abc', {}, 'completed', now=_fixed_now)

    entries = _read_entries(ledger_fs, '2025-06-24/wf/abc.jsonl')
    statuses = [e['status'] for e in entries]
    assert statuses == ['failed', 'completed']


def test_multiple_failures_accumulate() -> None:
    ledger_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    orchestrator.record_outcome('abc', {}, 'failed', error='first', now=_fixed_now)
    orchestrator.record_outcome('abc', {}, 'failed', error='second', now=_fixed_now)

    entries = _read_entries(ledger_fs, '2025-06-24/wf/abc.jsonl')
    errors = [e['error'] for e in entries]
    assert errors == ['first', 'second']


def test_noop_without_ledger_fs() -> None:
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24')

    orchestrator.record_outcome('abc', {}, 'completed', now=_fixed_now)


def test_noop_without_task_hash() -> None:
    ledger_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    orchestrator.record_outcome('', {}, 'completed', now=_fixed_now)

    assert ledger_fs._files == {}


def test_descriptor_is_copied_not_referenced() -> None:
    ledger_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    descriptor = {'system_id': '89665'}
    orchestrator.record_outcome('abc', descriptor, 'completed', now=_fixed_now)
    descriptor['system_id'] = 'mutated'

    entries = _read_entries(ledger_fs, '2025-06-24/wf/abc.jsonl')
    assert entries[0]['descriptor'] == {'system_id': '89665'}


def test_run_label_namespaces_the_per_task_path() -> None:
    """Same-day run1/run2 executions write to disjoint per-task dirs."""
    ledger_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(
        'wf', '2025-06-24', ledger_fs=ledger_fs, run_label='run1'
    )

    orchestrator.record_outcome(
        'abc', {'system_id': '89665'}, 'completed', now=_fixed_now
    )

    entries = _read_entries(ledger_fs, '2025-06-24/wf/run1/abc.jsonl')
    assert entries[0]['task_hash'] == 'abc'
    assert '2025-06-24/wf/abc.jsonl' not in ledger_fs._files


def test_routes_to_ledger_collector_when_configured() -> None:
    collector = LedgerCollector('wf', '2025-06-24')
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_collector=collector)

    orchestrator.record_outcome(
        'abc', {'system_id': '89665'}, 'completed', now=_fixed_now
    )

    ledger_fs = FakeFileSystem()
    collector.flush(ledger_fs, now=_fixed_now)
    [path] = list(ledger_fs._files)
    entries = _read_entries(ledger_fs, path)
    assert entries[0]['task_hash'] == 'abc'
    assert entries[0]['status'] == 'completed'


def test_ledger_collector_writes_no_per_task_files() -> None:
    """With a collector, record_outcome buffers in memory and leaves the
    per-task ledger filesystem untouched."""
    ledger_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(
        'wf',
        '2025-06-24',
        ledger_fs=ledger_fs,
        ledger_collector=LedgerCollector('wf', '2025-06-24'),
    )

    orchestrator.record_outcome('abc', {}, 'completed', now=_fixed_now)

    assert ledger_fs._files == {}
