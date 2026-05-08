"""Tests for WorkflowOrchestrator.record_outcome."""

import json
from datetime import datetime, timezone

from pv_prospect.etl import WorkflowOrchestrator

from ..helpers import FakeFileSystem

FIXED_TIME = datetime(2025, 6, 24, 10, 30, 15, 123456, tzinfo=timezone.utc)


def _fixed_now() -> datetime:
    return FIXED_TIME


def _read_entries(log_fs: FakeFileSystem, path: str) -> list[dict]:
    return [json.loads(line) for line in log_fs.read_text(path).splitlines() if line]


def test_appends_completed_entry() -> None:
    resources_fs = FakeFileSystem()
    log_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    orchestrator.record_outcome(
        'abc',
        {'system_id': '89665', 'date': '2025-06-24'},
        'completed',
        now=_fixed_now,
    )

    entries = _read_entries(log_fs, '2025-06-24/wf/abc.jsonl')
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
    resources_fs = FakeFileSystem()
    log_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    orchestrator.record_outcome(
        'abc',
        {'system_id': '89665'},
        'failed',
        error='HTTPError(503)',
        now=_fixed_now,
    )

    entries = _read_entries(log_fs, '2025-06-24/wf/abc.jsonl')
    assert entries[0]['status'] == 'failed'
    assert entries[0]['error'] == 'HTTPError(503)'


def test_completed_is_idempotent() -> None:
    resources_fs = FakeFileSystem()
    log_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    orchestrator.record_outcome('abc', {}, 'completed', now=_fixed_now)
    orchestrator.record_outcome('abc', {}, 'completed', now=_fixed_now)

    entries = _read_entries(log_fs, '2025-06-24/wf/abc.jsonl')
    assert len(entries) == 1


def test_failure_then_completed_appends_both() -> None:
    resources_fs = FakeFileSystem()
    log_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    orchestrator.record_outcome('abc', {}, 'failed', error='boom', now=_fixed_now)
    orchestrator.record_outcome('abc', {}, 'completed', now=_fixed_now)

    entries = _read_entries(log_fs, '2025-06-24/wf/abc.jsonl')
    statuses = [e['status'] for e in entries]
    assert statuses == ['failed', 'completed']


def test_multiple_failures_accumulate() -> None:
    resources_fs = FakeFileSystem()
    log_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    orchestrator.record_outcome('abc', {}, 'failed', error='first', now=_fixed_now)
    orchestrator.record_outcome('abc', {}, 'failed', error='second', now=_fixed_now)

    entries = _read_entries(log_fs, '2025-06-24/wf/abc.jsonl')
    errors = [e['error'] for e in entries]
    assert errors == ['first', 'second']


def test_noop_without_log_fs() -> None:
    resources_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24')

    orchestrator.record_outcome('abc', {}, 'completed', now=_fixed_now)

    assert resources_fs._files == {}


def test_noop_without_task_hash() -> None:
    resources_fs = FakeFileSystem()
    log_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    orchestrator.record_outcome('', {}, 'completed', now=_fixed_now)

    assert log_fs._files == {}


def test_descriptor_is_copied_not_referenced() -> None:
    resources_fs = FakeFileSystem()
    log_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    descriptor = {'system_id': '89665'}
    orchestrator.record_outcome('abc', descriptor, 'completed', now=_fixed_now)
    descriptor['system_id'] = 'mutated'

    entries = _read_entries(log_fs, '2025-06-24/wf/abc.jsonl')
    assert entries[0]['descriptor'] == {'system_id': '89665'}
