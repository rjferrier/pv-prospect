"""Tests for WorkflowOrchestrator.filter_remaining_tasks."""

import json

from pv_prospect.etl import WorkflowOrchestrator

from ..helpers import FakeFileSystem


def _task(task_hash: str | None) -> list[dict[str, str]]:
    env = [{'name': 'JOB_TYPE', 'value': 'extract_and_load'}]
    if task_hash is not None:
        env.append({'name': 'TASK_HASH', 'value': task_hash})
    return env


def _ledger_line(task_hash: str, status: str) -> str:
    return (
        json.dumps(
            {
                'recorded_at': '2025-06-24T10:30:00+00:00',
                'run_date': '2025-06-24',
                'workflow': 'wf',
                'task_hash': task_hash,
                'descriptor': {},
                'status': status,
            }
        )
        + '\n'
    )


def test_skips_tasks_with_completed_per_task_entry() -> None:
    log_fs = FakeFileSystem(
        {'2025-06-24/wf/done.jsonl': _ledger_line('done', 'completed')}
    )
    resources_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    remaining = orchestrator.filter_remaining_tasks([_task('done'), _task('pending')])

    hashes = [
        next(e['value'] for e in t if e['name'] == 'TASK_HASH') for t in remaining
    ]
    assert hashes == ['pending']


def test_includes_tasks_with_only_failed_entry() -> None:
    log_fs = FakeFileSystem({'2025-06-24/wf/abc.jsonl': _ledger_line('abc', 'failed')})
    resources_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    remaining = orchestrator.filter_remaining_tasks([_task('abc')])

    assert len(remaining) == 1


def test_skips_tasks_with_failed_then_completed() -> None:
    multi = _ledger_line('abc', 'failed') + _ledger_line('abc', 'completed')
    log_fs = FakeFileSystem({'2025-06-24/wf/abc.jsonl': multi})
    resources_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    remaining = orchestrator.filter_remaining_tasks([_task('abc')])

    assert remaining == []


def test_skips_tasks_with_completed_in_consolidated_ledger() -> None:
    log_fs = FakeFileSystem(
        {'2025-06-24/2025-06-24-110000-wf.jsonl': _ledger_line('done', 'completed')}
    )
    resources_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    remaining = orchestrator.filter_remaining_tasks([_task('done'), _task('pending')])

    hashes = [
        next(e['value'] for e in t if e['name'] == 'TASK_HASH') for t in remaining
    ]
    assert hashes == ['pending']


def test_returns_all_tasks_when_log_fs_is_none() -> None:
    resources_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24')

    remaining = orchestrator.filter_remaining_tasks([_task('a'), _task('b')])

    assert len(remaining) == 2


def test_returns_all_tasks_when_ledger_empty() -> None:
    log_fs = FakeFileSystem()
    resources_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    remaining = orchestrator.filter_remaining_tasks([_task('a'), _task('b')])

    assert len(remaining) == 2


def test_includes_tasks_without_task_hash() -> None:
    log_fs = FakeFileSystem({'2025-06-24/wf/a.jsonl': _ledger_line('a', 'completed')})
    resources_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    remaining = orchestrator.filter_remaining_tasks([_task(None), _task('a')])

    assert remaining == [_task(None)]


def test_preserves_input_order() -> None:
    log_fs = FakeFileSystem({'2025-06-24/wf/b.jsonl': _ledger_line('b', 'completed')})
    resources_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    remaining = orchestrator.filter_remaining_tasks(
        [_task('a'), _task('b'), _task('c')]
    )

    hashes = [
        next(e['value'] for e in t if e['name'] == 'TASK_HASH') for t in remaining
    ]
    assert hashes == ['a', 'c']


def test_only_reads_own_workflow_ledger() -> None:
    log_fs = FakeFileSystem(
        {'2025-06-24/other-wf/a.jsonl': _ledger_line('a', 'completed')}
    )
    resources_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    remaining = orchestrator.filter_remaining_tasks([_task('a')])

    assert len(remaining) == 1


def test_only_reads_own_run_date_consolidated_ledger() -> None:
    log_fs = FakeFileSystem(
        {
            '2025-06-23/2025-06-23-110000-wf.jsonl': _ledger_line('a', 'completed'),
        }
    )
    resources_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(resources_fs, 'wf', '2025-06-24', log_fs=log_fs)

    remaining = orchestrator.filter_remaining_tasks([_task('a')])

    assert len(remaining) == 1
