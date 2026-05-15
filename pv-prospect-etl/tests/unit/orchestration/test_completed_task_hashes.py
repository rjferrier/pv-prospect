"""Tests for WorkflowOrchestrator.completed_task_hashes."""

import json

from pv_prospect.etl import WorkflowOrchestrator

from ..helpers import FakeFileSystem


def _ledger_line(task_hash: str, status: str, run_date: str = '2025-06-24') -> str:
    return (
        json.dumps(
            {
                'recorded_at': f'{run_date}T10:30:00+00:00',
                'run_date': run_date,
                'workflow': 'wf',
                'task_hash': task_hash,
                'descriptor': {},
                'status': status,
            }
        )
        + '\n'
    )


def test_returns_empty_set_when_ledger_fs_is_none() -> None:
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24')

    assert orchestrator.completed_task_hashes() == set()


def test_returns_empty_set_when_ledger_empty() -> None:
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=FakeFileSystem())

    assert orchestrator.completed_task_hashes() == set()


def test_includes_per_task_completed_entry() -> None:
    ledger_fs = FakeFileSystem(
        {'2025-06-24/wf/abc.jsonl': _ledger_line('abc', 'completed')}
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.completed_task_hashes() == {'abc'}


def test_excludes_per_task_failed_only_entry() -> None:
    ledger_fs = FakeFileSystem(
        {'2025-06-24/wf/abc.jsonl': _ledger_line('abc', 'failed')}
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.completed_task_hashes() == set()


def test_includes_per_task_failed_then_completed() -> None:
    multi = _ledger_line('abc', 'failed') + _ledger_line('abc', 'completed')
    ledger_fs = FakeFileSystem({'2025-06-24/wf/abc.jsonl': multi})
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.completed_task_hashes() == {'abc'}


def test_includes_consolidated_completed_entry() -> None:
    ledger_fs = FakeFileSystem(
        {'2025-06-24/2025-06-24-110000-wf.jsonl': _ledger_line('done', 'completed')}
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.completed_task_hashes() == {'done'}


def test_includes_consolidated_from_prior_run_date() -> None:
    """Cross-day resume: a task completed on a previous run_date is
    still reported here today."""
    ledger_fs = FakeFileSystem(
        {
            '2025-06-23/2025-06-23-110000-wf.jsonl': _ledger_line(
                'a', 'completed', run_date='2025-06-23'
            ),
        }
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.completed_task_hashes() == {'a'}


def test_excludes_consolidated_from_other_workflow() -> None:
    ledger_fs = FakeFileSystem(
        {
            '2025-06-23/2025-06-23-110000-other.jsonl': _ledger_line(
                'a', 'completed', run_date='2025-06-23'
            ),
        }
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.completed_task_hashes() == set()


def test_excludes_per_task_from_other_workflow() -> None:
    ledger_fs = FakeFileSystem(
        {'2025-06-24/other-wf/a.jsonl': _ledger_line('a', 'completed')}
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.completed_task_hashes() == set()


def test_unions_per_task_and_consolidated_sources() -> None:
    ledger_fs = FakeFileSystem(
        {
            '2025-06-24/wf/today.jsonl': _ledger_line('today', 'completed'),
            '2025-06-23/2025-06-23-110000-wf.jsonl': _ledger_line(
                'yesterday', 'completed', run_date='2025-06-23'
            ),
        }
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.completed_task_hashes() == {'today', 'yesterday'}


def test_run_label_scopes_per_task_lookup_to_its_own_scratch_dir() -> None:
    """Within-run idempotency only needs the current run's own entries.

    Cross-run resumption flows through the consolidated layer, which
    ``list_consolidated_ledgers`` already covers — the per-task scan is
    only there for Cloud Run task retries within a single execution.
    """
    ledger_fs = FakeFileSystem(
        {
            '2025-06-24/wf/run1/a.jsonl': _ledger_line('a', 'completed'),
            '2025-06-24/wf/run2/b.jsonl': _ledger_line('b', 'completed'),
        }
    )
    orchestrator = WorkflowOrchestrator(
        'wf', '2025-06-24', ledger_fs=ledger_fs, run_label='run1'
    )

    assert orchestrator.completed_task_hashes() == {'a'}
