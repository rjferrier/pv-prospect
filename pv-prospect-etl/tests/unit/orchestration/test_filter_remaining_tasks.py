"""Tests for WorkflowOrchestrator.filter_remaining_tasks."""

import json

from pv_prospect.etl import WorkflowOrchestrator
from pv_prospect.etl.orchestration import compute_task_hash

from ..helpers import FakeFileSystem


def _env(label: str) -> list[dict[str, str]]:
    return [
        {'name': 'JOB_TYPE', 'value': 'extract_and_load'},
        {'name': 'LABEL', 'value': label},
    ]


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


def test_skips_tasks_with_completed_per_task_entry() -> None:
    done = _env('done')
    pending = _env('pending')
    done_hash = compute_task_hash(done)
    ledger_fs = FakeFileSystem(
        {f'2025-06-24/wf/{done_hash}.jsonl': _ledger_line(done_hash, 'completed')}
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.filter_remaining_tasks([done, pending]) == [pending]


def test_includes_tasks_with_only_failed_entry() -> None:
    abc = _env('abc')
    abc_hash = compute_task_hash(abc)
    ledger_fs = FakeFileSystem(
        {f'2025-06-24/wf/{abc_hash}.jsonl': _ledger_line(abc_hash, 'failed')}
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.filter_remaining_tasks([abc]) == [abc]


def test_skips_tasks_with_failed_then_completed() -> None:
    abc = _env('abc')
    abc_hash = compute_task_hash(abc)
    multi = _ledger_line(abc_hash, 'failed') + _ledger_line(abc_hash, 'completed')
    ledger_fs = FakeFileSystem({f'2025-06-24/wf/{abc_hash}.jsonl': multi})
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.filter_remaining_tasks([abc]) == []


def test_skips_tasks_with_completed_in_consolidated_ledger() -> None:
    done = _env('done')
    pending = _env('pending')
    done_hash = compute_task_hash(done)
    ledger_fs = FakeFileSystem(
        {'2025-06-24/2025-06-24-110000-wf.jsonl': _ledger_line(done_hash, 'completed')}
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.filter_remaining_tasks([done, pending]) == [pending]


def test_returns_all_tasks_when_ledger_fs_is_none() -> None:
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24')

    assert orchestrator.filter_remaining_tasks([_env('a'), _env('b')]) == [
        _env('a'),
        _env('b'),
    ]


def test_returns_all_tasks_when_ledger_empty() -> None:
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=FakeFileSystem())

    assert orchestrator.filter_remaining_tasks([_env('a'), _env('b')]) == [
        _env('a'),
        _env('b'),
    ]


def test_preserves_input_order() -> None:
    a, b, c = _env('a'), _env('b'), _env('c')
    b_hash = compute_task_hash(b)
    ledger_fs = FakeFileSystem(
        {f'2025-06-24/wf/{b_hash}.jsonl': _ledger_line(b_hash, 'completed')}
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.filter_remaining_tasks([a, b, c]) == [a, c]


def test_only_reads_own_workflow_per_task_ledger() -> None:
    a = _env('a')
    a_hash = compute_task_hash(a)
    ledger_fs = FakeFileSystem(
        {f'2025-06-24/other-wf/{a_hash}.jsonl': _ledger_line(a_hash, 'completed')}
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.filter_remaining_tasks([a]) == [a]


def test_consolidated_ledger_from_prior_run_date_skips_task() -> None:
    """Cross-day resume: a task completed on a previous run_date should
    not be re-executed today, even though today's per-task ledger dir is
    empty. Preserves same-data-window resume across re-triggers."""
    a = _env('a')
    a_hash = compute_task_hash(a)
    ledger_fs = FakeFileSystem(
        {
            '2025-06-23/2025-06-23-110000-wf.jsonl': _ledger_line(
                a_hash, 'completed', run_date='2025-06-23'
            ),
        }
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.filter_remaining_tasks([a]) == []


def test_consolidated_ledger_from_other_workflow_does_not_skip() -> None:
    a = _env('a')
    a_hash = compute_task_hash(a)
    ledger_fs = FakeFileSystem(
        {
            '2025-06-23/2025-06-23-110000-other.jsonl': _ledger_line(
                a_hash, 'completed', run_date='2025-06-23'
            ),
        }
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.filter_remaining_tasks([a]) == [a]


def test_ignores_pre_injected_task_hash_env_entry() -> None:
    """A pre-injected TASK_HASH on the env is irrelevant to filtering —
    the hash is computed from the env's other contents, so an env that
    happens to carry a literal 'TASK_HASH' value pointing at a completed
    entry is *not* skipped on that basis alone."""
    spurious = _env('a') + [{'name': 'TASK_HASH', 'value': 'completed-hash'}]
    ledger_fs = FakeFileSystem(
        {
            '2025-06-24/wf/completed-hash.jsonl': _ledger_line(
                'completed-hash', 'completed'
            )
        }
    )
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', ledger_fs=ledger_fs)

    assert orchestrator.filter_remaining_tasks([spurious]) == [spurious]
