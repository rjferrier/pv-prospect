"""Tests for WorkflowOrchestrator.filter_remaining_tasks."""

from pv_prospect.etl import WorkflowOrchestrator

from ..helpers import FakeFileSystem


def _task(task_hash: str | None) -> list[dict[str, str]]:
    env = [{'name': 'JOB_TYPE', 'value': 'extract_and_load'}]
    if task_hash is not None:
        env.append({'name': 'TASK_HASH', 'value': task_hash})
    return env


def test_skips_tasks_with_existing_checkpoint() -> None:
    fs = FakeFileSystem(
        {
            'checkpoints/wf/2025-06-24/done.json': '{"status": "completed"}',
        }
    )
    orchestrator = WorkflowOrchestrator(fs, 'wf', '2025-06-24')

    remaining = orchestrator.filter_remaining_tasks([_task('done'), _task('pending')])

    hashes = [
        next(e['value'] for e in t if e['name'] == 'TASK_HASH') for t in remaining
    ]
    assert hashes == ['pending']


def test_returns_all_tasks_when_no_checkpoints() -> None:
    fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(fs, 'wf', '2025-06-24')

    remaining = orchestrator.filter_remaining_tasks([_task('a'), _task('b')])

    assert len(remaining) == 2


def test_includes_tasks_without_task_hash() -> None:
    fs = FakeFileSystem({'checkpoints/wf/2025-06-24/a.json': 'x'})
    orchestrator = WorkflowOrchestrator(fs, 'wf', '2025-06-24')

    remaining = orchestrator.filter_remaining_tasks([_task(None), _task('a')])

    assert remaining == [_task(None)]


def test_preserves_input_order() -> None:
    fs = FakeFileSystem({'checkpoints/wf/2025-06-24/b.json': 'x'})
    orchestrator = WorkflowOrchestrator(fs, 'wf', '2025-06-24')

    remaining = orchestrator.filter_remaining_tasks(
        [_task('a'), _task('b'), _task('c')]
    )

    hashes = [
        next(e['value'] for e in t if e['name'] == 'TASK_HASH') for t in remaining
    ]
    assert hashes == ['a', 'c']


def test_only_reads_own_workflow_checkpoints() -> None:
    fs = FakeFileSystem(
        {
            'checkpoints/other-wf/2025-06-24/a.json': 'x',
        }
    )
    orchestrator = WorkflowOrchestrator(fs, 'wf', '2025-06-24')

    remaining = orchestrator.filter_remaining_tasks([_task('a')])

    assert len(remaining) == 1
