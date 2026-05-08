"""Tests for WorkflowOrchestrator.mark_task_completed."""

import json

from pv_prospect.etl import WorkflowOrchestrator

from ..helpers import FakeFileSystem


def test_writes_checkpoint_at_expected_path() -> None:
    fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(fs, 'wf', '2025-06-24')

    orchestrator.mark_task_completed('abc')

    assert fs._files['checkpoints/wf/2025-06-24/abc.json'] == json.dumps(
        {'status': 'completed'}
    )


def test_noop_when_task_hash_empty() -> None:
    fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(fs, 'wf', '2025-06-24')

    orchestrator.mark_task_completed('')

    assert fs._files == {}


def test_does_not_overwrite_existing_checkpoint() -> None:
    fs = FakeFileSystem({'checkpoints/wf/2025-06-24/abc.json': 'sentinel'})
    orchestrator = WorkflowOrchestrator(fs, 'wf', '2025-06-24')

    orchestrator.mark_task_completed('abc')

    assert fs._files['checkpoints/wf/2025-06-24/abc.json'] == 'sentinel'
