"""Tests for WorkflowOrchestrator.write_manifest."""

import json

import pytest
from pv_prospect.etl import WorkflowOrchestrator

from ..helpers import FakeFileSystem


def test_writes_manifest_at_expected_path() -> None:
    manifests_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', manifests_fs=manifests_fs)

    orchestrator.write_manifest([])

    assert '2025-06-24/wf.json' in manifests_fs._files


def test_serialises_phases_under_phases_key() -> None:
    manifests_fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', manifests_fs=manifests_fs)
    phases = [
        [[{'name': 'JOB_TYPE', 'value': 'extract_and_load'}]],
        [[{'name': 'JOB_TYPE', 'value': 'consolidate_logs'}]],
    ]

    orchestrator.write_manifest(phases)

    contents = json.loads(manifests_fs._files['2025-06-24/wf.json'])
    assert contents == {'phases': phases}


def test_overwrites_existing_manifest() -> None:
    manifests_fs = FakeFileSystem({'2025-06-24/wf.json': 'stale'})
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24', manifests_fs=manifests_fs)

    orchestrator.write_manifest([])

    assert json.loads(manifests_fs._files['2025-06-24/wf.json']) == {'phases': []}


def test_raises_without_manifests_fs() -> None:
    orchestrator = WorkflowOrchestrator('wf', '2025-06-24')

    with pytest.raises(RuntimeError):
        orchestrator.write_manifest([])
