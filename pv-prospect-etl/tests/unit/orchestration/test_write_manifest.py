"""Tests for WorkflowOrchestrator.write_manifest."""

import json

from pv_prospect.etl import WorkflowOrchestrator

from ..helpers import FakeFileSystem


def test_writes_manifest_at_expected_path() -> None:
    fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(fs, 'wf', '2025-06-24')

    orchestrator.write_manifest([])

    assert 'manifests/wf_2025-06-24.json' in fs._files


def test_serialises_phases_under_phases_key() -> None:
    fs = FakeFileSystem()
    orchestrator = WorkflowOrchestrator(fs, 'wf', '2025-06-24')
    phases = [
        [[{'name': 'JOB_TYPE', 'value': 'extract_and_load'}]],
        [[{'name': 'JOB_TYPE', 'value': 'consolidate_logs'}]],
    ]

    orchestrator.write_manifest(phases)

    contents = json.loads(fs._files['manifests/wf_2025-06-24.json'])
    assert contents == {'phases': phases}


def test_overwrites_existing_manifest() -> None:
    fs = FakeFileSystem({'manifests/wf_2025-06-24.json': 'stale'})
    orchestrator = WorkflowOrchestrator(fs, 'wf', '2025-06-24')

    orchestrator.write_manifest([])

    assert json.loads(fs._files['manifests/wf_2025-06-24.json']) == {'phases': []}
