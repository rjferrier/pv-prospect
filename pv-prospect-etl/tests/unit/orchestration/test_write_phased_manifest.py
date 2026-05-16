"""Tests for WorkflowOrchestrator.write_phased_manifest."""

import json

import pytest
from pv_prospect.etl import PHASED_MANIFEST_VERSION, WorkflowOrchestrator

from ..helpers import FakeFileSystem


def _make_orchestrator(manifests_fs: FakeFileSystem) -> WorkflowOrchestrator:
    return WorkflowOrchestrator('wf', '2026-05-16', manifests_fs=manifests_fs)


def test_writes_index_at_workflow_path() -> None:
    manifests_fs = FakeFileSystem()
    orchestrator = _make_orchestrator(manifests_fs)

    orchestrator.write_phased_manifest([])

    assert '2026-05-16/wf.json' in manifests_fs._files


def test_index_carries_version_and_phase_descriptors() -> None:
    manifests_fs = FakeFileSystem()
    orchestrator = _make_orchestrator(manifests_fs)
    phases = [
        [
            [
                {'name': 'STEP', 'value': 'clean'},
                {'name': 'LOCATION', 'value': '50,0'},
            ],
        ],
        [
            [
                {'name': 'STEP', 'value': 'prepare'},
                {'name': 'LOCATION', 'value': '50,0'},
            ],
        ],
    ]

    orchestrator.write_phased_manifest(phases)

    index = json.loads(manifests_fs._files['2026-05-16/wf.json'])
    assert index['version'] == PHASED_MANIFEST_VERSION
    assert [p['file'] for p in index['phases']] == [
        'wf.phase-0.json',
        'wf.phase-1.json',
    ]


def test_per_phase_files_carry_only_varying_rows() -> None:
    manifests_fs = FakeFileSystem()
    orchestrator = _make_orchestrator(manifests_fs)
    phase = [
        [
            {'name': 'STEP', 'value': 'clean'},
            {'name': 'LOCATION', 'value': '50,0'},
            {'name': 'TASK_HASH', 'value': 'h1'},
        ],
        [
            {'name': 'STEP', 'value': 'clean'},
            {'name': 'LOCATION', 'value': '51,0'},
            {'name': 'TASK_HASH', 'value': 'h2'},
        ],
    ]

    orchestrator.write_phased_manifest([phase])

    index = json.loads(manifests_fs._files['2026-05-16/wf.json'])
    assert index['phases'][0]['common_env'] == [{'name': 'STEP', 'value': 'clean'}]
    assert index['phases'][0]['task_keys'] == ['LOCATION', 'TASK_HASH']
    phase_doc = json.loads(manifests_fs._files['2026-05-16/wf.phase-0.json'])
    assert phase_doc == {'rows': [['50,0', 'h1'], ['51,0', 'h2']]}


def test_empty_phase_still_emits_phase_file() -> None:
    manifests_fs = FakeFileSystem()
    orchestrator = _make_orchestrator(manifests_fs)

    orchestrator.write_phased_manifest([[], []])

    assert json.loads(manifests_fs._files['2026-05-16/wf.phase-0.json']) == {'rows': []}
    assert json.loads(manifests_fs._files['2026-05-16/wf.phase-1.json']) == {'rows': []}


def test_overwrites_existing_files() -> None:
    manifests_fs = FakeFileSystem(
        {
            '2026-05-16/wf.json': 'stale',
            '2026-05-16/wf.phase-0.json': 'stale-phase',
        }
    )
    orchestrator = _make_orchestrator(manifests_fs)

    orchestrator.write_phased_manifest([[]])

    index = json.loads(manifests_fs._files['2026-05-16/wf.json'])
    assert index['version'] == PHASED_MANIFEST_VERSION


def test_raises_without_manifests_fs() -> None:
    orchestrator = WorkflowOrchestrator('wf', '2026-05-16')

    with pytest.raises(RuntimeError):
        orchestrator.write_phased_manifest([])
