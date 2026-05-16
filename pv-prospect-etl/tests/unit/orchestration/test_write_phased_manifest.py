"""Tests for WorkflowOrchestrator.write_phased_manifest."""

import json

import pytest
from pv_prospect.etl import PHASED_MANIFEST_VERSION, WorkflowOrchestrator
from pv_prospect.etl.orchestration import TASKS_PER_PHASE_FILE

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
    assert [p['files'] for p in index['phases']] == [
        ['wf.phase-0.part-0.json'],
        ['wf.phase-1.part-0.json'],
    ]


def test_per_phase_part_files_carry_only_varying_rows() -> None:
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
    assert index['phases'][0]['files'] == ['wf.phase-0.part-0.json']
    phase_part = json.loads(manifests_fs._files['2026-05-16/wf.phase-0.part-0.json'])
    assert phase_part == {'rows': [['50,0', 'h1'], ['51,0', 'h2']]}


def test_empty_phase_still_emits_single_part_file() -> None:
    manifests_fs = FakeFileSystem()
    orchestrator = _make_orchestrator(manifests_fs)

    orchestrator.write_phased_manifest([[], []])

    assert json.loads(manifests_fs._files['2026-05-16/wf.phase-0.part-0.json']) == {
        'rows': []
    }
    assert json.loads(manifests_fs._files['2026-05-16/wf.phase-1.part-0.json']) == {
        'rows': []
    }


def test_large_phase_is_sharded_across_multiple_part_files() -> None:
    """A phase exceeding TASKS_PER_PHASE_FILE rows lands across multiple
    part files; the index ``files`` list names each in order so the
    workflow can iterate them."""
    manifests_fs = FakeFileSystem()
    orchestrator = _make_orchestrator(manifests_fs)
    row_count = TASKS_PER_PHASE_FILE + 100
    phase = [
        [
            {'name': 'STEP', 'value': 'clean'},
            {'name': 'LOCATION', 'value': f'loc-{i}'},
        ]
        for i in range(row_count)
    ]

    orchestrator.write_phased_manifest([phase])

    index = json.loads(manifests_fs._files['2026-05-16/wf.json'])
    assert index['phases'][0]['files'] == [
        'wf.phase-0.part-0.json',
        'wf.phase-0.part-1.json',
    ]
    part0 = json.loads(manifests_fs._files['2026-05-16/wf.phase-0.part-0.json'])
    part1 = json.loads(manifests_fs._files['2026-05-16/wf.phase-0.part-1.json'])
    assert len(part0['rows']) == TASKS_PER_PHASE_FILE
    assert len(part1['rows']) == 100


def test_overwrites_existing_files() -> None:
    manifests_fs = FakeFileSystem(
        {
            '2026-05-16/wf.json': 'stale',
            '2026-05-16/wf.phase-0.part-0.json': 'stale-phase',
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
