"""Tests for read_completed_descriptors."""

import json

from pv_prospect.etl.storage.ledger import read_completed_descriptors

from ...helpers import FakeFileSystem


def _entry(status: str, descriptor: dict[str, str]) -> str:
    return (
        json.dumps(
            {
                'recorded_at': '2026-05-14T07:00:00+00:00',
                'run_date': '2026-05-14',
                'workflow': 'wf',
                'task_hash': 'h',
                'descriptor': descriptor,
                'status': status,
            }
        )
        + '\n'
    )


def test_returns_completed_descriptors() -> None:
    path = '2026-05-14/2026-05-14-070000-wf.jsonl'
    ledger_fs = FakeFileSystem(
        {
            path: (
                _entry('completed', {'data_source': 'pvoutput', 'pv_system_id': '1'})
                + _entry(
                    'completed',
                    {'data_source': 'pvoutput', 'pv_system_id': '2'},
                )
            )
        }
    )

    descriptors = read_completed_descriptors(ledger_fs, path)

    assert descriptors == [
        {'data_source': 'pvoutput', 'pv_system_id': '1'},
        {'data_source': 'pvoutput', 'pv_system_id': '2'},
    ]


def test_skips_failed_entries() -> None:
    path = '2026-05-14/2026-05-14-070000-wf.jsonl'
    ledger_fs = FakeFileSystem(
        {
            path: (
                _entry('failed', {'pv_system_id': '1'})
                + _entry('completed', {'pv_system_id': '2'})
            )
        }
    )

    descriptors = read_completed_descriptors(ledger_fs, path)

    assert descriptors == [{'pv_system_id': '2'}]


def test_tolerates_malformed_lines() -> None:
    path = '2026-05-14/2026-05-14-070000-wf.jsonl'
    ledger_fs = FakeFileSystem(
        {path: 'not json\n' + _entry('completed', {'pv_system_id': '1'})}
    )

    descriptors = read_completed_descriptors(ledger_fs, path)

    assert descriptors == [{'pv_system_id': '1'}]


def test_returns_empty_for_ledger_with_no_completed_entries() -> None:
    path = '2026-05-14/2026-05-14-070000-wf.jsonl'
    ledger_fs = FakeFileSystem({path: _entry('failed', {'pv_system_id': '1'})})

    assert read_completed_descriptors(ledger_fs, path) == []
