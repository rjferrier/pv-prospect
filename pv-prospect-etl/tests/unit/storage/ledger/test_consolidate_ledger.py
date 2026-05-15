"""Tests for consolidate_ledger."""

import json
from datetime import date, datetime, timezone

from pv_prospect.etl.storage.ledger import consolidate_ledger

from ...helpers import FakeFileSystem

CONSOLIDATION_TIME = datetime(2025, 6, 24, 11, 0, 0, tzinfo=timezone.utc)


def _fixed_now() -> datetime:
    return CONSOLIDATION_TIME


def _entry(recorded_at: str, task_hash: str, status: str) -> str:
    return (
        json.dumps(
            {
                'recorded_at': recorded_at,
                'run_date': '2025-06-24',
                'workflow': 'wf',
                'task_hash': task_hash,
                'descriptor': {'system_id': '89665'},
                'status': status,
            }
        )
        + '\n'
    )


def test_merges_entries_into_consolidated_file() -> None:
    log_fs = FakeFileSystem(
        {
            '2025-06-24/wf/aaa.jsonl': _entry(
                '2025-06-24T10:30:15+00:00', 'aaa', 'completed'
            ),
            '2025-06-24/wf/bbb.jsonl': _entry(
                '2025-06-24T10:30:16+00:00', 'bbb', 'failed'
            ),
        }
    )

    consolidate_ledger(log_fs, 'wf', date(2025, 6, 24), now=_fixed_now)

    content = log_fs._files['2025-06-24/2025-06-24-110000-wf.jsonl']
    lines = [line for line in content.strip().split('\n') if line]
    assert len(lines) == 2
    parsed = [json.loads(line) for line in lines]
    hashes = {p['task_hash'] for p in parsed}
    assert hashes == {'aaa', 'bbb'}


def test_sorts_entries_by_recorded_at() -> None:
    log_fs = FakeFileSystem(
        {
            '2025-06-24/wf/bbb.jsonl': _entry(
                '2025-06-24T10:30:16+00:00', 'bbb', 'failed'
            ),
            '2025-06-24/wf/aaa.jsonl': _entry(
                '2025-06-24T10:30:15+00:00', 'aaa', 'completed'
            ),
        }
    )

    consolidate_ledger(log_fs, 'wf', date(2025, 6, 24), now=_fixed_now)

    content = log_fs._files['2025-06-24/2025-06-24-110000-wf.jsonl']
    parsed = [json.loads(line) for line in content.strip().split('\n')]
    assert [p['task_hash'] for p in parsed] == ['aaa', 'bbb']


def test_preserves_multiple_entries_per_task() -> None:
    multi = _entry('2025-06-24T10:30:15+00:00', 'aaa', 'failed') + _entry(
        '2025-06-24T10:30:20+00:00', 'aaa', 'completed'
    )
    log_fs = FakeFileSystem({'2025-06-24/wf/aaa.jsonl': multi})

    consolidate_ledger(log_fs, 'wf', date(2025, 6, 24), now=_fixed_now)

    content = log_fs._files['2025-06-24/2025-06-24-110000-wf.jsonl']
    parsed = [json.loads(line) for line in content.strip().split('\n')]
    statuses = [p['status'] for p in parsed]
    assert statuses == ['failed', 'completed']


def test_deletes_per_task_files() -> None:
    log_fs = FakeFileSystem(
        {
            '2025-06-24/wf/aaa.jsonl': _entry(
                '2025-06-24T10:30:15+00:00', 'aaa', 'completed'
            ),
        }
    )

    consolidate_ledger(log_fs, 'wf', date(2025, 6, 24), now=_fixed_now)

    assert '2025-06-24/wf/aaa.jsonl' not in log_fs._files
    assert '2025-06-24/2025-06-24-110000-wf.jsonl' in log_fs._files


def test_noop_when_no_entries() -> None:
    log_fs = FakeFileSystem()

    consolidate_ledger(log_fs, 'wf', date(2025, 6, 24), now=_fixed_now)

    assert len(log_fs._files) == 0


def test_only_processes_matching_workflow() -> None:
    log_fs = FakeFileSystem(
        {
            '2025-06-24/wf-a/aaa.jsonl': _entry(
                '2025-06-24T10:30:15+00:00', 'aaa', 'completed'
            ),
            '2025-06-24/wf-b/bbb.jsonl': _entry(
                '2025-06-24T10:30:16+00:00', 'bbb', 'completed'
            ),
        }
    )

    consolidate_ledger(log_fs, 'wf-a', date(2025, 6, 24), now=_fixed_now)

    assert '2025-06-24/wf-b/bbb.jsonl' in log_fs._files
    assert '2025-06-24/wf-a/aaa.jsonl' not in log_fs._files
    assert '2025-06-24/2025-06-24-110000-wf-a.jsonl' in log_fs._files


def test_run_label_reads_only_its_scratch_subdir_and_names_the_consolidated_file() -> (
    None
):
    """Two same-day runs of the same workflow consolidate independently."""
    log_fs = FakeFileSystem(
        {
            '2025-06-24/wf/run1/aaa.jsonl': _entry(
                '2025-06-24T10:30:15+00:00', 'aaa', 'completed'
            ),
            '2025-06-24/wf/run2/bbb.jsonl': _entry(
                '2025-06-24T11:00:30+00:00', 'bbb', 'completed'
            ),
        }
    )

    consolidate_ledger(
        log_fs, 'wf', date(2025, 6, 24), run_label='run1', now=_fixed_now
    )

    # run1's consolidated file is named with run_label *before* workflow_name
    # so that ``list_consolidated_ledgers``'s ``*-<workflow>.jsonl`` pattern
    # still picks it up.
    assert '2025-06-24/2025-06-24-110000-run1-wf.jsonl' in log_fs._files
    consolidated = log_fs._files['2025-06-24/2025-06-24-110000-run1-wf.jsonl']
    hashes = {
        json.loads(line)['task_hash'] for line in consolidated.strip().split('\n')
    }
    assert hashes == {'aaa'}

    # run1's consolidate didn't touch run2's scratch entries.
    assert '2025-06-24/wf/run2/bbb.jsonl' in log_fs._files
    # run1's own scratch entries were cleaned up.
    assert '2025-06-24/wf/run1/aaa.jsonl' not in log_fs._files


def test_skips_malformed_lines() -> None:
    log_fs = FakeFileSystem(
        {
            '2025-06-24/wf/aaa.jsonl': (
                'not json\n' + _entry('2025-06-24T10:30:15+00:00', 'aaa', 'completed')
            ),
        }
    )

    consolidate_ledger(log_fs, 'wf', date(2025, 6, 24), now=_fixed_now)

    content = log_fs._files['2025-06-24/2025-06-24-110000-wf.jsonl']
    parsed = [json.loads(line) for line in content.strip().split('\n')]
    assert len(parsed) == 1
    assert parsed[0]['task_hash'] == 'aaa'
