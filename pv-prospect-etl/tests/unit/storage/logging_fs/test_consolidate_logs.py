"""Tests for consolidate_logs."""

from datetime import date, datetime, timezone

from pv_prospect.etl.storage.logging_fs import consolidate_logs

from ...helpers import FakeFileSystem

CONSOLIDATION_TIME = datetime(2025, 6, 24, 11, 0, 0, tzinfo=timezone.utc)


def _fixed_now() -> datetime:
    return CONSOLIDATION_TIME


def test_merges_entries_into_consolidated_file() -> None:
    log_fs = FakeFileSystem(
        {
            '2025-06-24/my-wf/103015000000.txt': '2025-06-24T10:30:15+00:00 CREATED raw/a.csv\n',
            '2025-06-24/my-wf/103016000000.txt': '2025-06-24T10:30:16+00:00 CREATED raw/b.csv\n',
        }
    )

    consolidate_logs(log_fs, 'my-wf', date(2025, 6, 24), now=_fixed_now)

    content = log_fs._files['2025-06-24/2025-06-24-110000-my-wf.txt']
    assert 'raw/a.csv' in content
    assert 'raw/b.csv' in content


def test_sorts_entries_by_timestamp() -> None:
    log_fs = FakeFileSystem(
        {
            '2025-06-24/wf/103016000000.txt': '2025-06-24T10:30:16+00:00 CREATED raw/b.csv\n',
            '2025-06-24/wf/103015000000.txt': '2025-06-24T10:30:15+00:00 CREATED raw/a.csv\n',
        }
    )

    consolidate_logs(log_fs, 'wf', date(2025, 6, 24), now=_fixed_now)

    content = log_fs._files['2025-06-24/2025-06-24-110000-wf.txt']
    lines = content.strip().split('\n')
    assert 'raw/a.csv' in lines[0]
    assert 'raw/b.csv' in lines[1]


def test_deletes_individual_entry_files() -> None:
    log_fs = FakeFileSystem(
        {
            '2025-06-24/wf/103015000000.txt': '2025-06-24T10:30:15+00:00 CREATED raw/a.csv\n',
            '2025-06-24/wf/103016000000.txt': '2025-06-24T10:30:16+00:00 CREATED raw/b.csv\n',
        }
    )

    consolidate_logs(log_fs, 'wf', date(2025, 6, 24), now=_fixed_now)

    assert '2025-06-24/wf/103015000000.txt' not in log_fs._files
    assert '2025-06-24/wf/103016000000.txt' not in log_fs._files
    assert '2025-06-24/2025-06-24-110000-wf.txt' in log_fs._files


def test_noop_when_no_entries() -> None:
    log_fs = FakeFileSystem()

    consolidate_logs(log_fs, 'wf', date(2025, 6, 24), now=_fixed_now)

    assert len(log_fs._files) == 0


def test_only_processes_matching_workflow() -> None:
    log_fs = FakeFileSystem(
        {
            '2025-06-24/wf-a/103015000000.txt': '2025-06-24T10:30:15+00:00 CREATED raw/a.csv\n',
            '2025-06-24/wf-b/103016000000.txt': '2025-06-24T10:30:16+00:00 CREATED raw/b.csv\n',
        }
    )

    consolidate_logs(log_fs, 'wf-a', date(2025, 6, 24), now=_fixed_now)

    assert '2025-06-24/wf-b/103016000000.txt' in log_fs._files
    assert '2025-06-24/wf-a/103015000000.txt' not in log_fs._files
    assert '2025-06-24/2025-06-24-110000-wf-a.txt' in log_fs._files


def test_run_label_reads_only_its_scratch_subdir_and_names_the_consolidated_file() -> (
    None
):
    """Two same-day runs of the same workflow consolidate independently."""
    log_fs = FakeFileSystem(
        {
            '2025-06-24/wf/run1/103015000000.txt': '2025-06-24T10:30:15+00:00 CREATED raw/a.csv\n',
            '2025-06-24/wf/run2/110030000000.txt': '2025-06-24T11:00:30+00:00 CREATED raw/b.csv\n',
        }
    )

    consolidate_logs(log_fs, 'wf', date(2025, 6, 24), run_label='run1', now=_fixed_now)

    # run1's consolidated file is named with run_label *before* workflow_name
    # so that ``list_consolidated_ledgers``'s ``*-<workflow>.<ext>`` pattern
    # still picks it up.
    assert '2025-06-24/2025-06-24-110000-run1-wf.txt' in log_fs._files
    consolidated = log_fs._files['2025-06-24/2025-06-24-110000-run1-wf.txt']
    assert 'raw/a.csv' in consolidated
    assert 'raw/b.csv' not in consolidated

    # run1's consolidate didn't touch run2's scratch entries.
    assert '2025-06-24/wf/run2/110030000000.txt' in log_fs._files
    # run1's own scratch entries were cleaned up.
    assert '2025-06-24/wf/run1/103015000000.txt' not in log_fs._files


def test_does_not_rmdir_the_per_write_directory() -> None:
    """We intentionally don't call rmdir on the per-write directory.

    GCS has no real directories — per-entry deletes already remove the
    data — and calling delete_folder under concurrent writes from another
    same-day workflow execution races with new files appearing in the
    folder (the FailedPrecondition: "folder you tried to delete is not
    empty" failure mode). Per-write deletes are all we need.
    """
    log_fs = FakeFileSystem(
        {
            '2025-06-24/wf/103015000000.txt': '...',
        }
    )
    log_fs.mkdir('2025-06-24/wf')

    consolidate_logs(log_fs, 'wf', date(2025, 6, 24), now=_fixed_now)

    assert '2025-06-24/wf' in log_fs.created_dirs
    assert '2025-06-24/wf/103015000000.txt' not in log_fs._files
