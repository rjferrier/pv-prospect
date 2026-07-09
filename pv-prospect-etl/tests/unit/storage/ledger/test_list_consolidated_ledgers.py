"""Tests for list_consolidated_ledgers."""

from pv_prospect.etl.storage.ledger import list_consolidated_ledgers

from ...helpers import FakeFileSystem


def test_returns_empty_when_no_ledgers() -> None:
    assert list_consolidated_ledgers(FakeFileSystem(), 'wf') == []


def test_returns_consolidated_files_for_workflow() -> None:
    ledger_fs = FakeFileSystem(
        {
            '2026-05-13/2026-05-13-070000-wf.jsonl': '{}\n',
            '2026-05-14/2026-05-14-070000-wf.jsonl': '{}\n',
        }
    )

    entries = list_consolidated_ledgers(ledger_fs, 'wf')

    assert [entry.path for entry in entries] == [
        '2026-05-13/2026-05-13-070000-wf.jsonl',
        '2026-05-14/2026-05-14-070000-wf.jsonl',
    ]


def test_sorted_by_name() -> None:
    ledger_fs = FakeFileSystem(
        {
            '2026-05-14/2026-05-14-070000-wf.jsonl': '{}\n',
            '2026-05-13/2026-05-13-235959-wf.jsonl': '{}\n',
            '2026-05-14/2026-05-14-020000-wf.jsonl': '{}\n',
        }
    )

    entries = list_consolidated_ledgers(ledger_fs, 'wf')

    assert [entry.name for entry in entries] == [
        '2026-05-13-235959-wf.jsonl',
        '2026-05-14-020000-wf.jsonl',
        '2026-05-14-070000-wf.jsonl',
    ]


def test_ignores_per_task_files() -> None:
    ledger_fs = FakeFileSystem(
        {
            '2026-05-14/wf/abc123.jsonl': '{}\n',
            '2026-05-14/2026-05-14-070000-wf.jsonl': '{}\n',
        }
    )

    entries = list_consolidated_ledgers(ledger_fs, 'wf')

    assert [entry.name for entry in entries] == ['2026-05-14-070000-wf.jsonl']


def test_excludes_other_workflows() -> None:
    ledger_fs = FakeFileSystem(
        {
            '2026-05-14/2026-05-14-070000-wf.jsonl': '{}\n',
            '2026-05-14/2026-05-14-070001-other.jsonl': '{}\n',
        }
    )

    entries = list_consolidated_ledgers(ledger_fs, 'wf')

    assert [entry.name for entry in entries] == ['2026-05-14-070000-wf.jsonl']


def test_includes_run_labeled_consolidated_files() -> None:
    """Same-day run1/run2 consolidates are both surfaced.

    ``consolidate_logs`` / ``consolidate_ledger`` put the optional
    ``run_label`` *between* the HHMMSS stamp and the workflow name in the
    filename (``<date>-<HHMMSS>-<run_label>-<workflow>.jsonl``), so the
    ``*-<workflow>.jsonl`` matcher still picks them up.
    """
    ledger_fs = FakeFileSystem(
        {
            '2026-05-14/2026-05-14-053000-run1-wf.jsonl': '{}\n',
            '2026-05-14/2026-05-14-073000-run2-wf.jsonl': '{}\n',
        }
    )

    entries = list_consolidated_ledgers(ledger_fs, 'wf')

    assert [entry.name for entry in entries] == [
        '2026-05-14-053000-run1-wf.jsonl',
        '2026-05-14-073000-run2-wf.jsonl',
    ]


def test_since_excludes_earlier_run_dates() -> None:
    ledger_fs = FakeFileSystem(
        {
            '2026-05-13/2026-05-13-070000-wf.jsonl': '{}\n',
            '2026-05-14/2026-05-14-070000-wf.jsonl': '{}\n',
            '2026-05-15/2026-05-15-070000-wf.jsonl': '{}\n',
        }
    )

    entries = list_consolidated_ledgers(
        ledger_fs, 'wf', since='2026-05-14-070000-wf.jsonl'
    )

    assert [entry.name for entry in entries] == [
        '2026-05-14-070000-wf.jsonl',
        '2026-05-15-070000-wf.jsonl',
    ]


def test_since_is_a_bound_not_a_filter() -> None:
    """*since* trims whole run-date directories, nothing finer.

    A ledger from earlier on the same day as *since* is still returned —
    the caller applies its own strict comparison. Bounding on the date
    alone is what lets the backend scan from a directory rather than
    filter a full listing.
    """
    ledger_fs = FakeFileSystem(
        {
            '2026-05-14/2026-05-14-020000-wf.jsonl': '{}\n',
            '2026-05-14/2026-05-14-070000-wf.jsonl': '{}\n',
        }
    )

    entries = list_consolidated_ledgers(
        ledger_fs, 'wf', since='2026-05-14-070000-wf.jsonl'
    )

    assert [entry.name for entry in entries] == [
        '2026-05-14-020000-wf.jsonl',
        '2026-05-14-070000-wf.jsonl',
    ]


def test_empty_since_scans_the_whole_history() -> None:
    ledger_fs = FakeFileSystem(
        {
            '2026-05-13/2026-05-13-070000-wf.jsonl': '{}\n',
            '2026-05-14/2026-05-14-070000-wf.jsonl': '{}\n',
        }
    )

    entries = list_consolidated_ledgers(ledger_fs, 'wf', since='')

    assert len(entries) == 2


def test_excludes_workflow_whose_name_extends_this_one() -> None:
    """A workflow name that is a prefix of another (``pv-prospect-extract``
    vs ``pv-prospect-extract-pv-sites-backfill``) must not pull in the
    longer-named workflow's consolidated files."""
    ledger_fs = FakeFileSystem(
        {
            '2026-05-14/2026-05-14-020000-pv-prospect-extract.jsonl': '{}\n',
            (
                '2026-05-14/2026-05-14-024800-'
                'pv-prospect-extract-pv-sites-backfill.jsonl'
            ): '{}\n',
        }
    )

    entries = list_consolidated_ledgers(ledger_fs, 'pv-prospect-extract')

    assert [entry.name for entry in entries] == [
        '2026-05-14-020000-pv-prospect-extract.jsonl'
    ]
