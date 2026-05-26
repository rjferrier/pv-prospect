"""Tests for run_consolidate_logs."""

import tempfile
from pathlib import Path

from pv_prospect.etl import run_consolidate_logs
from pv_prospect.etl.storage.backends.local import LocalStorageConfig


def _local_config(prefix: str) -> LocalStorageConfig:
    return LocalStorageConfig(prefix=prefix)


def test_merges_per_task_ledger_files_into_one_consolidated_file() -> None:
    """Per-task scratch ledger files written during the run are merged
    into one daily consolidated file at end-of-workflow. This is the
    fan-in step that lets the run survive having many per-task files."""
    with (
        tempfile.TemporaryDirectory() as log_dir,
        tempfile.TemporaryDirectory() as ledger_dir,
    ):
        ledger_root = Path(ledger_dir)
        per_task_dir = ledger_root / '2025-06-24' / 'wf'
        per_task_dir.mkdir(parents=True)
        (per_task_dir / 'task-a.jsonl').write_text(
            '{"recorded_at": "2025-06-24T10:30:15+00:00", "task_hash": "site-a", '
            '"status": "completed", "run_date": "2025-06-24", "workflow": "wf", '
            '"descriptor": {}}\n'
        )
        (per_task_dir / 'task-b.jsonl').write_text(
            '{"recorded_at": "2025-06-24T10:30:16+00:00", "task_hash": "site-b", '
            '"status": "completed", "run_date": "2025-06-24", "workflow": "wf", '
            '"descriptor": {}}\n'
        )

        run_consolidate_logs(
            log_storage=_local_config(log_dir),
            ledger_storage=_local_config(ledger_dir),
            workflow_name='wf',
            run_date='2025-06-24',
        )

        consolidated_files = list((ledger_root / '2025-06-24').glob('*-wf.jsonl'))
        assert len(consolidated_files) == 1
        assert sum(1 for _ in consolidated_files[0].read_text().splitlines()) == 2
        # Per-task scratch files have been swept up after merging.
        assert not (per_task_dir / 'task-a.jsonl').exists()
        assert not (per_task_dir / 'task-b.jsonl').exists()


def test_skips_when_workflow_name_missing() -> None:
    """Without a workflow name the merge target would be ambiguous; the
    function logs a warning and returns rather than writing anywhere."""
    with tempfile.TemporaryDirectory() as ledger_dir:
        run_consolidate_logs(
            log_storage=None,
            ledger_storage=_local_config(ledger_dir),
            workflow_name='',
            run_date='2025-06-24',
        )

        assert list(Path(ledger_dir).rglob('*')) == []


def test_skips_streams_with_unconfigured_storage() -> None:
    """When the log or ledger storage isn't configured, that stream's
    consolidation is skipped silently — the workflow may run without one
    or the other (e.g. ledger-disabled local dev)."""
    with tempfile.TemporaryDirectory() as ledger_dir:
        ledger_root = Path(ledger_dir)
        per_task_dir = ledger_root / '2025-06-24' / 'wf'
        per_task_dir.mkdir(parents=True)
        (per_task_dir / 'task-a.jsonl').write_text(
            '{"recorded_at": "2025-06-24T10:30:15+00:00", "task_hash": "site-a", '
            '"status": "completed", "run_date": "2025-06-24", "workflow": "wf", '
            '"descriptor": {}}\n'
        )

        run_consolidate_logs(
            log_storage=None,
            ledger_storage=_local_config(ledger_dir),
            workflow_name='wf',
            run_date='2025-06-24',
        )

        # Ledger stream ran (per-task swept up); log stream had no
        # configured storage so nothing happened to it.
        assert not (per_task_dir / 'task-a.jsonl').exists()
