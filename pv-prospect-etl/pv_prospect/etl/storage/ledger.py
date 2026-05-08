"""JSONL task-outcome ledger.

Each task records one or more JSONL entries describing its outcome
(``status='completed'`` or ``status='failed'``) on the *log filesystem*.
During a run, entries live at ``<YYYY-MM-DD>/<workflow_name>/<task_hash>.jsonl``
— one file per task, partitioned by hash so concurrent tasks never write
to the same file. At workflow end, ``consolidate_ledger`` merges the
per-task files into a single ``<YYYY-MM-DD>/<YYYY-MM-DD-HHMMSS>-<workflow>.jsonl``
sorted by ``recorded_at`` and removes the originals.

The schema of each entry is documented at
``WorkflowOrchestrator.record_outcome``.
"""

import json
import logging
from datetime import date, datetime, timezone
from typing import Callable

from pv_prospect.etl.storage.base import FileSystem

logger = logging.getLogger(__name__)

NowFn = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ledger_prefix(run_date: str, workflow_name: str) -> str:
    """Per-run directory holding per-task ledger files."""
    return f'{run_date}/{workflow_name}'


def ledger_entry_path(run_date: str, workflow_name: str, task_hash: str) -> str:
    """Path of a single task's ledger file within a run."""
    return f'{ledger_prefix(run_date, workflow_name)}/{task_hash}.jsonl'


def consolidate_ledger(
    log_fs: FileSystem,
    workflow_name: str,
    log_date: date,
    now: NowFn = _utc_now,
) -> None:
    """Merge per-task ledger files into one consolidated JSONL.

    Reads every ``*.jsonl`` under ``<YYYY-MM-DD>/<workflow_name>/``,
    concatenates the JSON lines, sorts them by ``recorded_at``, writes a
    single consolidated file at
    ``<YYYY-MM-DD>/<YYYY-MM-DD-HHMMSS>-<workflow_name>.jsonl``, and removes
    the per-task files.
    """
    date_str = log_date.strftime('%Y-%m-%d')
    prefix = ledger_prefix(date_str, workflow_name)
    entries = log_fs.list_files(prefix, '*.jsonl')
    if not entries:
        logger.info(
            'No ledger entries to consolidate for %s/%s', date_str, workflow_name
        )
        return

    records: list[tuple[str, str]] = []
    for entry in entries:
        content = log_fs.read_text(entry.path)
        for line in content.splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                logger.warning('Skipping malformed ledger line in %s', entry.path)
                continue
            records.append((rec.get('recorded_at', ''), line))

    records.sort(key=lambda r: r[0])

    consolidated_name = f'{date_str}-{now().strftime("%H%M%S")}-{workflow_name}.jsonl'
    consolidated_path = f'{date_str}/{consolidated_name}'
    log_fs.write_text(consolidated_path, '\n'.join(line for _, line in records) + '\n')
    logger.info(
        'Consolidated %d ledger entries into %s', len(records), consolidated_path
    )

    for entry in entries:
        log_fs.delete(entry.path)

    log_fs.rmdir(prefix)
    parent = '/'.join(prefix.split('/')[:-1])
    if parent:
        try:
            log_fs.rmdir(parent)
        except Exception:  # nosec B110
            pass
