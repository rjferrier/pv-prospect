"""JSONL task-outcome ledger.

Each task records one or more JSONL entries describing its outcome
(``status='completed'`` or ``status='failed'``) on the *ledger filesystem*.
During a run, entries live at ``<run_date>/<workflow_name>/<task_hash>.jsonl``
— one file per task, partitioned by hash so concurrent tasks never write
to the same file. At workflow end, ``consolidate_ledger`` merges the
per-task files into a single ``<run_date>/<run_date>-<HHMMSS>-<workflow>.jsonl``
sorted by ``recorded_at`` and removes the originals.

The ``run_date`` is the workflow's UTC trigger date, pinned at the
workflow's ``init`` step and propagated to every task as ``RUN_DATE``. It
is distinct from any ``start_date``/``end_date`` describing the data
window being processed — those live inside each entry's ``descriptor``.

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
    ledger_fs: FileSystem,
    workflow_name: str,
    run_date: date,
    now: NowFn = _utc_now,
) -> None:
    """Merge per-task ledger files into one consolidated JSONL.

    Reads every ``*.jsonl`` under ``<run_date>/<workflow_name>/``,
    concatenates the JSON lines, sorts them by ``recorded_at``, writes a
    single consolidated file at
    ``<run_date>/<run_date>-<HHMMSS>-<workflow_name>.jsonl``, and removes
    the per-task files.
    """
    date_str = run_date.strftime('%Y-%m-%d')
    prefix = ledger_prefix(date_str, workflow_name)
    entries = ledger_fs.list_files(prefix, '*.jsonl')
    if not entries:
        logger.info(
            'No ledger entries to consolidate for %s/%s', date_str, workflow_name
        )
        return

    records: list[tuple[str, str]] = []
    for entry in entries:
        content = ledger_fs.read_text(entry.path)
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
    ledger_fs.write_text(
        consolidated_path, '\n'.join(line for _, line in records) + '\n'
    )
    logger.info(
        'Consolidated %d ledger entries into %s', len(records), consolidated_path
    )

    for entry in entries:
        ledger_fs.delete(entry.path)

    ledger_fs.rmdir(prefix)
