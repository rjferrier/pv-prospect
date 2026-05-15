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

import fnmatch
import json
import logging
from datetime import date, datetime, timezone
from typing import Callable

from pv_prospect.etl.storage.base import FileEntry, FileSystem

logger = logging.getLogger(__name__)

NowFn = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ledger_prefix(run_date: str, workflow_name: str, run_label: str = '') -> str:
    """Per-run directory holding per-task ledger files.

    *run_label* namespaces the directory when multiple workflow executions
    share a ``run_date`` so their consolidates never race on the same
    folder. Default ``''`` (the prevailing case — one execution per day)
    keeps the directory layout flat.
    """
    if run_label:
        return f'{run_date}/{workflow_name}/{run_label}'
    return f'{run_date}/{workflow_name}'


def ledger_entry_path(
    run_date: str, workflow_name: str, task_hash: str, run_label: str = ''
) -> str:
    """Path of a single task's ledger file within a run."""
    return f'{ledger_prefix(run_date, workflow_name, run_label)}/{task_hash}.jsonl'


def consolidate_ledger(
    ledger_fs: FileSystem,
    workflow_name: str,
    run_date: date,
    run_label: str = '',
    now: NowFn = _utc_now,
) -> None:
    """Merge per-task ledger files into one consolidated JSONL.

    Reads every ``*.jsonl`` under
    ``<run_date>/<workflow_name>[/<run_label>]/``, concatenates the JSON
    lines, sorts them by ``recorded_at``, writes a single consolidated
    file at
    ``<run_date>/<run_date>-<HHMMSS>-<workflow_name>[-<run_label>].jsonl``,
    and removes the per-task files. *run_label* keeps concurrent same-day
    executions disjoint at the scratch layer; the consolidated layer
    encodes it in the filename so listings can still distinguish same-day
    runs.
    """
    date_str = run_date.strftime('%Y-%m-%d')
    prefix = ledger_prefix(date_str, workflow_name, run_label)
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

    consolidated_stem = f'{run_label}-{workflow_name}' if run_label else workflow_name
    consolidated_name = (
        f'{date_str}-{now().strftime("%H%M%S")}-{consolidated_stem}.jsonl'
    )
    consolidated_path = f'{date_str}/{consolidated_name}'
    ledger_fs.write_text(
        consolidated_path, '\n'.join(line for _, line in records) + '\n'
    )
    logger.info(
        'Consolidated %d ledger entries into %s', len(records), consolidated_path
    )

    for entry in entries:
        ledger_fs.delete(entry.path)


def list_consolidated_ledgers(
    ledger_fs: FileSystem, workflow_name: str
) -> list[FileEntry]:
    """Return every consolidated ledger file for *workflow_name*, name-sorted.

    Consolidated files are named
    ``<run_date>/<run_date>-<HHMMSS>-<workflow_name>.jsonl`` (see
    :func:`consolidate_ledger`). Because the run date leads the filename,
    a lexical sort by name is also a chronological sort. Per-task ledger
    files (``<run_date>/<workflow_name>/<task_hash>.jsonl``) do not carry
    the ``-<workflow_name>.jsonl`` suffix and so are excluded, as are the
    consolidated files of other workflows whose name is a prefix of this
    one (e.g. ``pv-prospect-extract`` vs
    ``pv-prospect-extract-pv-sites-backfill``).
    """
    pattern = f'*-{workflow_name}.jsonl'
    entries = [
        entry
        for entry in ledger_fs.list_files('', '*.jsonl', recursive=True)
        if fnmatch.fnmatch(entry.name, pattern)
    ]
    return sorted(entries, key=lambda entry: entry.name)


def read_completed_descriptors(
    ledger_fs: FileSystem, path: str
) -> list[dict[str, str]]:
    """Return the ``descriptor`` of every ``completed`` entry in one ledger.

    Parses a single consolidated ledger file line by line; ``failed``
    entries and malformed lines are skipped. Each returned dict is the
    opaque per-workflow ``descriptor`` recorded by
    :meth:`WorkflowOrchestrator.record_outcome`.
    """
    descriptors: list[dict[str, str]] = []
    for line in ledger_fs.read_text(path).splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            logger.warning('Skipping malformed ledger line in %s', path)
            continue
        if not isinstance(rec, dict) or rec.get('status') != 'completed':
            continue
        descriptor = rec.get('descriptor')
        if isinstance(descriptor, dict):
            descriptors.append(descriptor)
    return descriptors
