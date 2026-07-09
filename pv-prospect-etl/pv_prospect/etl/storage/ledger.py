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
import threading
from datetime import date, datetime, timezone
from typing import Callable, Mapping

from pv_prospect.etl.storage.base import FileEntry, FileSystem

logger = logging.getLogger(__name__)

NowFn = Callable[[], datetime]

# Every ledger path starts with the run date, so the first ten characters
# of a consolidated ledger's name are also the directory it lives in.
_RUN_DATE_LEN = len('YYYY-MM-DD')


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


def consolidated_ledger_path(
    date_str: str, workflow_name: str, run_label: str = '', now: NowFn = _utc_now
) -> str:
    """Return the path of a run's consolidated ledger file.

    ``<date_str>/<date_str>-<HHMMSS>-[<run_label>-]<workflow_name>.jsonl``.
    The run date leads the name so a lexical sort is also chronological;
    *run_label* is folded into the stem so same-day runs stay distinct
    yet still match the ``*-<workflow_name>.jsonl`` discovery pattern.
    """
    stem = f'{run_label}-{workflow_name}' if run_label else workflow_name
    return f'{date_str}/{date_str}-{now().strftime("%H%M%S")}-{stem}.jsonl'


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
    file (see :func:`consolidated_ledger_path`), and removes the per-task
    files.

    This is the consolidation path for *distributed* workflows, where
    each task is a separate process that can only record its outcome to
    its own file. A single-process workflow should instead accumulate
    outcomes in a :class:`LedgerCollector`, which sidesteps the per-task
    file fan-out — and the O(N) serial GCS round-trips this function
    needs to undo it.
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

    consolidated_path = consolidated_ledger_path(
        date_str, workflow_name, run_label, now
    )
    ledger_fs.write_text(
        consolidated_path, '\n'.join(line for _, line in records) + '\n'
    )
    logger.info(
        'Consolidated %d ledger entries into %s', len(records), consolidated_path
    )

    for entry in entries:
        ledger_fs.delete(entry.path)


class LedgerCollector:
    """In-memory accumulator of task-outcome ledger entries.

    A drop-in alternative to per-task ledger files for a *single-process*
    workflow: every task records its outcome into one shared, thread-safe
    buffer and :meth:`flush` writes a single consolidated ledger file at
    the end. This sidesteps the per-task file fan-out — and the O(N)
    serial-GCS-round-trip :func:`consolidate_ledger` it forces — which
    does not fit its time budget once a run has tens of thousands of
    tasks.

    Idempotent on completed entries like
    :meth:`WorkflowOrchestrator.record_outcome`: a second ``completed``
    entry for a task hash already seen as ``completed`` is dropped;
    ``failed`` entries always accumulate.
    """

    def __init__(self, workflow_name: str, run_date: str, run_label: str = '') -> None:
        self._workflow_name = workflow_name
        self._run_date = run_date
        self._run_label = run_label
        self._lock = threading.Lock()
        self._entries: list[dict[str, object]] = []
        self._completed: set[str] = set()

    def record(self, entry: Mapping[str, object]) -> None:
        """Buffer one ledger *entry*.

        *entry* is the dict shape produced by
        :meth:`WorkflowOrchestrator.record_outcome`. Thread-safe; a
        repeated ``completed`` entry for the same ``task_hash`` is
        dropped.
        """
        stored = dict(entry)
        task_hash = str(stored.get('task_hash', ''))
        with self._lock:
            if stored.get('status') == 'completed':
                if task_hash in self._completed:
                    return
                self._completed.add(task_hash)
            self._entries.append(stored)

    def flush(self, ledger_fs: FileSystem, now: NowFn = _utc_now) -> None:
        """Write the buffered entries as one consolidated ledger file.

        Entries are sorted by ``recorded_at`` and written to the path
        :func:`consolidated_ledger_path` returns — the same path
        :func:`consolidate_ledger` produces — so
        :func:`list_consolidated_ledgers` discovers it identically. A
        no-op when nothing was recorded.

        Use this mode when the workflow runs as a single Cloud Run task
        for the whole day (e.g. the transform backfill) — the flush is
        the final consolidated file, no end-of-workflow consolidation
        step needed.
        """
        with self._lock:
            entries = sorted(self._entries, key=lambda e: str(e.get('recorded_at', '')))
        if not entries:
            logger.info(
                'No ledger entries to flush for %s/%s',
                self._run_date,
                self._workflow_name,
            )
            return
        path = consolidated_ledger_path(
            self._run_date, self._workflow_name, self._run_label, now
        )
        ledger_fs.write_text(path, '\n'.join(json.dumps(e) for e in entries) + '\n')
        logger.info('Consolidated %d ledger entries into %s', len(entries), path)

    def flush_per_task(self, ledger_fs: FileSystem, task_hash: str) -> None:
        """Write the buffered entries as one per-task scratch ledger file.

        Entries are sorted by ``recorded_at`` and written to
        :func:`ledger_entry_path` — the same scratch path
        :func:`consolidate_ledger` reads — so the end-of-workflow
        consolidation step merges this task's file with peer tasks' into
        one daily consolidated file. A no-op when nothing was recorded.

        Use this mode when the workflow dispatches multiple Cloud Run
        tasks per day (e.g. the extraction backfills' batched
        dispatches): each task buffers its sub-task outcomes in memory
        and writes one file containing them all, instead of one per
        sub-task. The handful of per-task files the workflow ends up
        with is what the existing :func:`consolidate_ledger` step then
        merges into the daily consolidated file.
        """
        with self._lock:
            entries = sorted(self._entries, key=lambda e: str(e.get('recorded_at', '')))
        if not entries:
            logger.info(
                'No ledger entries to flush for %s/%s',
                self._run_date,
                self._workflow_name,
            )
            return
        path = ledger_entry_path(
            self._run_date, self._workflow_name, task_hash, self._run_label
        )
        ledger_fs.write_text(path, '\n'.join(json.dumps(e) for e in entries) + '\n')
        logger.info('Wrote %d ledger entries to per-task file %s', len(entries), path)


def list_consolidated_ledgers(
    ledger_fs: FileSystem, workflow_name: str, since: str = ''
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

    The ``-<workflow_name>.jsonl`` suffix is passed to the filesystem as
    the listing *pattern* rather than applied to a full listing
    afterwards, so a backend that can filter server-side (GCS) does the
    work proportional to the number of matches instead of to the size of
    the ledger tree.

    *since* is a consolidated-ledger **name** — typically a caller's
    high-water mark — below which nothing need be scanned. Only run-date
    directories at or after that name's date are listed. It is a
    conservative bound, not a filter: ledgers from earlier the same day
    are still returned, so a caller wanting a strict ``> since`` must
    still say so.
    """
    entries = ledger_fs.list_files(
        '',
        f'*-{workflow_name}.jsonl',
        recursive=True,
        start_offset=since[:_RUN_DATE_LEN],
    )
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
