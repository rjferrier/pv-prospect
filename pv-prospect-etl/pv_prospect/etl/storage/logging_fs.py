"""FileSystem decorator that logs write operations to a separate filesystem."""

import logging
import threading
from datetime import date, datetime, timezone
from typing import Callable

from pv_prospect.etl.storage.base import FileEntry, FileSystem

logger = logging.getLogger(__name__)

NowFn = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class LoggingFileSystem:
    """FileSystem decorator that records every write to a separate log filesystem.

    Each write produces a single-line log entry file at
    ``<run_date>/<workflow_name>[/<run_label>]/<HHMMSSffffff>.txt`` on the
    *log_fs*, where ``run_date`` is the workflow's UTC trigger date pinned
    at the workflow's ``init`` step. ``run_label`` is an optional
    same-day-run discriminator that namespaces the per-write scratch
    directory so concurrent workflow executions don't race on it (default
    ``''`` is the prevailing case of one execution per day). The entry
    contains the UTC timestamp and the fully-qualified path
    (``<label>/<relative_path>``) of the written file.

    When a *log_collector* is supplied, log lines are appended to that
    in-memory :class:`LogCollector` instead of written as per-write
    files — the single-process path, which avoids the file fan-out and
    the O(N) :func:`consolidate_logs` it would otherwise force.
    """

    def __init__(
        self,
        inner: FileSystem,
        log_fs: FileSystem,
        workflow_name: str,
        run_date: str,
        label: str,
        run_label: str = '',
        log_collector: 'LogCollector | None' = None,
        now: NowFn = _utc_now,
    ) -> None:
        self._inner = inner
        self._log_fs = log_fs
        self._workflow_name = workflow_name
        self._run_date = run_date
        self._label = label
        self._run_label = run_label
        self._log_collector = log_collector
        self._now = now

    def __str__(self) -> str:
        return str(self._inner)

    def _log_write(self, path: str) -> None:
        now = self._now()
        full_path = f'{self._label}/{path}' if self._label else path
        line = f'{now.isoformat()} CREATED {full_path}'
        if self._log_collector is not None:
            self._log_collector.record(line)
            return
        time_str = now.strftime('%H%M%S%f')
        log_path = (
            f'{self._run_date}/'
            f'{log_scratch_subprefix(self._workflow_name, self._run_label)}/'
            f'{time_str}.txt'
        )
        try:
            self._log_fs.write_text(log_path, line + '\n')
        except Exception:
            logger.warning('Failed to write log entry for %s', full_path, exc_info=True)

    def exists(self, path: str) -> bool:
        return self._inner.exists(path)

    def read_text(self, path: str) -> str:
        return self._inner.read_text(path)

    def write_text(self, path: str, content: str) -> None:
        self._inner.write_text(path, content)
        self._log_write(path)

    def append_text(self, path: str, content: str) -> None:
        self._inner.append_text(path, content)

    def read_bytes(self, path: str) -> bytes:
        return self._inner.read_bytes(path)

    def write_bytes(self, path: str, content: bytes) -> None:
        self._inner.write_bytes(path, content)
        self._log_write(path)

    def delete(self, path: str) -> None:
        self._inner.delete(path)

    def mkdir(self, path: str) -> None:
        self._inner.mkdir(path)

    def rmdir(self, path: str) -> None:
        self._inner.rmdir(path)

    def list_files(
        self, prefix: str, pattern: str = '*', recursive: bool = False
    ) -> list[FileEntry]:
        return self._inner.list_files(prefix, pattern, recursive)


def log_scratch_subprefix(workflow_name: str, run_label: str) -> str:
    """Return the per-write scratch sub-prefix (relative to ``<run_date>/``).

    ``<workflow_name>[/<run_label>]``. The optional *run_label* namespaces
    the scratch directory when multiple workflow executions share a
    ``run_date`` so their consolidates never race on the same folder.
    Default ``''`` (the prevailing case — one execution per day) keeps
    the directory layout flat.
    """
    if run_label:
        return f'{workflow_name}/{run_label}'
    return workflow_name


def consolidated_log_path(
    date_str: str, workflow_name: str, run_label: str = '', now: NowFn = _utc_now
) -> str:
    """Return the path of a run's consolidated write-audit log file.

    ``<date_str>/<date_str>-<HHMMSS>-[<run_label>-]<workflow_name>.txt``.
    Mirrors the consolidated-ledger naming so same-day runs stay distinct.
    """
    stem = f'{run_label}-{workflow_name}' if run_label else workflow_name
    return f'{date_str}/{date_str}-{now().strftime("%H%M%S")}-{stem}.txt'


def consolidate_logs(
    log_fs: FileSystem,
    workflow_name: str,
    run_date: date,
    run_label: str = '',
    now: NowFn = _utc_now,
) -> None:
    """Merge individual log entry files into a single consolidated log.

    Reads all files under ``<run_date>/<workflow_name>[/<run_label>]/``,
    sorts them by content (which starts with an ISO timestamp), writes a
    consolidated file (see :func:`consolidated_log_path`), and deletes the
    individual entry files.

    This is the consolidation path for *distributed* workflows. A
    single-process workflow should instead route its
    :class:`LoggingFileSystem` through a :class:`LogCollector`, which
    avoids the per-write file fan-out this function has to undo.
    """
    date_str = run_date.strftime('%Y-%m-%d')
    prefix = f'{date_str}/{log_scratch_subprefix(workflow_name, run_label)}'
    entries = log_fs.list_files(prefix, '*.txt')
    if not entries:
        logger.info('No log entries to consolidate for %s/%s', date_str, workflow_name)
        return

    lines: list[str] = []
    for entry in entries:
        content = log_fs.read_text(entry.path)
        lines.append(content.strip())

    lines.sort()

    consolidated_path = consolidated_log_path(date_str, workflow_name, run_label, now)
    log_fs.write_text(consolidated_path, '\n'.join(lines) + '\n')
    logger.info('Consolidated %d log entries into %s', len(lines), consolidated_path)

    for entry in entries:
        log_fs.delete(entry.path)


class LogCollector:
    """In-memory accumulator of write-audit log lines.

    The single-process counterpart to the per-write log files
    :class:`LoggingFileSystem` produces by default: each logged write
    appends one line to a shared, thread-safe buffer and :meth:`flush`
    writes a single consolidated log file at the end, avoiding the
    per-write file fan-out and the O(N) :func:`consolidate_logs` it
    forces. Pass one to every :class:`LoggingFileSystem` whose writes
    should land in the same consolidated log.
    """

    def __init__(self, workflow_name: str, run_date: str, run_label: str = '') -> None:
        self._workflow_name = workflow_name
        self._run_date = run_date
        self._run_label = run_label
        self._lock = threading.Lock()
        self._lines: list[str] = []

    def record(self, line: str) -> None:
        """Buffer one log *line* (no trailing newline). Thread-safe."""
        with self._lock:
            self._lines.append(line)

    def flush(self, log_fs: FileSystem, now: NowFn = _utc_now) -> None:
        """Write the buffered lines as one consolidated log file.

        Lines are sorted (each begins with an ISO timestamp) and written
        to the path :func:`consolidated_log_path` returns — the same path
        :func:`consolidate_logs` produces. A no-op when nothing was
        recorded.
        """
        with self._lock:
            lines = sorted(self._lines)
        if not lines:
            logger.info(
                'No log entries to flush for %s/%s',
                self._run_date,
                self._workflow_name,
            )
            return
        path = consolidated_log_path(
            self._run_date, self._workflow_name, self._run_label, now
        )
        log_fs.write_text(path, '\n'.join(lines) + '\n')
        logger.info('Consolidated %d log entries into %s', len(lines), path)
