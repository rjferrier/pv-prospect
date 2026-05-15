"""FileSystem decorator that logs write operations to a separate filesystem."""

import logging
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
    """

    def __init__(
        self,
        inner: FileSystem,
        log_fs: FileSystem,
        workflow_name: str,
        run_date: str,
        label: str,
        run_label: str = '',
        now: NowFn = _utc_now,
    ) -> None:
        self._inner = inner
        self._log_fs = log_fs
        self._workflow_name = workflow_name
        self._run_date = run_date
        self._label = label
        self._run_label = run_label
        self._now = now

    def __str__(self) -> str:
        return str(self._inner)

    def _log_write(self, path: str) -> None:
        now = self._now()
        time_str = now.strftime('%H%M%S%f')
        full_path = f'{self._label}/{path}' if self._label else path
        log_path = (
            f'{self._run_date}/'
            f'{log_scratch_subprefix(self._workflow_name, self._run_label)}/'
            f'{time_str}.txt'
        )
        log_content = f'{now.isoformat()} CREATED {full_path}\n'
        try:
            self._log_fs.write_text(log_path, log_content)
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
    consolidated file at
    ``<run_date>/<run_date>-<HHMMSS>-<workflow_name>[-<run_label>].txt``,
    and deletes the individual entry files. *run_label* keeps concurrent
    same-day executions disjoint at the scratch layer; the consolidated
    layer encodes it in the filename so listings can still distinguish
    same-day runs.
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

    consolidated_stem = f'{run_label}-{workflow_name}' if run_label else workflow_name
    consolidated_name = f'{date_str}-{now().strftime("%H%M%S")}-{consolidated_stem}.txt'
    consolidated_path = f'{date_str}/{consolidated_name}'
    log_fs.write_text(consolidated_path, '\n'.join(lines) + '\n')
    logger.info('Consolidated %d log entries into %s', len(lines), consolidated_path)

    for entry in entries:
        log_fs.delete(entry.path)
