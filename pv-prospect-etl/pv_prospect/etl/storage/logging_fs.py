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
    ``<YYYY-MM-DD>/<workflow_name>/<HHMMSSffffff>.txt`` on the *log_fs*.
    The entry contains the UTC timestamp and the fully-qualified path
    (``<label>/<relative_path>``) of the written file.
    """

    def __init__(
        self,
        inner: FileSystem,
        log_fs: FileSystem,
        workflow_name: str,
        label: str,
        now: NowFn = _utc_now,
    ) -> None:
        self._inner = inner
        self._log_fs = log_fs
        self._workflow_name = workflow_name
        self._label = label
        self._now = now

    def __str__(self) -> str:
        return str(self._inner)

    def _log_write(self, path: str) -> None:
        now = self._now()
        date_str = now.strftime('%Y-%m-%d')
        time_str = now.strftime('%H%M%S%f')
        full_path = f'{self._label}/{path}' if self._label else path
        log_path = f'{date_str}/{self._workflow_name}/{time_str}.txt'
        log_content = f'{now.isoformat()} {full_path}\n'
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


def consolidate_logs(
    log_fs: FileSystem,
    workflow_name: str,
    log_date: date,
    now: NowFn = _utc_now,
) -> None:
    """Merge individual log entry files into a single consolidated log.

    Reads all files under ``<YYYY-MM-DD>/<workflow_name>/``, sorts them
    by content (which starts with an ISO timestamp), writes a consolidated
    file at ``<YYYY-MM-DD>/<YYYY-MM-DD-HHMMSS>-<workflow_name>.txt``, and
    deletes the individual entry files.
    """
    date_str = log_date.strftime('%Y-%m-%d')
    prefix = f'{date_str}/{workflow_name}'
    entries = log_fs.list_files(prefix, '*.txt')
    if not entries:
        logger.info('No log entries to consolidate for %s/%s', date_str, workflow_name)
        return

    lines: list[str] = []
    for entry in entries:
        content = log_fs.read_text(entry.path)
        lines.append(content.strip())

    lines.sort()

    consolidated_name = f'{date_str}-{now().strftime("%H%M%S")}-{workflow_name}.txt'
    consolidated_path = f'{date_str}/{consolidated_name}'
    log_fs.write_text(consolidated_path, '\n'.join(lines) + '\n')
    logger.info('Consolidated %d log entries into %s', len(lines), consolidated_path)

    for entry in entries:
        log_fs.delete(entry.path)

    log_fs.rmdir(prefix)
