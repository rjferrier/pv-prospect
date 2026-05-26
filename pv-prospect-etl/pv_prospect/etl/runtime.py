"""Cloud Run Job runtime helpers shared across extract and transform.

The helpers here build the per-execution filesystem handles and run the
end-of-workflow consolidation step. They live in the ETL library so the
extract and transform packages share one implementation rather than each
maintaining its own near-identical copy.

The pieces:

- :func:`resolve_run_date`: pull the workflow's UTC trigger date from
  ``RUN_DATE``, falling back to today's date for local one-off
  invocations.
- :func:`get_logging_filesystem`: wrap a staged-data filesystem in
  :class:`~pv_prospect.etl.storage.LoggingFileSystem` when a workflow
  name and log-storage backend are configured; optionally route writes
  through an in-memory :class:`~pv_prospect.etl.storage.LogCollector` to
  avoid the per-write file fan-out.
- :func:`run_consolidate_logs`: invoke
  :func:`~pv_prospect.etl.storage.consolidate_logs` and
  :func:`~pv_prospect.etl.storage.consolidate_ledger` for the end-of-run
  ``consolidate_logs`` task.
"""

import logging
import os
from datetime import date

from pv_prospect.etl.storage import (
    AnyStorageConfig,
    FileSystem,
    LogCollector,
    LoggingFileSystem,
    consolidate_ledger,
    consolidate_logs,
    get_filesystem,
)

logger = logging.getLogger(__name__)


def resolve_run_date() -> str:
    """Return the workflow's UTC trigger date.

    Read from ``RUN_DATE`` (set once by the Cloud Workflow ``init`` step
    and propagated to every task); fall back to :func:`date.today` for
    local one-off invocations.
    """
    return os.environ.get('RUN_DATE') or date.today().isoformat()


def get_logging_filesystem(
    storage_config: AnyStorageConfig,
    log_storage: AnyStorageConfig | None,
    workflow_name: str,
    run_date: str,
    label: str,
    run_label: str,
    log_collector: LogCollector | None = None,
) -> FileSystem:
    """Return a filesystem for *storage_config*, optionally write-logged.

    When both ``workflow_name`` and ``log_storage`` are set, the returned
    filesystem is a :class:`LoggingFileSystem` decorating the underlying
    backend so every write also produces a write-audit log entry. With
    ``log_collector`` set, those log entries are buffered in memory
    instead of written as individual files (the single-process path —
    avoids the per-write GCS file fan-out).

    When ``workflow_name`` or ``log_storage`` is missing, write-logging
    is disabled and the underlying filesystem is returned directly.
    """
    fs: FileSystem = get_filesystem(storage_config)
    if workflow_name and log_storage:
        log_fs = get_filesystem(log_storage)
        return LoggingFileSystem(
            fs,
            log_fs,
            workflow_name,
            run_date,
            label,
            run_label=run_label,
            log_collector=log_collector,
        )
    logger.warning(
        'Write-logging disabled for %s (no WORKFLOW_NAME or log_storage)', label
    )
    return fs


def run_consolidate_logs(
    log_storage: AnyStorageConfig | None,
    ledger_storage: AnyStorageConfig | None,
    workflow_name: str,
    run_date: str,
    run_label: str = '',
) -> None:
    """Run the end-of-workflow consolidation for write-audit logs and ledger.

    Merges the per-task scratch files written during the run into the
    daily consolidated files. No-op for either stream whose storage
    backend is unconfigured; warns and returns early when
    ``workflow_name`` is missing (without it the merge target would be
    ambiguous).
    """
    if not workflow_name:
        logger.warning('consolidate_logs: workflow_name not configured')
        return
    run_date_obj = date.fromisoformat(run_date)
    if log_storage:
        log_fs = get_filesystem(log_storage)
        consolidate_logs(log_fs, workflow_name, run_date_obj, run_label=run_label)
    if ledger_storage:
        ledger_fs = get_filesystem(ledger_storage)
        consolidate_ledger(ledger_fs, workflow_name, run_date_obj, run_label=run_label)
