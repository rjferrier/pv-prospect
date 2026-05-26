"""Tests for get_logging_filesystem."""

import tempfile

from pv_prospect.etl import get_logging_filesystem
from pv_prospect.etl.storage import LogCollector, LoggingFileSystem
from pv_prospect.etl.storage.backends.local import LocalStorageConfig


def _local_config(prefix: str) -> LocalStorageConfig:
    return LocalStorageConfig(prefix=prefix)


def test_returns_logging_filesystem_when_workflow_and_log_storage_configured() -> None:
    with (
        tempfile.TemporaryDirectory() as data_dir,
        tempfile.TemporaryDirectory() as log_dir,
    ):
        fs = get_logging_filesystem(
            _local_config(data_dir),
            _local_config(log_dir),
            workflow_name='wf',
            run_date='2025-06-24',
            label='raw',
            run_label='',
        )

        assert isinstance(fs, LoggingFileSystem)


def test_returns_plain_filesystem_when_log_storage_missing() -> None:
    with tempfile.TemporaryDirectory() as data_dir:
        fs = get_logging_filesystem(
            _local_config(data_dir),
            log_storage=None,
            workflow_name='wf',
            run_date='2025-06-24',
            label='raw',
            run_label='',
        )

        assert not isinstance(fs, LoggingFileSystem)


def test_returns_plain_filesystem_when_workflow_name_missing() -> None:
    with (
        tempfile.TemporaryDirectory() as data_dir,
        tempfile.TemporaryDirectory() as log_dir,
    ):
        fs = get_logging_filesystem(
            _local_config(data_dir),
            _local_config(log_dir),
            workflow_name='',
            run_date='2025-06-24',
            label='raw',
            run_label='',
        )

        assert not isinstance(fs, LoggingFileSystem)


def test_routes_writes_through_log_collector_when_supplied() -> None:
    """A supplied LogCollector receives the write-audit lines in memory
    instead of the LoggingFileSystem fanning them out as per-write files."""
    with (
        tempfile.TemporaryDirectory() as data_dir,
        tempfile.TemporaryDirectory() as log_dir,
    ):
        collector = LogCollector('wf', '2025-06-24')
        fs = get_logging_filesystem(
            _local_config(data_dir),
            _local_config(log_dir),
            workflow_name='wf',
            run_date='2025-06-24',
            label='raw',
            run_label='',
            log_collector=collector,
        )

        fs.write_text('a.csv', 'data')

        # The collector's internal buffer captured the breadcrumb; the
        # log filesystem on disk got nothing.
        collector.flush_per_task(
            get_logging_filesystem(
                _local_config(log_dir),
                log_storage=None,
                workflow_name='wf',
                run_date='2025-06-24',
                label='log',
                run_label='',
            ),
            task_hash='batch-1',
        )
