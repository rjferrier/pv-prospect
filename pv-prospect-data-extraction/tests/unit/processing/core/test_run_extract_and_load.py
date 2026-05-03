"""Tests for _run_extract_and_load exit behaviour on task failure.

These test the entrypoint-level sys.exit(1) behaviour when one or more
extract_and_load tasks return a failure result, which is distinct from the
core.extract_and_load unit tests that only verify the Result value object.
"""

from unittest.mock import MagicMock, patch

import pytest
from pv_prospect.data_extraction import DataSource
from pv_prospect.data_extraction.processing import core
from pv_prospect.data_extraction.processing.entrypoint import _run_extract_and_load
from pv_prospect.data_extraction.processing.value_objects import Result

from .helpers import FakeFileSystem, make_pv_site

_PV_SITE = make_pv_site()
_DATA_SOURCE = DataSource.PVOUTPUT


def _success() -> Result:
    return Result.success(MagicMock())


def _failure() -> Result:
    return Result.failure(MagicMock(), RuntimeError('API error'))


def _run(monkeypatch: pytest.MonkeyPatch, result: Result) -> None:
    """Call _run_extract_and_load with one site/date-range, returning *result*."""
    monkeypatch.setenv('START_DATE', '2025-06-01')
    monkeypatch.setenv('PV_SYSTEM_ID', '42248')

    with (
        patch(
            'pv_prospect.data_extraction.processing.entrypoint._resolve_sites',
            return_value=[_PV_SITE],
        ),
        patch.object(core, 'extract_and_load', return_value=result),
        patch('pv_prospect.data_extraction.processing.entrypoint.build_pv_site_repo'),
        patch('pv_prospect.data_extraction.processing.entrypoint.Extractor'),
    ):
        _run_extract_and_load(FakeFileSystem(), FakeFileSystem(), _DATA_SOURCE)


def test_exits_with_1_when_task_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(SystemExit) as exc_info:
        _run(monkeypatch, _failure())
    assert exc_info.value.code == 1


def test_does_not_exit_when_task_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    # Should complete without raising SystemExit.
    _run(monkeypatch, _success())
