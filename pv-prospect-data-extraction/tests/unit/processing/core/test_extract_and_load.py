"""Tests for extract_and_load."""

from datetime import date
from unittest.mock import MagicMock

from pv_prospect.common.domain import DateRange
from pv_prospect.data_extraction import DataSource, TimeSeries
from pv_prospect.data_extraction.processing import extract_and_load
from pv_prospect.data_extraction.processing.value_objects import ResultType
from pv_prospect.data_sources import build_time_series_csv_file_path

from .helpers import FakeFileSystem, make_pv_site

_DATE_RANGE = DateRange(date(2025, 6, 1), date(2025, 6, 2))
_PV_SITE = make_pv_site()


def test_dry_run_returns_skipped() -> None:
    result = extract_and_load(
        lambda _: MagicMock(),
        DataSource.PVOUTPUT,
        FakeFileSystem(),
        _PV_SITE,
        _DATE_RANGE,
        dry_run=True,
    )

    assert result.type == ResultType.SKIPPED_DRY_RUN


def test_dry_run_does_not_write() -> None:
    staging_fs = FakeFileSystem()

    extract_and_load(
        lambda _: MagicMock(),
        DataSource.PVOUTPUT,
        staging_fs,
        _PV_SITE,
        _DATE_RANGE,
        dry_run=True,
    )

    assert staging_fs.written_texts == {}


def test_overwrites_when_all_files_exist() -> None:
    expected_path = build_time_series_csv_file_path(
        'timeseries', DataSource.PVOUTPUT, _PV_SITE, _DATE_RANGE
    )

    rows = [['2025-06-01', '100']]
    mock_extractor = MagicMock()
    mock_extractor.extract.return_value = [TimeSeries(entity=_PV_SITE, rows=rows)]

    staging_fs = FakeFileSystem({expected_path: 'old,data'})
    result = extract_and_load(
        lambda _: mock_extractor,
        DataSource.PVOUTPUT,
        staging_fs,
        _PV_SITE,
        _DATE_RANGE,
        dry_run=False,
    )

    assert result.type == ResultType.SUCCESS
    assert staging_fs.written_csv_rows[expected_path] == rows


def test_extracts_and_writes_csv() -> None:
    rows = [['2025-06-01', '100'], ['2025-06-01', '200']]
    mock_extractor = MagicMock()
    mock_extractor.extract.return_value = [TimeSeries(entity=_PV_SITE, rows=rows)]

    staging_fs = FakeFileSystem()
    result = extract_and_load(
        lambda _: mock_extractor,
        DataSource.PVOUTPUT,
        staging_fs,
        _PV_SITE,
        _DATE_RANGE,
        dry_run=False,
    )

    assert result.type == ResultType.SUCCESS
    assert len(staging_fs.written_texts) == 1
    assert list(staging_fs.written_csv_rows.values())[0] == rows


def test_failure_on_extraction_error() -> None:
    mock_extractor = MagicMock()
    mock_extractor.extract.side_effect = RuntimeError('API timeout')

    result = extract_and_load(
        lambda _: mock_extractor,
        DataSource.PVOUTPUT,
        FakeFileSystem(),
        _PV_SITE,
        _DATE_RANGE,
        dry_run=False,
    )

    assert result.type == ResultType.FAILURE
    assert result.failure_details is not None
    assert 'API timeout' in str(result.failure_details.error)
