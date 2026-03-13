"""Tests for extract_and_load."""

from datetime import date
from unittest.mock import MagicMock

from pv_prospect.common import DateRange
from pv_prospect.data_extraction.extractors import SourceDescriptor
from pv_prospect.data_extraction.extractors.base import TimeSeries
from pv_prospect.data_extraction.processing.core import (
    build_csv_file_path,
    extract_and_load,
)
from pv_prospect.data_extraction.processing.value_objects import ResultType

from .helpers import FakeFileSystem, FakeTimeSeriesDescriptor, make_pv_site

_DATE_RANGE = DateRange(date(2025, 6, 1), date(2025, 6, 2))


def test_dry_run_returns_skipped() -> None:
    result = extract_and_load(
        lambda _: make_pv_site(),
        lambda _: MagicMock(),
        SourceDescriptor.PVOUTPUT,
        FakeFileSystem(),
        42248,
        _DATE_RANGE,
        overwrite=False,
        dry_run=True,
    )

    assert result.type == ResultType.SKIPPED_DRY_RUN


def test_dry_run_does_not_write() -> None:
    staging_fs = FakeFileSystem()

    extract_and_load(
        lambda _: make_pv_site(),
        lambda _: MagicMock(),
        SourceDescriptor.PVOUTPUT,
        staging_fs,
        42248,
        _DATE_RANGE,
        overwrite=False,
        dry_run=True,
    )

    assert staging_fs.written_texts == {}


def test_skips_when_all_files_exist() -> None:
    ts_desc = FakeTimeSeriesDescriptor('power')
    mock_data_extractor = MagicMock()
    mock_data_extractor.get_time_series_descriptors.return_value = [ts_desc]

    expected_path = build_csv_file_path(
        'timeseries', SourceDescriptor.PVOUTPUT, ts_desc, date(2025, 6, 1)
    )

    result = extract_and_load(
        lambda _: make_pv_site(),
        lambda _: mock_data_extractor,
        SourceDescriptor.PVOUTPUT,
        FakeFileSystem({expected_path: ''}),
        42248,
        _DATE_RANGE,
        overwrite=False,
        dry_run=False,
    )

    assert result.type == ResultType.SKIPPED_EXISTING


def test_extracts_and_writes_csv() -> None:
    ts_desc = FakeTimeSeriesDescriptor('power')
    rows = [['2025-06-01', '100'], ['2025-06-01', '200']]
    mock_data_extractor = MagicMock()
    mock_data_extractor.get_time_series_descriptors.return_value = [ts_desc]
    mock_data_extractor.extract.return_value = [
        TimeSeries(descriptor=ts_desc, rows=rows)
    ]

    staging_fs = FakeFileSystem()
    result = extract_and_load(
        lambda _: make_pv_site(),
        lambda _: mock_data_extractor,
        SourceDescriptor.PVOUTPUT,
        staging_fs,
        42248,
        _DATE_RANGE,
        overwrite=False,
        dry_run=False,
    )

    assert result.type == ResultType.SUCCESS
    assert len(staging_fs.written_texts) == 1
    assert list(staging_fs.written_csv_rows.values())[0] == rows


def test_overwrites_existing_when_flag_set() -> None:
    ts_desc = FakeTimeSeriesDescriptor('power')
    mock_data_extractor = MagicMock()
    mock_data_extractor.get_time_series_descriptors.return_value = [ts_desc]
    mock_data_extractor.extract.return_value = [
        TimeSeries(descriptor=ts_desc, rows=[['row']])
    ]

    expected_path = build_csv_file_path(
        'timeseries', SourceDescriptor.PVOUTPUT, ts_desc, date(2025, 6, 1)
    )

    staging_fs = FakeFileSystem({expected_path: ''})
    result = extract_and_load(
        lambda _: make_pv_site(),
        lambda _: mock_data_extractor,
        SourceDescriptor.PVOUTPUT,
        staging_fs,
        42248,
        _DATE_RANGE,
        overwrite=True,
        dry_run=False,
    )

    assert result.type == ResultType.SUCCESS
    assert len(staging_fs.written_texts) == 1


def test_failure_when_site_not_found() -> None:
    result = extract_and_load(
        lambda _: None,
        lambda _: MagicMock(),
        SourceDescriptor.PVOUTPUT,
        FakeFileSystem(),
        99999,
        _DATE_RANGE,
        overwrite=False,
        dry_run=False,
    )

    assert result.type == ResultType.FAILURE
    assert result.failure_details is not None
    assert 'Unable to retrieve PVSite' in str(result.failure_details.error)


def test_failure_on_extraction_error() -> None:
    ts_desc = FakeTimeSeriesDescriptor('power')
    mock_data_extractor = MagicMock()
    mock_data_extractor.get_time_series_descriptors.return_value = [ts_desc]
    mock_data_extractor.extract.side_effect = RuntimeError('API timeout')

    result = extract_and_load(
        lambda _: make_pv_site(),
        lambda _: mock_data_extractor,
        SourceDescriptor.PVOUTPUT,
        FakeFileSystem(),
        42248,
        _DATE_RANGE,
        overwrite=False,
        dry_run=False,
    )

    assert result.type == ResultType.FAILURE
    assert result.failure_details is not None
    assert 'API timeout' in str(result.failure_details.error)
