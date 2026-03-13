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

from .helpers import FakeExtractor, FakeLoader, FakeTimeSeriesDescriptor, make_pv_site

_DATE_RANGE = DateRange(date(2025, 6, 1), date(2025, 6, 2))


def test_dry_run_returns_skipped() -> None:
    result = extract_and_load(
        lambda _: make_pv_site(),
        lambda _: MagicMock(),
        SourceDescriptor.PVOUTPUT,
        FakeExtractor(),
        FakeLoader(),
        42248,
        _DATE_RANGE,
        overwrite=False,
        dry_run=True,
    )

    assert result.type == ResultType.SKIPPED_DRY_RUN


def test_dry_run_does_not_write() -> None:
    loader = FakeLoader()

    extract_and_load(
        lambda _: make_pv_site(),
        lambda _: MagicMock(),
        SourceDescriptor.PVOUTPUT,
        FakeExtractor(),
        loader,
        42248,
        _DATE_RANGE,
        overwrite=False,
        dry_run=True,
    )

    assert loader.written_csvs == {}


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
        FakeExtractor({expected_path: ''}),
        FakeLoader(),
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

    loader = FakeLoader()
    result = extract_and_load(
        lambda _: make_pv_site(),
        lambda _: mock_data_extractor,
        SourceDescriptor.PVOUTPUT,
        FakeExtractor(),
        loader,
        42248,
        _DATE_RANGE,
        overwrite=False,
        dry_run=False,
    )

    assert result.type == ResultType.SUCCESS
    assert len(loader.written_csvs) == 1
    assert list(loader.written_csvs.values())[0] == rows


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

    loader = FakeLoader()
    result = extract_and_load(
        lambda _: make_pv_site(),
        lambda _: mock_data_extractor,
        SourceDescriptor.PVOUTPUT,
        FakeExtractor({expected_path: ''}),
        loader,
        42248,
        _DATE_RANGE,
        overwrite=True,
        dry_run=False,
    )

    assert result.type == ResultType.SUCCESS
    assert len(loader.written_csvs) == 1


def test_failure_when_site_not_found() -> None:
    result = extract_and_load(
        lambda _: None,
        lambda _: MagicMock(),
        SourceDescriptor.PVOUTPUT,
        FakeExtractor(),
        FakeLoader(),
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
        FakeExtractor(),
        FakeLoader(),
        42248,
        _DATE_RANGE,
        overwrite=False,
        dry_run=False,
    )

    assert result.type == ResultType.FAILURE
    assert result.failure_details is not None
    assert 'API timeout' in str(result.failure_details.error)
