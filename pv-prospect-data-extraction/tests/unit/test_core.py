"""Tests for core extraction logic."""

import io
from datetime import date
from decimal import Decimal
from typing import Iterable
from unittest.mock import MagicMock, patch

from pv_prospect.common import DateRange
from pv_prospect.common.domain.location import Location
from pv_prospect.common.domain.pv_site import (
    PanelGeometry,
    PVSite,
    Shading,
    System,
)
from pv_prospect.data_extraction.extractors import SourceDescriptor
from pv_prospect.data_extraction.extractors.base import TimeSeries
from pv_prospect.data_extraction.processing.core import (
    _build_csv_file_path,
    _format_date,
    extract_and_load,
    preprocess,
)
from pv_prospect.data_extraction.processing.value_objects import ResultType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeTimeSeriesDescriptor:
    def __init__(self, name: str) -> None:
        self._name = name

    def __str__(self) -> str:
        return self._name


def _make_pv_site(pvo_sys_id: int = 42248) -> PVSite:
    return PVSite(
        pvo_sys_id=pvo_sys_id,
        name='Test Site',
        location=Location(latitude=Decimal('51.6'), longitude=Decimal('-4.2')),
        shading=Shading.NONE,
        panel_system=System(brand='TestBrand', capacity=4000),
        panel_geometries=[PanelGeometry(azimuth=180, tilt=35, area_fraction=1.0)],
        inverter_system=System(brand='TestInverter', capacity=3600),
    )


class FakeExtractor:
    """Minimal Extractor that returns canned file contents."""

    def __init__(self, files: dict[str, str] | None = None) -> None:
        self._files = files or {}

    def read_file(self, file_path: str) -> io.TextIOWrapper:  # type: ignore[override]
        if file_path not in self._files:
            raise FileNotFoundError(file_path)
        return io.StringIO(self._files[file_path])  # type: ignore[return-value]

    def file_exists(self, file_path: str) -> bool:
        return file_path in self._files


class FakeLoader:
    """Minimal Loader that records calls."""

    def __init__(self, existing_files: set[str] | None = None) -> None:
        self.created_folders: list[str] = []
        self.written_texts: dict[str, str] = {}
        self.written_csvs: dict[str, list[list[str]]] = {}
        self._existing = existing_files or set()

    def create_folder(self, folder_path: str) -> str | None:
        self.created_folders.append(folder_path)
        return folder_path

    def file_exists(self, file_path: str) -> bool:
        return file_path in self._existing

    def write_text(self, file_path: str, text: str, overwrite: bool = False) -> None:
        self.written_texts[file_path] = text

    def write_csv(
        self, file_path: str, rows: Iterable[Iterable[str]], overwrite: bool = False
    ) -> None:
        self.written_csvs[file_path] = [list(row) for row in rows]


# ---------------------------------------------------------------------------
# _format_date / _build_csv_file_path
# ---------------------------------------------------------------------------


class TestFormatDate:
    def test_standard_date(self) -> None:
        assert _format_date(date(2025, 6, 1)) == '20250601'

    def test_pads_month_and_day(self) -> None:
        assert _format_date(date(2025, 1, 9)) == '20250109'


class TestBuildCsvFilePath:
    def test_builds_expected_path(self) -> None:
        descriptor = FakeTimeSeriesDescriptor('temperature')
        path = _build_csv_file_path(
            'timeseries',
            SourceDescriptor.OPENMETEO_QUARTERHOURLY,
            descriptor,
            date(2025, 6, 1),
        )
        assert (
            path
            == 'timeseries/openmeteo/quarterhourly/openmeteo-quarterhourly_temperature_20250601.csv'
        )


# ---------------------------------------------------------------------------
# preprocess
# ---------------------------------------------------------------------------


class TestPreprocess:
    @patch('pv_prospect.data_extraction.processing.core.resolve_path')
    def test_creates_timeseries_folder(self, mock_resolve: MagicMock) -> None:
        mock_resolve.return_value = 'blob/path'
        loader = FakeLoader(existing_files={'pv_sites.csv', 'location_mapping.csv'})

        preprocess(
            SourceDescriptor.PVOUTPUT,
            FakeExtractor(),
            loader,
            dvc_prefix='resources',
        )

        assert loader.created_folders == ['timeseries/pvoutput']

    @patch('pv_prospect.data_extraction.processing.core.resolve_path')
    def test_provisions_missing_resources(self, mock_resolve: MagicMock) -> None:
        mock_resolve.side_effect = lambda path: f'resolved:{path}'
        versioned_extractor = FakeExtractor(
            {
                'resolved:resources/pv_sites.csv.dvc': 'sites-content',
                'resolved:resources/location_mapping.csv.dvc': 'mapping-content',
            }
        )
        loader = FakeLoader()

        preprocess(
            SourceDescriptor.PVOUTPUT,
            versioned_extractor,
            loader,
            dvc_prefix='resources',
        )

        assert 'pv_sites.csv' in loader.written_texts
        assert loader.written_texts['pv_sites.csv'] == 'sites-content'
        assert 'location_mapping.csv' in loader.written_texts
        assert loader.written_texts['location_mapping.csv'] == 'mapping-content'

    @patch('pv_prospect.data_extraction.processing.core.resolve_path')
    def test_skips_existing_resources(self, mock_resolve: MagicMock) -> None:
        mock_resolve.return_value = 'blob/path'
        loader = FakeLoader(existing_files={'pv_sites.csv', 'location_mapping.csv'})

        preprocess(
            SourceDescriptor.PVOUTPUT,
            FakeExtractor(),
            loader,
            dvc_prefix='resources',
        )

        assert loader.written_texts == {}

    @patch('pv_prospect.data_extraction.processing.core.resolve_path')
    def test_returns_folder_ids(self, mock_resolve: MagicMock) -> None:
        mock_resolve.return_value = 'blob/path'
        loader = FakeLoader(existing_files={'pv_sites.csv', 'location_mapping.csv'})

        result = preprocess(
            SourceDescriptor.PVOUTPUT,
            FakeExtractor(),
            loader,
            dvc_prefix='resources',
        )

        assert result == ['timeseries/pvoutput']


# ---------------------------------------------------------------------------
# extract_and_load
# ---------------------------------------------------------------------------


class TestExtractAndLoadDryRun:
    def test_returns_skipped_dry_run(self) -> None:
        result = extract_and_load(
            source_descriptor=SourceDescriptor.PVOUTPUT,
            pv_system_id=42248,
            date_range=DateRange(date(2025, 6, 1), date(2025, 6, 2)),
            resources_extractor=FakeExtractor(),
            time_series_loader=FakeLoader(),
            overwrite=False,
            dry_run=True,
        )

        assert result.type == ResultType.SKIPPED_DRY_RUN

    def test_does_not_write(self) -> None:
        loader = FakeLoader()

        extract_and_load(
            source_descriptor=SourceDescriptor.PVOUTPUT,
            pv_system_id=42248,
            date_range=DateRange(date(2025, 6, 1), date(2025, 6, 2)),
            resources_extractor=FakeExtractor(),
            time_series_loader=loader,
            overwrite=False,
            dry_run=True,
        )

        assert loader.written_csvs == {}


class TestExtractAndLoadSkipsExisting:
    @patch('pv_prospect.data_extraction.processing.core.get_pv_site_by_system_id')
    @patch('pv_prospect.data_extraction.processing.core.get_extractor')
    def test_returns_skipped_when_all_files_exist(
        self, mock_get_extractor: MagicMock, mock_get_site: MagicMock
    ) -> None:
        pv_site = _make_pv_site()
        mock_get_site.return_value = pv_site

        ts_desc = FakeTimeSeriesDescriptor('power')
        mock_data_extractor = MagicMock()
        mock_data_extractor.get_time_series_descriptors.return_value = [ts_desc]
        mock_get_extractor.return_value = mock_data_extractor

        # The expected CSV path for this descriptor already exists
        expected_path = _build_csv_file_path(
            'timeseries', SourceDescriptor.PVOUTPUT, ts_desc, date(2025, 6, 1)
        )
        resources_extractor = FakeExtractor({expected_path: ''})

        result = extract_and_load(
            source_descriptor=SourceDescriptor.PVOUTPUT,
            pv_system_id=42248,
            date_range=DateRange(date(2025, 6, 1), date(2025, 6, 2)),
            resources_extractor=resources_extractor,
            time_series_loader=FakeLoader(),
            overwrite=False,
            dry_run=False,
        )

        assert result.type == ResultType.SKIPPED_EXISTING


class TestExtractAndLoadSuccess:
    @patch('pv_prospect.data_extraction.processing.core.get_pv_site_by_system_id')
    @patch('pv_prospect.data_extraction.processing.core.get_extractor')
    def test_extracts_and_writes_csv(
        self, mock_get_extractor: MagicMock, mock_get_site: MagicMock
    ) -> None:
        pv_site = _make_pv_site()
        mock_get_site.return_value = pv_site

        ts_desc = FakeTimeSeriesDescriptor('power')
        rows = [['2025-06-01', '100'], ['2025-06-01', '200']]
        mock_data_extractor = MagicMock()
        mock_data_extractor.get_time_series_descriptors.return_value = [ts_desc]
        mock_data_extractor.extract.return_value = [
            TimeSeries(descriptor=ts_desc, rows=rows)
        ]
        mock_get_extractor.return_value = mock_data_extractor

        loader = FakeLoader()
        result = extract_and_load(
            source_descriptor=SourceDescriptor.PVOUTPUT,
            pv_system_id=42248,
            date_range=DateRange(date(2025, 6, 1), date(2025, 6, 2)),
            resources_extractor=FakeExtractor(),
            time_series_loader=loader,
            overwrite=False,
            dry_run=False,
        )

        assert result.type == ResultType.SUCCESS
        assert len(loader.written_csvs) == 1
        written_rows = list(loader.written_csvs.values())[0]
        assert written_rows == rows

    @patch('pv_prospect.data_extraction.processing.core.get_pv_site_by_system_id')
    @patch('pv_prospect.data_extraction.processing.core.get_extractor')
    def test_overwrites_existing_when_flag_set(
        self, mock_get_extractor: MagicMock, mock_get_site: MagicMock
    ) -> None:
        pv_site = _make_pv_site()
        mock_get_site.return_value = pv_site

        ts_desc = FakeTimeSeriesDescriptor('power')
        mock_data_extractor = MagicMock()
        mock_data_extractor.get_time_series_descriptors.return_value = [ts_desc]
        mock_data_extractor.extract.return_value = [
            TimeSeries(descriptor=ts_desc, rows=[['row']])
        ]
        mock_get_extractor.return_value = mock_data_extractor

        # File already exists, but overwrite=True
        expected_path = _build_csv_file_path(
            'timeseries', SourceDescriptor.PVOUTPUT, ts_desc, date(2025, 6, 1)
        )
        resources_extractor = FakeExtractor({expected_path: ''})

        loader = FakeLoader()
        result = extract_and_load(
            source_descriptor=SourceDescriptor.PVOUTPUT,
            pv_system_id=42248,
            date_range=DateRange(date(2025, 6, 1), date(2025, 6, 2)),
            resources_extractor=resources_extractor,
            time_series_loader=loader,
            overwrite=True,
            dry_run=False,
        )

        assert result.type == ResultType.SUCCESS
        assert len(loader.written_csvs) == 1


class TestExtractAndLoadFailure:
    @patch('pv_prospect.data_extraction.processing.core.get_pv_site_by_system_id')
    @patch('pv_prospect.data_extraction.processing.core.get_extractor')
    def test_returns_failure_when_site_not_found(
        self, mock_get_extractor: MagicMock, mock_get_site: MagicMock
    ) -> None:
        mock_get_extractor.return_value = MagicMock()
        mock_get_site.return_value = None

        result = extract_and_load(
            source_descriptor=SourceDescriptor.PVOUTPUT,
            pv_system_id=99999,
            date_range=DateRange(date(2025, 6, 1), date(2025, 6, 2)),
            resources_extractor=FakeExtractor(),
            time_series_loader=FakeLoader(),
            overwrite=False,
            dry_run=False,
        )

        assert result.type == ResultType.FAILURE
        assert result.failure_details is not None
        assert 'Unable to retrieve PVSite' in str(result.failure_details.error)

    @patch('pv_prospect.data_extraction.processing.core.get_pv_site_by_system_id')
    @patch('pv_prospect.data_extraction.processing.core.get_extractor')
    def test_returns_failure_on_extraction_error(
        self, mock_get_extractor: MagicMock, mock_get_site: MagicMock
    ) -> None:
        pv_site = _make_pv_site()
        mock_get_site.return_value = pv_site

        ts_desc = FakeTimeSeriesDescriptor('power')
        mock_data_extractor = MagicMock()
        mock_data_extractor.get_time_series_descriptors.return_value = [ts_desc]
        mock_data_extractor.extract.side_effect = RuntimeError('API timeout')
        mock_get_extractor.return_value = mock_data_extractor

        result = extract_and_load(
            source_descriptor=SourceDescriptor.PVOUTPUT,
            pv_system_id=42248,
            date_range=DateRange(date(2025, 6, 1), date(2025, 6, 2)),
            resources_extractor=FakeExtractor(),
            time_series_loader=FakeLoader(),
            overwrite=False,
            dry_run=False,
        )

        assert result.type == ResultType.FAILURE
        assert result.failure_details is not None
        assert 'API timeout' in str(result.failure_details.error)
