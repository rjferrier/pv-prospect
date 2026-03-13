"""Tests for build_csv_file_path."""

from datetime import date

from pv_prospect.data_extraction import SourceDescriptor
from pv_prospect.data_extraction.processing.core import build_csv_file_path

from .helpers import FakeTimeSeriesDescriptor


def test_builds_expected_path() -> None:
    descriptor = FakeTimeSeriesDescriptor('temperature')
    path = build_csv_file_path(
        'timeseries',
        SourceDescriptor.OPENMETEO_QUARTERHOURLY,
        descriptor,
        date(2025, 6, 1),
    )
    assert (
        path
        == 'timeseries/openmeteo/quarterhourly/openmeteo-quarterhourly_temperature_20250601.csv'
    )
