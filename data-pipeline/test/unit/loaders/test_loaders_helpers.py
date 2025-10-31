"""Module-style unit tests for loader helper functions."""

import os
from datetime import date

from loaders.helpers import build_csv_file_path, format_date
from extractors.data_sources import SourceDescriptor


def test_format_date():
    """Test date formatting to YYYYMMDD."""
    assert format_date(date(2024, 1, 15)) == '20240115'
    assert format_date(date(2024, 12, 31)) == '20241231'
    assert format_date(date(2024, 1, 1)) == '20240101'


def test_build_csv_file_path_simple_descriptor():
    """Test building file path with simple descriptor."""
    result = build_csv_file_path(
        SourceDescriptor.PVOUTPUT,
        12345,
        date(2024, 1, 15)
    )
    expected = os.path.join('data', 'pvoutput', 'pvoutput_12345_20240115.csv')
    assert result == expected


def test_build_csv_file_path_nested_descriptor():
    """Test building file path with nested descriptor (contains slash)."""
    result = build_csv_file_path(
        SourceDescriptor.OPENMETEO_HOURLY,
        67890,
        date(2024, 10, 29)
    )
    expected = os.path.join('data', 'openmeteo/hourly', 'openmeteo-hourly_67890_20241029.csv')
    assert result == expected


def test_build_csv_file_path_multiple_slashes():
    """Test building file path with multiple slashes in descriptor."""
    result = build_csv_file_path(
        SourceDescriptor.OPENMETEO_V0_QUARTERHOURLY,
        11111,
        date(2024, 2, 29)
    )
    expected = os.path.join('data', 'openmeteo/v0/quarterhourly', 'openmeteo-v0-quarterhourly_11111_20240229.csv')
    assert result == expected

