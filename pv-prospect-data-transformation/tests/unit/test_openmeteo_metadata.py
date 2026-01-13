"""Tests for OpenMeteoFileMetadata to verify Decimal latitude/longitude."""
from decimal import Decimal
from datetime import date

from pv_prospect.data_transformation.helpers.file_metadata import OpenMeteoFileMetadata


def test_openmeteo_metadata_coordinates_are_decimals():
    """Test that latitude and longitude are returned as Decimals with 4 decimal places."""
    # Test case: openmeteo-historical_526604_07799_20260112.csv
    filename = 'openmeteo-historical_526604_07799_20260112.csv'

    metadata = OpenMeteoFileMetadata.from_filename(filename)

    assert metadata is not None
    assert isinstance(metadata.latitude, Decimal)
    assert isinstance(metadata.longitude, Decimal)
    assert metadata.latitude == Decimal('52.6604')
    assert metadata.longitude == Decimal('0.7799')
    assert metadata.from_date == date(2026, 1, 12)


def test_openmeteo_metadata_negative_coordinates():
    """Test that negative coordinates are handled correctly as Decimals."""
    # Test case: openmeteo-historical_-41776_-123456_20260112.csv
    filename = 'openmeteo-historical_-41776_-123456_20260112.csv'

    metadata = OpenMeteoFileMetadata.from_filename(filename)

    assert metadata is not None
    assert isinstance(metadata.latitude, Decimal)
    assert isinstance(metadata.longitude, Decimal)
    assert metadata.latitude == Decimal('-4.1776')
    assert metadata.longitude == Decimal('-12.3456')


def test_openmeteo_metadata_small_coordinates():
    """Test that small coordinates (< 1 degree) are handled correctly as Decimals."""
    # Test case: coordinates less than 1 degree
    filename = 'openmeteo-historical_5604_799_20260112.csv'

    metadata = OpenMeteoFileMetadata.from_filename(filename)

    assert metadata is not None
    assert isinstance(metadata.latitude, Decimal)
    assert isinstance(metadata.longitude, Decimal)
    assert metadata.latitude == Decimal('0.5604')
    assert metadata.longitude == Decimal('0.0799')

