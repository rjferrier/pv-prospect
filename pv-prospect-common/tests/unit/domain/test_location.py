"""Tests for Location domain model"""
import pytest
from decimal import Decimal
from pv_prospect.common.domain.location import Location


class TestLocationGetCoordinates:
    """Tests for the get_coordinates method"""

    def test_get_coordinates_returns_formatted_string(self):
        """Test that get_coordinates returns latitude,longitude as a string"""
        location = Location.from_floats(latitude=51.5074, longitude=-0.1278)
        assert location.get_coordinates() == "51.5074,-0.1278"

    def test_get_coordinates_with_negative_values(self):
        """Test get_coordinates with negative latitude and longitude"""
        location = Location.from_floats(latitude=-33.8688, longitude=-151.2093)
        assert location.get_coordinates() == "-33.8688,-151.2093"


class TestLocationFromFloats:
    """Tests for the from_floats factory method"""

    def test_from_floats_creates_location_with_decimal_types(self):
        """Test that from_floats creates Location with Decimal types"""
        location = Location.from_floats(latitude=51.5074, longitude=-0.1278)
        assert isinstance(location.latitude, Decimal)
        assert isinstance(location.longitude, Decimal)

    def test_from_floats_rounds_to_four_decimal_places(self):
        """Test that from_floats rounds coordinates to 4 decimal places"""
        location = Location.from_floats(latitude=51.507456789, longitude=-0.127812345)
        assert location.latitude == Decimal('51.5075')
        assert location.longitude == Decimal('-0.1278')


