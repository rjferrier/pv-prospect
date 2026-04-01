"""Tests for Location domain model."""

from decimal import Decimal

from pv_prospect.common.domain import Location


def test_get_coordinates_returns_formatted_string():
    location = Location.from_coordinates('51.5074', '-0.1278')

    assert location.to_coordinate_string() == '51.5074,-0.1278'


def test_get_coordinates_with_negative_values():
    location = Location.from_coordinates('-33.8688', '-151.2093')

    assert location.to_coordinate_string() == '-33.8688,-151.2093'


def test_from_coordinates_with_strings():
    location = Location.from_coordinates('51.5074', '-0.1278')

    assert isinstance(location.latitude, Decimal)
    assert isinstance(location.longitude, Decimal)


def test_from_coordinates_with_decimals():
    location = Location.from_coordinates(Decimal('51.5074'), Decimal('-0.1278'))

    assert location.latitude == Decimal('51.5074')
    assert location.longitude == Decimal('-0.1278')


def test_from_coordinates_rounds_to_four_decimal_places():
    location = Location.from_coordinates('51.507456789', '-0.127812345')

    assert location.latitude == Decimal('51.5075')
    assert location.longitude == Decimal('-0.1278')


def test_get_filename_friendly_coords_positive_coordinates():
    location = Location.from_coordinates(Decimal('52.6604'), Decimal('0.7808'))

    assert location.to_coordinate_string(filename_friendly=True) == '526604_07808'


def test_get_filename_friendly_coords_negative_longitude():
    location = Location.from_coordinates(Decimal('50.49'), Decimal('-3.54'))

    assert location.to_coordinate_string(filename_friendly=True) == '504900_-35400'


def test_from_filename_friendly_coords_restores_coordinates():
    location = Location.from_coordinate_string('504900_-35400', filename_friendly=True)

    assert location.latitude == Decimal('50.49')
    assert location.longitude == Decimal('-3.54')


def test_from_filename_friendly_coords_with_leading_zero_longitude():
    location = Location.from_coordinate_string('526404_07808', filename_friendly=True)

    assert location.latitude == Decimal('52.6404')
    assert location.longitude == Decimal('0.7808')


def test_from_filename_friendly_coords_with_negative_longitude():
    location = Location.from_coordinate_string('516004_-41776', filename_friendly=True)

    assert location.latitude == Decimal('51.6004')
    assert location.longitude == Decimal('-4.1776')


def test_from_filename_friendly_coords_with_small_coordinate():
    location = Location.from_coordinate_string('1234_5678', filename_friendly=True)

    assert location.latitude == Decimal('0.1234')
    assert location.longitude == Decimal('0.5678')


def test_from_filename_friendly_coords_roundtrips():
    original = Location.from_coordinates(Decimal('52.6604'), Decimal('0.7808'))
    coord_str = original.to_coordinate_string(filename_friendly=True)
    roundtripped = Location.from_coordinate_string(coord_str, filename_friendly=True)

    assert roundtripped == original
