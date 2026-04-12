"""Tests for ArbitrarySite domain model."""

from pv_prospect.common.domain import ArbitrarySite, Location


def test_id_returns_filename_friendly_coordinate_string():
    grid_point = ArbitrarySite.from_id('526604_07808')

    assert grid_point.id == '526604_07808'


def test_bin_floors_fractional_positive_coordinates():
    grid_point = ArbitrarySite(Location.from_coordinates('0.9', '0.0'))

    assert grid_point.bin == '0_0'


def test_bin_floors_at_integer_boundary():
    grid_point = ArbitrarySite(Location.from_coordinates('1.0', '0.0'))

    assert grid_point.bin == '1_0'


def test_bin_floors_negative_coordinate_towards_negative_infinity():
    grid_point = ArbitrarySite(Location.from_coordinates('-0.1', '0.0'))

    assert grid_point.bin == '-1_0'


def test_bin_zero_coordinates():
    grid_point = ArbitrarySite(Location.from_coordinates('0.0', '0.0'))

    assert grid_point.bin == '0_0'


def test_bin_realistic_uk_coordinates():
    grid_point = ArbitrarySite.from_id('526604_07808')

    assert grid_point.bin == '52_0'


def test_from_id_roundtrips():
    grid_point = ArbitrarySite.from_id('526604_07808')

    assert ArbitrarySite.from_id(grid_point.id) == grid_point
