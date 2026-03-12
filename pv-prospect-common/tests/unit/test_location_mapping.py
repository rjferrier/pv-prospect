"""Tests for location mapping repository"""

import io
from decimal import Decimal

from pv_prospect.common.domain.location import Location
from pv_prospect.common.location_mapping import (
    _location_by_pv_system_id,
    _parse_coordinate,
    _parse_row,
    _str_to_coordinate,
    build_location_mapping_repo,
)


class TestStrToCoordinate:
    def test_positive_latitude_with_integer_part(self) -> None:
        assert _str_to_coordinate('516004') == Decimal('51.6004')

    def test_positive_longitude_with_integer_part(self) -> None:
        assert _str_to_coordinate('07808') == Decimal('0.7808')

    def test_negative_latitude_with_integer_part(self) -> None:
        assert _str_to_coordinate('-516004') == Decimal('-51.6004')

    def test_negative_longitude_with_integer_part(self) -> None:
        assert _str_to_coordinate('-41776') == Decimal('-4.1776')

    def test_coordinate_with_leading_zero(self) -> None:
        assert _str_to_coordinate('515995') == Decimal('51.5995')

    def test_coordinate_only_decimal_part(self) -> None:
        assert _str_to_coordinate('1234') == Decimal('0.1234')

    def test_negative_coordinate_only_decimal_part(self) -> None:
        assert _str_to_coordinate('-5678') == Decimal('-0.5678')

    def test_very_small_coordinate(self) -> None:
        assert _str_to_coordinate('123') == Decimal('0.0123')

    def test_large_latitude(self) -> None:
        assert _str_to_coordinate('526404') == Decimal('52.6404')


class TestParseCoordinate:
    def test_parse_valid_coordinate_positive(self) -> None:
        location = _parse_coordinate('516004_-41776')
        assert isinstance(location, Location)
        assert location.latitude == Decimal('51.6004')
        assert location.longitude == Decimal('-4.1776')

    def test_parse_valid_coordinate_mixed_sign(self) -> None:
        location = _parse_coordinate('526404_07808')
        assert location.latitude == Decimal('52.6404')
        assert location.longitude == Decimal('0.7808')


class TestParseRow:
    def test_parse_row_returns_tuple(self) -> None:
        row = {'pv_system_id': '42248', 'openmeteo_coords': '516004_-41776'}
        result = _parse_row(row)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], int)
        assert isinstance(result[1], Location)

    def test_parse_row_correct_pv_system_id(self) -> None:
        row = {'pv_system_id': '42248', 'openmeteo_coords': '516004_-41776'}
        pv_system_id, _ = _parse_row(row)
        assert pv_system_id == 42248

    def test_parse_row_correct_location(self) -> None:
        row = {'pv_system_id': '89665', 'openmeteo_coords': '526404_07808'}
        pv_system_id, location = _parse_row(row)
        assert pv_system_id == 89665
        assert location.latitude == Decimal('52.6404')
        assert location.longitude == Decimal('0.7808')


class TestBuildLocationMappingRepo:
    def setup_method(self) -> None:
        _location_by_pv_system_id.clear()

    def test_build_repo_from_sample_csv(self) -> None:
        csv_content = """pv_system_id,openmeteo_coords
42248,516004_-41776
89665,526404_07808
"""
        build_location_mapping_repo(io.StringIO(csv_content))

        assert len(_location_by_pv_system_id) == 2
        assert 42248 in _location_by_pv_system_id
        assert 89665 in _location_by_pv_system_id

    def test_build_repo_correct_locations(self) -> None:
        csv_content = """pv_system_id,openmeteo_coords
42248,516004_-41776
89665,526404_07808
"""
        build_location_mapping_repo(io.StringIO(csv_content))

        assert _location_by_pv_system_id[42248].latitude == Decimal('51.6004')
        assert _location_by_pv_system_id[42248].longitude == Decimal('-4.1776')
        assert _location_by_pv_system_id[89665].latitude == Decimal('52.6404')
        assert _location_by_pv_system_id[89665].longitude == Decimal('0.7808')

    def test_build_repo_only_once(self) -> None:
        csv_content1 = 'pv_system_id,openmeteo_coords\n42248,516004_-41776\n'
        build_location_mapping_repo(io.StringIO(csv_content1))
        assert len(_location_by_pv_system_id) == 1

        csv_content2 = 'pv_system_id,openmeteo_coords\n99999,526404_07808\n'
        build_location_mapping_repo(io.StringIO(csv_content2))

        assert len(_location_by_pv_system_id) == 1
        assert 42248 in _location_by_pv_system_id
        assert 99999 not in _location_by_pv_system_id

    def test_build_repo_from_empty_csv(self) -> None:
        build_location_mapping_repo(io.StringIO('pv_system_id,openmeteo_coords\n'))
        assert len(_location_by_pv_system_id) == 0
