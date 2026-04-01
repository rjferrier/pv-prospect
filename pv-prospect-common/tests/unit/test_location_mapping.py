"""Tests for location mapping repository."""

import io
from decimal import Decimal

from pv_prospect.common import build_location_mapping_repo
from pv_prospect.common.location_mapping import _location_by_pv_system_id


def _clear_repo() -> None:
    _location_by_pv_system_id.clear()


def test_build_repo_from_sample_csv():
    _clear_repo()
    csv_content = """pv_system_id,openmeteo_coords
42248,516004_-41776
89665,526404_07808
"""
    build_location_mapping_repo(io.StringIO(csv_content))

    assert len(_location_by_pv_system_id) == 2
    assert 42248 in _location_by_pv_system_id
    assert 89665 in _location_by_pv_system_id


def test_build_repo_correct_locations():
    _clear_repo()
    csv_content = """pv_system_id,openmeteo_coords
42248,516004_-41776
89665,526404_07808
"""
    build_location_mapping_repo(io.StringIO(csv_content))

    assert _location_by_pv_system_id[42248].latitude == Decimal('51.6004')
    assert _location_by_pv_system_id[42248].longitude == Decimal('-4.1776')
    assert _location_by_pv_system_id[89665].latitude == Decimal('52.6404')
    assert _location_by_pv_system_id[89665].longitude == Decimal('0.7808')


def test_build_repo_only_once():
    _clear_repo()
    build_location_mapping_repo(
        io.StringIO('pv_system_id,openmeteo_coords\n42248,516004_-41776\n')
    )
    assert len(_location_by_pv_system_id) == 1

    build_location_mapping_repo(
        io.StringIO('pv_system_id,openmeteo_coords\n99999,526404_07808\n')
    )

    assert len(_location_by_pv_system_id) == 1
    assert 42248 in _location_by_pv_system_id
    assert 99999 not in _location_by_pv_system_id


def test_build_repo_from_empty_csv():
    _clear_repo()
    build_location_mapping_repo(io.StringIO('pv_system_id,openmeteo_coords\n'))

    assert len(_location_by_pv_system_id) == 0
