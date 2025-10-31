"""Module-style tests for processing.pv_site_repo."""

from datetime import date

import pytest

from processing.pv_site_repo import (
    _create_pv_site_from_csv_row,
    _create_panel_geometry_from_row,
)
from domain.pv_site import PVSite, PanelGeometry
from domain.location import Location, Shading


def test_create_panel_geometry_all_fields_present():
    row = {
        'panel_azimuth_1': '180',
        'panel_elevation_1': '30',
        'panel_area_fraction_1': '0.6',
    }
    result = _create_panel_geometry_from_row(row, 1)

    assert result is not None
    assert result.azimuth == 180
    assert result.tilt == 30
    assert result.area_fraction == 0.6


def test_create_panel_geometry_missing_azimuth_uses_default():
    row = {
        'panel_azimuth_1': '',
        'panel_elevation_1': '25',
        'panel_area_fraction_1': '1.0',
    }
    result = _create_panel_geometry_from_row(row, 1)

    assert result is not None
    assert result.azimuth == 180
    assert result.tilt == 25


def test_create_panel_geometry_missing_tilt_uses_default():
    row = {
        'panel_azimuth_1': '90',
        'panel_elevation_1': '',
        'panel_area_fraction_1': '0.5',
    }
    result = _create_panel_geometry_from_row(row, 1)

    assert result is not None
    assert result.azimuth == 90
    assert result.tilt == 0


def test_create_panel_geometry_missing_area_fraction_uses_default():
    row = {
        'panel_azimuth_1': '270',
        'panel_elevation_1': '20',
        'panel_area_fraction_1': '',
    }
    result = _create_panel_geometry_from_row(row, 1)

    assert result is not None
    assert result.area_fraction == 1.0


def test_create_panel_geometry_all_fields_missing_returns_none():
    row = {
        'panel_azimuth_1': '',
        'panel_elevation_1': '',
        'panel_area_fraction_1': '',
    }
    result = _create_panel_geometry_from_row(row, 1)

    assert result is None


def test_create_panel_geometry_second_index():
    row = {
        'panel_azimuth_2': '90',
        'panel_elevation_2': '35',
        'panel_area_fraction_2': '0.4',
    }
    result = _create_panel_geometry_from_row(row, 2)

    assert result is not None
    assert result.azimuth == 90
    assert result.tilt == 35
    assert result.area_fraction == 0.4


def test_create_pv_site_minimal_valid_row():
    row = {
        'pvoutput_system_id': '12345',
        'name': 'Test Site',
        'latitude': '51.5074',
        'longitude': '-0.1278',
        'shading': 'NONE',
        'panel_brand': 'SolarTech',
        'panels_capacity': '5000',
        'inverter_brand': 'InverterCo',
        'inverter_capacity': '5000',
        'panel_azimuth_1': '180',
        'panel_elevation_1': '30',
        'panel_area_fraction_1': '1.0',
        'installation_date': '',
    }

    site = _create_pv_site_from_csv_row(row)

    assert site.pvo_sys_id == 12345
    assert site.name == 'Test Site'
    assert site.location.latitude == 51.5074
    assert site.location.longitude == -0.1278
    assert site.location.shading == 'NONE'
    assert site.panel_system.brand == 'SolarTech'
    assert site.panel_system.capacity == 5000
    assert site.inverter_system.brand == 'InverterCo'
    assert site.inverter_system.capacity == 5000
    assert len(site.panel_geometries) == 1
    assert site.panel_geometries[0].azimuth == 180
    assert site.installation_date is None


def test_create_pv_site_with_installation_date():
    row = {
        'pvoutput_system_id': '12345',
        'name': 'Test Site',
        'latitude': '51.5074',
        'longitude': '-0.1278',
        'shading': 'LOW',
        'panel_brand': 'SolarTech',
        'panels_capacity': '5000',
        'inverter_brand': 'InverterCo',
        'inverter_capacity': '5000',
        'panel_azimuth_1': '180',
        'panel_elevation_1': '30',
        'panel_area_fraction_1': '1.0',
        'installation_date': '2023-05-15',
    }

    site = _create_pv_site_from_csv_row(row)

    assert site.installation_date == date(2023, 5, 15)


def test_create_pv_site_two_panel_geometries():
    row = {
        'pvoutput_system_id': '12345',
        'name': 'Test Site',
        'latitude': '51.5074',
        'longitude': '-0.1278',
        'shading': 'MEDIUM',
        'panel_brand': 'SolarTech',
        'panels_capacity': '5000',
        'inverter_brand': 'InverterCo',
        'inverter_capacity': '5000',
        'panel_azimuth_1': '180',
        'panel_elevation_1': '30',
        'panel_area_fraction_1': '0.6',
        'panel_azimuth_2': '90',
        'panel_elevation_2': '25',
        'panel_area_fraction_2': '0.4',
        'installation_date': '',
    }

    site = _create_pv_site_from_csv_row(row)

    assert len(site.panel_geometries) == 2
    assert site.panel_geometries[0].azimuth == 180
    assert site.panel_geometries[0].area_fraction == 0.6
    assert site.panel_geometries[1].azimuth == 90
    assert site.panel_geometries[1].area_fraction == 0.4


def test_create_pv_site_no_panel_geometries_raises_error():
    row = {
        'pvoutput_system_id': '12345',
        'name': 'Test Site',
        'latitude': '51.5074',
        'longitude': '-0.1278',
        'shading': 'NONE',
        'panel_brand': 'SolarTech',
        'panels_capacity': '5000',
        'inverter_brand': 'InverterCo',
        'inverter_capacity': '5000',
        'panel_azimuth_1': '',
        'panel_elevation_1': '',
        'panel_area_fraction_1': '',
        'panel_azimuth_2': '',
        'panel_elevation_2': '',
        'panel_area_fraction_2': '',
        'installation_date': '',
    }

    with pytest.raises(ValueError, match="At least one panel geometry must be defined"):
        _create_pv_site_from_csv_row(row)

