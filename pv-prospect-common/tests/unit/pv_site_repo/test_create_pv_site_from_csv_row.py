"""Tests for create_pv_site_from_csv_row."""

from datetime import date

import pytest
from pv_prospect.common.pv_site_repo import create_pv_site_from_csv_row

from .helpers import make_csv_row, make_two_panel_row


def test_parses_system_id() -> None:
    assert create_pv_site_from_csv_row(make_csv_row()).pvo_sys_id == 89665


def test_parses_name() -> None:
    assert create_pv_site_from_csv_row(make_csv_row()).name == 'Test Site'


def test_parses_location() -> None:
    site = create_pv_site_from_csv_row(make_csv_row())
    assert float(site.location.latitude) == 51.5074
    assert float(site.location.longitude) == -0.1278


def test_parses_panel_system() -> None:
    site = create_pv_site_from_csv_row(make_csv_row())
    assert site.panel_system.brand == 'SunPower'
    assert site.panel_system.capacity == 4000


def test_parses_inverter_system() -> None:
    site = create_pv_site_from_csv_row(make_csv_row())
    assert site.inverter_system.brand == 'Fronius'
    assert site.inverter_system.capacity == 3600


def test_parses_installation_date() -> None:
    assert create_pv_site_from_csv_row(make_csv_row()).installation_date == date(
        2020, 6, 15
    )


def test_installation_date_none_when_empty() -> None:
    row = make_csv_row()
    row['installation_date'] = ''
    assert create_pv_site_from_csv_row(row).installation_date is None


def test_single_panel_geometry() -> None:
    assert len(create_pv_site_from_csv_row(make_csv_row()).panel_geometries) == 1


def test_two_panel_geometries() -> None:
    site = create_pv_site_from_csv_row(make_two_panel_row())
    assert len(site.panel_geometries) == 2
    assert site.panel_geometries[0].azimuth == 160
    assert site.panel_geometries[1].azimuth == 200


def test_raises_when_no_geometries() -> None:
    row = make_csv_row()
    row['panel_azimuth_1'] = ''
    row['panel_elevation_1'] = ''
    row['panel_area_fraction_1'] = ''
    with pytest.raises(ValueError, match='panel geometry'):
        create_pv_site_from_csv_row(row)
