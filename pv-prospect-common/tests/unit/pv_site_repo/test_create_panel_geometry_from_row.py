"""Tests for create_panel_geometry_from_row."""

from pv_prospect.common.pv_site_repo import create_panel_geometry_from_row

from .helpers import make_csv_row


def test_creates_geometry_from_valid_data() -> None:
    result = create_panel_geometry_from_row(make_csv_row(), 1)
    assert result is not None
    assert result.azimuth == 180
    assert result.tilt == 30
    assert result.area_fraction == 1.0


def test_returns_none_when_all_fields_missing() -> None:
    result = create_panel_geometry_from_row(make_csv_row(), 2)
    assert result is None


def test_defaults_azimuth_to_180_when_missing() -> None:
    row = {
        'panel_azimuth_1': '',
        'panel_elevation_1': '30',
        'panel_area_fraction_1': '1.0',
    }
    result = create_panel_geometry_from_row(row, 1)
    assert result is not None
    assert result.azimuth == 180


def test_defaults_tilt_to_0_when_missing() -> None:
    row = {
        'panel_azimuth_1': '180',
        'panel_elevation_1': '',
        'panel_area_fraction_1': '1.0',
    }
    result = create_panel_geometry_from_row(row, 1)
    assert result is not None
    assert result.tilt == 0


def test_defaults_area_fraction_to_1_when_missing() -> None:
    row = {
        'panel_azimuth_1': '180',
        'panel_elevation_1': '30',
        'panel_area_fraction_1': '',
    }
    result = create_panel_geometry_from_row(row, 1)
    assert result is not None
    assert result.area_fraction == 1.0
