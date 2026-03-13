"""Tests for PVSite domain models."""

from decimal import Decimal
from math import isclose, pi

from pv_prospect.common.domain import Location, PanelGeometry, PVSite, Shading, System


def test_panel_geometry_azimuth_radians_zero():
    pg = PanelGeometry(azimuth=0, tilt=0, area_fraction=1.0)
    assert pg.azimuth_radians == 0.0


def test_panel_geometry_azimuth_radians_180():
    pg = PanelGeometry(azimuth=180, tilt=0, area_fraction=1.0)
    assert isclose(pg.azimuth_radians, pi)


def test_panel_geometry_azimuth_radians_90():
    pg = PanelGeometry(azimuth=90, tilt=0, area_fraction=1.0)
    assert isclose(pg.azimuth_radians, pi / 2)


def test_panel_geometry_tilt_radians_zero():
    pg = PanelGeometry(azimuth=0, tilt=0, area_fraction=1.0)
    assert pg.tilt_radians == 0.0


def test_panel_geometry_tilt_radians_45():
    pg = PanelGeometry(azimuth=0, tilt=45, area_fraction=1.0)
    assert isclose(pg.tilt_radians, pi / 4)


def test_panel_geometry_v_norm_returns_three_element_tuple():
    pg = PanelGeometry(azimuth=180, tilt=30, area_fraction=1.0)
    result = pg.v_norm
    assert isinstance(result, tuple)
    assert len(result) == 3


def test_panel_geometry_v_norm_zero_tilt_points_straight_up():
    pg = PanelGeometry(azimuth=0, tilt=0, area_fraction=1.0)
    cx, cy, cz = pg.v_norm
    assert isclose(cx, 0.0, abs_tol=1e-10)
    assert isclose(cy, 0.0, abs_tol=1e-10)
    assert isclose(cz, -1.0)


def test_pv_site_str_includes_name_and_id():
    site = PVSite(
        pvo_sys_id=89665,
        name='Test Site',
        location=Location(Decimal('51.5'), Decimal('-0.1')),
        shading=Shading.NONE,
        panel_system=System(brand='SunPower', capacity=4000),
        panel_geometries=[PanelGeometry(azimuth=180, tilt=30, area_fraction=1.0)],
        inverter_system=System(brand='Fronius', capacity=3600),
    )
    assert str(site) == 'Test Site (system_id=89665)'
