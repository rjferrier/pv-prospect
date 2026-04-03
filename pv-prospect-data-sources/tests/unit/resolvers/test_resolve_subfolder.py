from decimal import Decimal

from pv_prospect.common.domain import (
    GridPoint,
    Location,
    PanelGeometry,
    PVSite,
    Shading,
    System,
)
from pv_prospect.data_sources import resolve_subfolder
from pv_prospect.data_sources.data_source import DataSourceType


def _make_pv_site(pvo_sys_id: int = 89665) -> PVSite:
    return PVSite(
        pvo_sys_id=pvo_sys_id,
        name='Test',
        location=Location(latitude=Decimal('51.6'), longitude=Decimal('-4.2')),
        shading=Shading.NONE,
        panel_system=System(brand='Test', capacity=4000),
        panel_geometries=(PanelGeometry(azimuth=180, tilt=35, area_fraction=1.0),),
        inverter_system=System(brand='Test', capacity=3600),
    )


def test_weather_with_grid_point_returns_weather_grid_bin_subfolder() -> None:
    grid_point = GridPoint.from_id('526604_07808')

    result = resolve_subfolder(DataSourceType.WEATHER, grid_point)

    assert result == f'weather-grid/{grid_point.bin}'


def test_weather_with_pv_site_returns_pv_sites_subfolder() -> None:
    pv_site = _make_pv_site()

    result = resolve_subfolder(DataSourceType.WEATHER, pv_site)

    assert result == 'pv-sites/89665'


def test_pv_with_pv_site_returns_system_id() -> None:
    pv_site = _make_pv_site()

    result = resolve_subfolder(DataSourceType.PV, pv_site)

    assert result == '89665'
