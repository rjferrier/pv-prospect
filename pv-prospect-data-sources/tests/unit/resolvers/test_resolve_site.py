from decimal import Decimal

import pytest
from pv_prospect.common.domain import (
    ArbitrarySite,
    Location,
    PanelGeometry,
    PVSite,
    Shading,
    System,
)
from pv_prospect.data_sources import DataSourceType, resolve_site


def _stub_get_pv_site(pv_system_id: int) -> PVSite:
    return PVSite(
        pvo_sys_id=pv_system_id,
        name='Test',
        location=Location.from_coordinates(Decimal('50.49'), Decimal('-3.54')),
        shading=Shading.NONE,
        panel_system=System(brand='Test', capacity=4000),
        panel_geometries=(PanelGeometry(azimuth=180, tilt=35, area_fraction=1.0),),
        inverter_system=System(brand='Test', capacity=3600),
    )


def test_from_location_str() -> None:
    result = resolve_site(
        DataSourceType.WEATHER, _stub_get_pv_site, location_str='50.49,-3.54'
    )

    assert result == ArbitrarySite(
        Location.from_coordinates(Decimal('50.49'), Decimal('-3.54'))
    )


def test_from_pv_system_id() -> None:
    result = resolve_site(DataSourceType.WEATHER, _stub_get_pv_site, pv_system_id=89665)

    assert result == PVSite(
        pvo_sys_id=89665,
        name='Test',
        location=Location.from_coordinates(Decimal('50.49'), Decimal('-3.54')),
        shading=Shading.NONE,
        panel_system=System(brand='Test', capacity=4000),
        panel_geometries=(PanelGeometry(azimuth=180, tilt=35, area_fraction=1.0),),
        inverter_system=System(brand='Test', capacity=3600),
    )


def test_both_args_raises() -> None:
    with pytest.raises(ValueError, match='Cannot provide both pv_system_id'):
        resolve_site(
            DataSourceType.WEATHER,
            _stub_get_pv_site,
            pv_system_id=89665,
            location_str='50.49,-3.54',
        )


def test_no_args_raises() -> None:
    with pytest.raises(ValueError, match='Either'):
        resolve_site(DataSourceType.WEATHER, _stub_get_pv_site)
