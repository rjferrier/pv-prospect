from datetime import date
from decimal import Decimal

from pv_prospect.common.domain import (
    ArbitrarySite,
    DateRange,
    Location,
    PanelGeometry,
    PVSite,
    Shading,
    System,
)
from pv_prospect.data_sources import DataSource, identify_time_series


def test_identify_openmeteo_time_series_single_day():
    grid_point = ArbitrarySite.from_id('526604_07808')

    result = identify_time_series(
        DataSource.OPENMETEO_QUARTERHOURLY,
        grid_point,
        DateRange.of_single_day(date(2025, 6, 24)),
    )

    assert result == 'openmeteo-quarterhourly_526604_07808_20250624'


def test_identify_openmeteo_time_series_multi_day():
    grid_point = ArbitrarySite.from_id('526604_07808')

    result = identify_time_series(
        DataSource.OPENMETEO_QUARTERHOURLY,
        grid_point,
        DateRange(date(2025, 6, 24), date(2025, 6, 30)),
    )

    assert result == 'openmeteo-quarterhourly_526604_07808_20250624_20250630'


def test_identify_pvoutput_time_series():
    pv_site = PVSite(
        pvo_sys_id=89665,
        name='Test',
        location=Location(latitude=Decimal('51.6'), longitude=Decimal('-4.2')),
        shading=Shading.NONE,
        panel_system=System(brand='Test', capacity=4000),
        panel_geometries=(PanelGeometry(azimuth=180, tilt=35, area_fraction=1.0),),
        inverter_system=System(brand='Test', capacity=3600),
    )

    result = identify_time_series(
        DataSource.PVOUTPUT,
        pv_site,
        DateRange.of_single_day(date(2025, 6, 1)),
    )

    assert result == 'pvoutput_89665_20250601'
