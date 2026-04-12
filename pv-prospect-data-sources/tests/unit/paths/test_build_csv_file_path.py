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
from pv_prospect.data_sources import (
    DataSource,
    build_time_series_csv_file_path,
)


def test_builds_path_for_openmeteo_source():
    grid_point = ArbitrarySite.from_id('526604_07808')

    result = build_time_series_csv_file_path(
        'timeseries',
        DataSource.OPENMETEO_QUARTERHOURLY,
        grid_point,
        DateRange.of_single_day(date(2025, 6, 24)),
    )

    assert result == (
        'timeseries/openmeteo/quarterhourly/weather-grid/52_0/'
        'openmeteo-quarterhourly_526604_07808_20250624.csv'
    )


def test_builds_path_for_pvoutput_source():
    pv_site = PVSite(
        pvo_sys_id=89665,
        name='Test',
        location=Location(latitude=Decimal('51.6'), longitude=Decimal('-4.2')),
        shading=Shading.NONE,
        panel_system=System(brand='Test', capacity=4000),
        panel_geometries=(PanelGeometry(azimuth=180, tilt=35, area_fraction=1.0),),
        inverter_system=System(brand='Test', capacity=3600),
    )

    result = build_time_series_csv_file_path(
        'timeseries',
        DataSource.PVOUTPUT,
        pv_site,
        DateRange.of_single_day(date(2025, 6, 1)),
    )

    assert result == 'timeseries/pvoutput/89665/pvoutput_89665_20250601.csv'
