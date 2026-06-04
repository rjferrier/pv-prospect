"""Shared test helpers for the validation unit tests."""

from datetime import date
from decimal import Decimal

import pandas as pd
from pv_prospect.common.domain.location import Location
from pv_prospect.common.domain.pv_site import PanelGeometry, PVSite, Shading, System


def make_pv_site(
    system_id: int = 12345,
    panels_capacity: int = 4000,
    inverter_capacity: int = 3600,
    installation_date: date | None = date(2018, 6, 1),
) -> PVSite:
    return PVSite(
        pvo_sys_id=system_id,
        name='Test Site',
        location=Location(Decimal('51.5'), Decimal('-0.1')),
        shading=Shading.NONE,
        panel_system=System(brand='Generic', capacity=panels_capacity),
        panel_geometries=(PanelGeometry(azimuth=180, tilt=35, area_fraction=1.0),),
        inverter_system=System(brand='Generic', capacity=inverter_capacity),
        installation_date=installation_date,
    )


def make_window_df(
    system_id: int = 12345,
    n_days: int = 5,
    power: float = 500.0,
    power_max: float = 1000.0,
    start: str = '2025-01-01',
) -> pd.DataFrame:
    times = pd.date_range(start=start, periods=n_days, freq='D')
    return pd.DataFrame(
        {
            'system_id': system_id,
            'time': times,
            'temperature': 10.0,
            'plane_of_array_irradiance': 200.0,
            'power': power,
            'power_max': power_max,
        }
    )
