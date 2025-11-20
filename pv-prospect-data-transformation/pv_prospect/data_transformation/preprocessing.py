from datetime import datetime
from typing import Collection, Iterable

import pandas as pd
from pv_prospect.common import PVSite, Location, PanelGeometry
from pytz import timezone
from solarpy import standard2solar_time, irradiance_on_plane, beam_irradiance

from dataframe_functions import reduce_rows

ALTITUDE = 0

UKTZ = timezone('Europe/London')
UTC = timezone('UTC')


def preprocess(openmeteo: pd.DataFrame, pvoutput: pd.DataFrame, pv_site: PVSite) -> pd.DataFrame:
    openmeteo['time'] = pd.to_datetime(openmeteo['time'])
    _clean_pvoutput_times(pvoutput)
    pvo_reduced = reduce_rows(
        pvoutput[['time', 'power']], openmeteo['time']
    )
    joined = openmeteo.join(pvo_reduced.set_index('time'), on='time', how='inner')

    # to be continued

    return joined


def _clean_pvoutput_times(df: pd.DataFrame) -> pd.DataFrame:
    df['time'] = (
        pd.to_datetime(df['date'].astype(str) + 'T' + df['time'])
        .apply(_undo_uk_daylight_savings)
    )
    df.drop('date', axis=1, inplace=True)
    return df


def _undo_uk_daylight_savings(dt_local):
    return UKTZ.localize(dt_local).astimezone(UTC).replace(tzinfo=None)


def _calculate_solar_incidence_factor(
        dt: datetime, location: Location, panel_geometries: Iterable[PanelGeometry]
) -> float:
    solar_time = standard2solar_time(dt, location.longitude)

    dni = _calculate_direct_normal_irradiance(solar_time, location)
    if dni <= 0:
        return 1.0

    gti = _calculate_global_tilted_irradiance(solar_time, location, panel_geometries)
    return min(gti / dni, 1.0)


def _calculate_global_tilted_irradiance(
        solar_time: datetime, loc: Location, panel_geometries: Collection[PanelGeometry]
) -> float:
    irradiances_per_panel_group = [
        irradiance_on_plane(geom.v_norm, ALTITUDE, solar_time, loc.latitude) * geom.area_fraction
        for geom in panel_geometries
    ]
    return sum(irradiances_per_panel_group)


def _calculate_direct_normal_irradiance(solar_time: datetime, location: Location) -> float:
    return beam_irradiance(ALTITUDE, solar_time, location.longitude)
