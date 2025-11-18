import sys
from datetime import datetime
from typing import Collection

from pv_prospect.common import PVSite, Location, PanelGeometry
from pyspark.sql import DataFrame, functions as f
from solarpy import standard2solar_time, irradiance_on_plane, beam_irradiance

MACHINE_EPSILON = sys.float_info.epsilon
ALTITUDE = 0


def preprocess(openmeteo: DataFrame, pvoutput: DataFrame, pv_site: PVSite) -> DataFrame:
    _normalise_pvoutput_datetimes(pvoutput)
    # to be continued
    return ...


def _normalise_pvoutput_datetimes(df: DataFrame) -> DataFrame:
    ts = f.to_timestamp(f.concat_ws('T', f.col('date').cast('string'), f.date_format(f.col('time'), "HH:mm:ss")), "yyyyMMdd'T'HH:mm:ss")
    df = df.withColumn('time', f.to_utc_timestamp(ts, 'Europe/London'))
    df.drop('date')


def _calculate_solar_incidence_factor(dt: datetime, pv_site: PVSite) -> float:
    solar_time = standard2solar_time(dt, pv_site.location.longitude)
    return (
            _calculate_irradiance_on_panels(solar_time, pv_site.location, pv_site.panel_geometries) /
            _calculate_direct_normal_irradiance(solar_time, pv_site.location)
    )


def _calculate_irradiance_on_panels(
        solar_time: datetime, loc: Location, panel_geometries: Collection[PanelGeometry]
) -> float:
    irradiances_per_panel_group = [
        max(MACHINE_EPSILON, irradiance_on_plane(geom.v_norm, ALTITUDE, solar_time, loc.latitude)) * geom.area_fraction
        for geom in panel_geometries
    ]
    return sum(irradiances_per_panel_group)


def _calculate_direct_normal_irradiance(solar_time: datetime, location: Location) -> float:
    return max(MACHINE_EPSILON, beam_irradiance(ALTITUDE, solar_time, location.longitude))
