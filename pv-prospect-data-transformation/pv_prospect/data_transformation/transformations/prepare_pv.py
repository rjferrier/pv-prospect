import numpy as np
import pandas as pd
import pvlib

from pv_prospect.common import PVSite
from pv_prospect.data_transformation.helpers.data_operations import reduce_rows

ALTITUDE = 0

DEFAULT_KEEP_COLUMNS = ('temperature', 'plane_of_array_irradiance', 'power')


def prepare_pv(
    weather_df: pd.DataFrame,
    pv_df: pd.DataFrame,
    pv_site: PVSite,
    keep_columns: tuple[str, ...] = DEFAULT_KEEP_COLUMNS,
    timescale_days: int | None = 1,
) -> pd.DataFrame:
    """
    Join cleaned weather and PV-output data, calculate POA irradiance, and
    select the final feature set for PV-model training.

    Args:
        weather_df: Cleaned weather DataFrame (output of clean_weather).
        pv_df: Cleaned PV-output DataFrame (output of clean_pv).
        pv_site: PVSite containing location and panel geometries.
        keep_columns: Column names to retain alongside 'time'.
        timescale_days: If set, downsample to this many days using
            time-weighted averaging. None keeps original resolution.

    Returns:
        Prepared DataFrame with 'time' and the specified columns.
    """
    weather_df = weather_df.copy()
    weather_df['time'] = pd.to_datetime(weather_df['time'])

    pv_df = pv_df.copy()
    pv_df['time'] = pd.to_datetime(pv_df['time'])

    # Align PV-output times to weather times and inner-join
    pvo_reduced = reduce_rows(pv_df[['time', 'power']], weather_df['time'])
    joined = weather_df.join(pvo_reduced.set_index('time'), on='time', how='inner')

    # Calculate POA irradiance
    joined['plane_of_array_irradiance'] = _calculate_poa_irradiance(joined, pv_site)

    # Select columns
    columns_to_keep = ['time'] + [col for col in keep_columns if col in joined.columns]
    result = joined[columns_to_keep].copy()

    # Downsample if requested
    if timescale_days is not None and timescale_days > 0 and not result.empty:
        result = _downsample(result, timescale_days)

    # Drop rows with any NaN
    result = result.dropna()

    return result


def _calculate_poa_irradiance(df: pd.DataFrame, pv_site: PVSite) -> pd.Series:
    """
    Calculate plane-of-array irradiance for each row in the dataframe.

    Uses pvlib to calculate POA based on DNI, DHI, solar position, and
    panel geometries (tilt, azimuth, area fraction).

    Args:
        df: DataFrame containing 'time', 'direct_normal_irradiance', and
            'diffuse_radiation' columns.
        pv_site: PVSite with location and panel_geometries.

    Returns:
        Series with POA irradiance values.
    """
    location = pvlib.location.Location(
        float(pv_site.location.latitude),
        float(pv_site.location.longitude),
        altitude=ALTITUDE,
        tz='UTC',
    )

    # Subtract 30 minutes because data is right-labeled hourly intervals
    times = df['time'] - pd.Timedelta('30min')
    times_utc = pd.DatetimeIndex(times).tz_localize('UTC')

    solar_position = location.get_solarposition(times_utc)
    apparent_zenith = solar_position['apparent_zenith']
    solar_azimuth = solar_position['azimuth']

    dni = df['direct_normal_irradiance'].values
    dhi = df['diffuse_radiation'].values

    # GHI = DNI * cos(apparent_zenith) + DHI
    zenith_radians = np.radians(apparent_zenith)
    ghi = dni * np.cos(zenith_radians) + dhi

    # Calculate POA for each panel geometry, weighted by area_fraction
    poa_total = np.zeros(len(df))

    for panel_geom in pv_site.panel_geometries:
        poa_components = pvlib.irradiance.get_total_irradiance(
            surface_tilt=panel_geom.tilt,
            surface_azimuth=panel_geom.azimuth,
            dni=dni,
            ghi=ghi,
            dhi=dhi,
            solar_zenith=apparent_zenith,
            solar_azimuth=solar_azimuth,
            model='isotropic',
        )
        poa_total += poa_components['poa_global'].values * panel_geom.area_fraction

    return pd.Series(poa_total, index=df.index)


def _downsample(df: pd.DataFrame, timescale_days: int) -> pd.DataFrame:
    """Downsample a DataFrame using time-weighted averaging."""
    start_time = df['time'].min().normalize()
    end_time = df['time'].max()
    ref_times = pd.date_range(
        start=start_time + pd.Timedelta(days=timescale_days),
        end=end_time + pd.Timedelta(days=timescale_days),
        freq=f'{timescale_days}D',
    )
    return reduce_rows(df, ref_times.to_series())
