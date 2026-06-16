"""Plane-of-array (POA) irradiance and its climatological reconstruction.

``compute_poa_irradiance`` is the *single* POA implementation shared by the
transformation pipeline (``prepare_pv``) and the prediction API, so that the POA
the PV model is trained on and the POA fed to it at inference are computed
identically. Duplicating the calculation would let the two drift silently.

``reconstruct_hourly_poa`` / ``reconstruct_daily_mean_poa`` turn the weather
model's monthly-mean DNI/DHI back into POA via a pvlib clear-sky intraday shape.
They are prediction-time reconstruction (training uses measured DNI/DHI, never
reconstructs) and live here — beside ``compute_poa_irradiance`` they wrap — so
every consumer (the prediction API and offline tools such as
``pv-prospect-map``) shares one implementation.

POA is computed at **sea level** (``ALTITUDE = 0``). Site elevation is an input
*feature* of the weather model, not a parameter of the POA geometry, and the
training pipeline has always used a flat altitude. Both callers must therefore
use the same altitude, or inference POA would diverge from the POA the PV model
trained on; that is why the constant lives here and is the shared default.
"""

from __future__ import annotations

import datetime
from decimal import Decimal

import numpy as np
import pandas as pd
import pvlib

from pv_prospect.common.domain import Location, PanelGeometry

ALTITUDE = 0.0


def compute_poa_irradiance(
    times: pd.DatetimeIndex,
    dni: np.ndarray,
    dhi: np.ndarray,
    location: Location,
    panel_geometry: PanelGeometry,
    altitude: float = ALTITUDE,
) -> np.ndarray:
    """Plane-of-array global irradiance for a single panel geometry.

    Args:
        times: Timestamps for the irradiance samples. Any cadence-labelling
            correction (e.g. the transformation pipeline's right-labelled
            half-hour shift) must be applied by the caller; tz-naive indices
            are interpreted as UTC.
        dni: Direct normal irradiance (W/m2), aligned with ``times``.
        dhi: Diffuse horizontal irradiance (W/m2), aligned with ``times``.
        location: Site location (latitude/longitude).
        panel_geometry: Panel tilt and azimuth.
        altitude: Site altitude in metres; defaults to sea level (see module
            docstring).

    Returns:
        ``poa_global`` (W/m2) for each timestamp.
    """
    times = pd.DatetimeIndex(times)
    if times.tz is None:
        times = times.tz_localize('UTC')

    pvlib_location = pvlib.location.Location(
        float(location.latitude),
        float(location.longitude),
        altitude=altitude,
        tz='UTC',
    )
    solar_position = pvlib_location.get_solarposition(times)
    apparent_zenith = solar_position['apparent_zenith']
    solar_azimuth = solar_position['azimuth']

    # GHI = DNI * cos(apparent_zenith) + DHI
    ghi = dni * np.cos(np.radians(apparent_zenith)) + dhi

    poa_components = pvlib.irradiance.get_total_irradiance(
        surface_tilt=panel_geometry.tilt,
        surface_azimuth=panel_geometry.azimuth,
        dni=dni,
        ghi=ghi,
        dhi=dhi,
        solar_zenith=apparent_zenith,
        solar_azimuth=solar_azimuth,
        model='isotropic',
    )
    return np.asarray(poa_components['poa_global'].values)


def reconstruct_hourly_poa(
    latitude: float,
    longitude: float,
    representative_date: datetime.date,
    dni_monthly_mean: float,
    dhi_monthly_mean: float,
    tilt: int,
    azimuth: int,
) -> np.ndarray:
    """Reconstruct 24-element hourly POA from monthly-mean 24h-mean DNI and DHI.

    The weather model outputs monthly-mean daily DNI/DHI (24h-mean, night zeros
    included). This uses the pvlib Ineichen clear-sky profile for the
    representative date as the intraday *shape*, scales each component so its
    24h-mean equals the weather model output, then returns hourly POA (W/m²) as
    a (24,) array. The PV model trains on the 24h-mean POA that ``prepare_pv``
    computes, so this reconstruction is self-consistent with the training corpus.

    ``representative_date`` should be the month midpoint (month 1st + 14 days),
    matching ``downsample_to_monthly``'s convention.

    POA altitude is always 0 (sea level) — see ``ALTITUDE``.
    """
    day_start = pd.Timestamp(representative_date, tz='UTC')
    times = pd.date_range(day_start, periods=24, freq='h')

    pvlib_loc = pvlib.location.Location(latitude, longitude, tz='UTC')
    clearsky = pvlib_loc.get_clearsky(times)
    cs_dni = clearsky['dni'].values
    cs_dhi = clearsky['dhi'].values

    mean_cs_dni = cs_dni.mean()
    mean_cs_dhi = cs_dhi.mean()
    scaled_dni = (
        cs_dni * (dni_monthly_mean / mean_cs_dni) if mean_cs_dni > 0 else np.zeros(24)
    )
    scaled_dhi = (
        cs_dhi * (dhi_monthly_mean / mean_cs_dhi) if mean_cs_dhi > 0 else np.zeros(24)
    )

    loc = Location(Decimal(str(round(latitude, 4))), Decimal(str(round(longitude, 4))))
    panel_geometry = PanelGeometry(azimuth=azimuth, tilt=tilt, area_fraction=1.0)
    return compute_poa_irradiance(times, scaled_dni, scaled_dhi, loc, panel_geometry)


def reconstruct_daily_mean_poa(
    latitude: float,
    longitude: float,
    representative_date: datetime.date,
    dni_monthly_mean: float,
    dhi_monthly_mean: float,
    tilt: int,
    azimuth: int,
) -> float:
    """Reconstruct daily-mean POA from monthly-mean 24h-mean DNI and DHI.

    Returns the 24h-mean of ``reconstruct_hourly_poa`` — matching the
    daily-mean-of-hourly-POA the PV model trained on.
    """
    return float(
        reconstruct_hourly_poa(
            latitude, longitude, representative_date,
            dni_monthly_mean, dhi_monthly_mean, tilt, azimuth,
        ).mean()
    )
