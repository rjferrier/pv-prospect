"""Plane-of-array (POA) irradiance.

This is the *single* POA implementation shared by the transformation pipeline
(``prepare_pv``) and the prediction API, so that the POA the PV model is trained
on and the POA fed to it at inference are computed identically. Duplicating the
calculation would let the two drift silently.

POA is computed at **sea level** (``ALTITUDE = 0``). Site elevation is an input
*feature* of the weather model, not a parameter of the POA geometry, and the
training pipeline has always used a flat altitude. Both callers must therefore
use the same altitude, or inference POA would diverge from the POA the PV model
trained on; that is why the constant lives here and is the shared default.
"""

from __future__ import annotations

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
