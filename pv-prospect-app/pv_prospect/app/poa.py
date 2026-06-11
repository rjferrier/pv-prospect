"""POA reconstruction: monthly-mean 24h-mean DNI/DHI → daily-mean POA.

The weather model outputs monthly-mean daily DNI/DHI (24h-mean, night zeros
included). The PV model trains on the 24 h-mean POA that prepare_pv now
computes, so this reconstruction is self-consistent with the training corpus.

Method: use pvlib clear-sky as the hourly *shape* for DNI and DHI, scale each
independently so its 24h-mean equals the weather model's monthly-mean output,
then compute hourly POA with compute_poa_irradiance and average over all 24h.

The API carries a known ~30% systematic underestimate against the current
(data-v2026-05-31) trained artifacts, which were built on the old daytime-mean
POA convention and have not yet been retrained on the 24 h-mean corpus
(see briefs/pv-yield-overestimate.md).
"""

from __future__ import annotations

import datetime
from decimal import Decimal

import numpy as np
import pandas as pd
import pvlib
from pv_prospect.common.domain import Location, PanelGeometry
from pv_prospect.physics import compute_poa_irradiance


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

    Uses the pvlib Ineichen clear-sky profile for the representative date as
    the intraday shape, scales each component so its 24h-mean equals the
    weather model output, then returns the 24h-mean of the resulting hourly
    POA — matching the daily-mean-of-hourly-POA the PV model trained on.

    ``representative_date`` should be the month midpoint (month 1st + 14 days),
    matching ``downsample_to_monthly``'s convention.

    ``altitude`` is always 0 (sea level) — see ``pv_prospect.physics.ALTITUDE``.
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
    poa_hourly = compute_poa_irradiance(
        times, scaled_dni, scaled_dhi, loc, panel_geometry
    )
    return float(poa_hourly.mean())
