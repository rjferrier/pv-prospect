"""Annual-mean capacity factor over a grid of cells.

Runs the production prediction primitives — ``predict_weather`` (weather model),
``reconstruct_daily_mean_poa`` (shared physics), ``predict_capacity_factor`` (PV
model) — so the result is faithful to ``/predict``. It mirrors
``pv-prospect-app``'s ``chain.predict_yield`` per-month structure but returns the
PV model's raw daily-mean **capacity factor** instead of integrating energy:

  * Capacity factor is panel-rating-independent, so with a fixed tilt/azimuth the
    map is a pure climatological resource surface.
  * No inverter clamp is applied (that is a per-install AC limit, irrelevant to a
    resource map); ``age_years`` defaults to 0 (a new install).
"""

from __future__ import annotations

import datetime

import numpy as np
import pandas as pd

from pv_prospect.map.store import ModelStore
from pv_prospect.model import predict_capacity_factor, predict_weather
from pv_prospect.physics import reconstruct_daily_mean_poa

# Fixed panel geometry for the map (degrees). 180 = due south.
DEFAULT_AZIMUTH = 180
DEFAULT_TILT = 37.5

# Days per calendar month (non-leap); used to day-weight the 12 monthly CFs.
MONTH_WEIGHTS = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

# 365.25 matches the weather model's cyclic day-of-year convention (no
# year-boundary discontinuity across leap years).
_DAYS_PER_YEAR = 365.25


def representative_dates() -> list[datetime.date]:
    """Month-midpoint dates (1st + 14 days) — the weather model's convention."""
    return [
        datetime.date(2001, m, 1) + datetime.timedelta(days=14) for m in range(1, 13)
    ]


def monthly_capacity_factors(
    cells: pd.DataFrame,
    model_store: ModelStore,
    tilt: float,
    azimuth: int,
    age_years: float,
) -> np.ndarray:
    """Return a ``(12, n_cells)`` array of monthly-mean capacity factors.

    ``cells`` must carry ``latitude``, ``longitude``, ``elevation``. For each
    representative month: weather model → monthly-mean DNI/DHI/temperature
    (batched over all cells), POA reconstruction per cell, then PV model →
    capacity factor (batched).
    """
    latitudes = cells['latitude'].to_numpy()
    longitudes = cells['longitude'].to_numpy()
    elevations = cells['elevation'].to_numpy()
    n_cells = len(cells)
    out = np.empty((12, n_cells))

    for month_index, representative_date in enumerate(representative_dates()):
        day_of_year = representative_date.timetuple().tm_yday
        angle = 2 * np.pi * day_of_year / _DAYS_PER_YEAR

        weather_input = pd.DataFrame(
            {
                'latitude': latitudes,
                'longitude': longitudes,
                'elevation': elevations,
                'day_of_year_sin': np.sin(angle),
                'day_of_year_cos': np.cos(angle),
            }
        )
        weather = predict_weather(model_store.weather, weather_input)
        dni = weather['direct_normal_irradiance'].to_numpy()
        dhi = weather['diffuse_radiation'].to_numpy()
        temperature = weather['temperature'].to_numpy()

        poa = np.array(
            [
                reconstruct_daily_mean_poa(
                    latitudes[i],
                    longitudes[i],
                    representative_date,
                    dni[i],
                    dhi[i],
                    tilt,
                    azimuth,
                )
                for i in range(n_cells)
            ]
        )

        pv_input = pd.DataFrame(
            {
                'day_of_year': day_of_year,
                'temperature': temperature,
                'plane_of_array_irradiance': poa,
                'age_years': age_years,
            }
        )
        out[month_index] = predict_capacity_factor(model_store.pv, pv_input)
        print(f'  computed month {month_index + 1}/12', flush=True)

    return out


def annual_mean_capacity_factor(
    monthly_cf: np.ndarray, weights: tuple[int, ...] = MONTH_WEIGHTS
) -> np.ndarray:
    """Day-weighted mean of the 12 monthly capacity factors, per cell."""
    return np.average(monthly_cf, axis=0, weights=np.array(weights))
