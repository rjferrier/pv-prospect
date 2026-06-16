"""Inference chain: weather → POA → PV → clamp → energy integration.

All functions are pure (no I/O, no global state) so they are independently
testable and can be called from tests without loading the FastAPI app.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass

import numpy as np
import pandas as pd
from pv_prospect.model import (
    clamped_power_pred,
    predict_capacity_factor,
    predict_weather,
)
from pv_prospect.model.domain import ModelArtifact, WeatherModelArtifact
from pv_prospect.physics import reconstruct_hourly_poa

# UK bounding box — models were trained exclusively on UK sites.
_UK_LAT_MIN, _UK_LAT_MAX = 49.5, 61.0
_UK_LON_MIN, _UK_LON_MAX = -9.0, 2.0


class OutsideUKDomainError(ValueError):
    """Raised when coordinates are outside the UK training domain."""


@dataclass
class RunPvModelResult:
    capacity_factor: np.ndarray  # raw model output, pre-clamp
    clamped_power_w: np.ndarray  # delivered power, post inverter clamp


@dataclass
class YieldResult:
    expected_annual_kwh: float
    monthly_kwh: list[float]


def run_pv_model(
    pv_artifact: ModelArtifact,
    feature_df: pd.DataFrame,
    panel_capacity_w: np.ndarray,
    inverter_capacity_w: np.ndarray,
) -> RunPvModelResult:
    """Run the PV model and apply the inverter clamp.

    Single shared inference path for /validate.  Returns both the raw
    capacity-factor output (pre-clamp) and the delivered power (post-clamp) so
    callers can expose whichever they need.

    Note: the daily-mean clamp here is correct for /validate (real measured
    daily-mean inputs, clipped days excluded from metrics), but is not used by
    /predict, which clips on reconstructed hourly power via
    ``clipped_daily_mean_power``.
    """
    cf = predict_capacity_factor(pv_artifact, feature_df)
    return RunPvModelResult(
        cf, clamped_power_pred(cf, panel_capacity_w, inverter_capacity_w)
    )


def clipped_daily_mean_power(
    cf: np.ndarray,
    panel_capacity_w: float,
    inverter_capacity_w: float,
    poa_shape_norm: np.ndarray,
) -> np.ndarray:
    """Return daily-mean AC power with inverter clipping applied per hour.

    Args:
        cf: (D,) unclipped daily-mean capacity factor from the PV model.
        panel_capacity_w: DC panel capacity in watts.
        inverter_capacity_w: Inverter AC capacity in watts.
        poa_shape_norm: (H,) hourly POA shape normalised so its mean is 1.
            Derived from the clear-sky intraday profile; mean-1 ensures the
            daily energy is preserved when no clipping occurs.

    Returns:
        (D,) daily-mean AC power in watts, after per-hour inverter clipping.

    When ``inverter_capacity_w`` exceeds the peak reconstructed DC power
    (``max(cf * panel * poa_shape_norm)``), the clip is inert and the result
    equals ``cf * panel_capacity_w`` exactly (backward compatible).

    Clipping losses are conservatively estimated: the clear-sky shape has a
    lower peak/mean ratio than real cloudy-spike days, so this slightly
    under-clips (safe direction).
    """
    dc_w = cf[:, None] * panel_capacity_w * poa_shape_norm[None, :]
    ac_w = np.minimum(dc_w, inverter_capacity_w)
    return ac_w.mean(axis=1)


def check_uk_domain(latitude: float, longitude: float) -> None:
    """Raise ``OutsideUKDomainError`` if the coordinates are outside the UK."""
    if not (
        _UK_LAT_MIN <= latitude <= _UK_LAT_MAX
        and _UK_LON_MIN <= longitude <= _UK_LON_MAX
    ):
        raise OutsideUKDomainError(
            f'({latitude}, {longitude}) is outside the UK domain '
            f'[{_UK_LAT_MIN}–{_UK_LAT_MAX}°N, {_UK_LON_MIN}–{_UK_LON_MAX}°E]. '
            'The models were trained on UK sites only.'
        )


def predict_yield(
    latitude: float,
    longitude: float,
    elevation: float,
    start_date: datetime.date,
    end_date: datetime.date,
    panels_capacity_w: float,
    inverter_capacity_w: float,
    tilt: int,
    azimuth: int,
    age_years: float,
    pv_artifact: ModelArtifact,
    weather_artifact: WeatherModelArtifact,
) -> YieldResult:
    """Run the full weather → POA → PV inference chain.

    Iterates over calendar months covered by ``[start_date, end_date]``.
    Each month is processed independently:
      1. Weather model → monthly-mean DNI/DHI.
      2. POA reconstruction → hourly clear-sky profile for the representative day.
      3. PV model → unclipped daily-mean capacity-factor for each day.
      4. Redistribute each day's energy across the hourly shape, clip per hour
         to the inverter capacity, re-average to daily mean, integrate → kWh.

    Step 4 applies the inverter clamp on instantaneous (hourly) power rather
    than the daily mean, so clipping losses appear at realistic DC/AC ratios.
    The clear-sky shape is used as the intraday envelope; clipping losses are
    slightly under-estimated relative to real cloudy-spike days (safe direction).

    Year information in the dates is ignored; only day-of-year drives the
    model (this is a climatological estimate, not a forecast).
    """
    months = _months_in_range(start_date, end_date)
    monthly_kwh = []

    for month_start in months:
        rep_day = month_start + datetime.timedelta(days=14)
        doy = rep_day.timetuple().tm_yday
        angle = 2 * np.pi * doy / 365.25

        wx_row = pd.DataFrame(
            {
                'latitude': [latitude],
                'longitude': [longitude],
                'elevation': [elevation],
                'day_of_year_sin': [np.sin(angle)],
                'day_of_year_cos': [np.cos(angle)],
            }
        )
        wx_pred = predict_weather(weather_artifact, wx_row)
        dni = float(wx_pred['direct_normal_irradiance'].iloc[0])
        dhi = float(wx_pred['diffuse_radiation'].iloc[0])
        temperature = float(wx_pred['temperature'].iloc[0])

        poa_hourly = reconstruct_hourly_poa(
            latitude, longitude, rep_day, dni, dhi, tilt, azimuth
        )
        poa_mean = float(poa_hourly.mean())
        poa_shape_norm = (
            poa_hourly / poa_mean if poa_mean > 0 else np.ones(len(poa_hourly))
        )

        days = _days_in_month_within_range(month_start, start_date, end_date)
        pv_rows = pd.DataFrame(
            {
                'day_of_year': [d.timetuple().tm_yday for d in days],
                'temperature': [temperature] * len(days),
                'plane_of_array_irradiance': [poa_mean] * len(days),
                # Structural input routed to the degradation factor (not an MLP
                # feature). A prospect has no install history → age_years=0.
                'age_years': [age_years] * len(days),
            }
        )
        cf = predict_capacity_factor(pv_artifact, pv_rows)
        daily_ac_w = clipped_daily_mean_power(
            cf, panels_capacity_w, inverter_capacity_w, poa_shape_norm
        )
        monthly_kwh.append(float((daily_ac_w * 24 / 1000).sum()))

    return YieldResult(
        expected_annual_kwh=sum(monthly_kwh),
        monthly_kwh=monthly_kwh,
    )


def _months_in_range(start: datetime.date, end: datetime.date) -> list[datetime.date]:
    """Return the first day of each calendar month that overlaps [start, end]."""
    months = []
    current = start.replace(day=1)
    while current <= end:
        months.append(current)
        # Advance to first day of next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months


def _days_in_month_within_range(
    month_start: datetime.date,
    range_start: datetime.date,
    range_end: datetime.date,
) -> list[datetime.date]:
    """Return all days in the given month that fall within [range_start, range_end]."""
    if month_start.month == 12:
        month_end = month_start.replace(year=month_start.year + 1, month=1)
    else:
        month_end = month_start.replace(month=month_start.month + 1)
    days = []
    day = max(month_start, range_start)
    while day < month_end and day <= range_end:
        days.append(day)
        day += datetime.timedelta(days=1)
    return days
