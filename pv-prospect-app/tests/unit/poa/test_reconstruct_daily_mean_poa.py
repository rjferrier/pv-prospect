"""Tests for reconstruct_daily_mean_poa."""

from __future__ import annotations

import datetime

import pytest
from pv_prospect.app.poa import reconstruct_daily_mean_poa


def test_returns_positive_poa_for_valid_summer_inputs() -> None:
    poa = reconstruct_daily_mean_poa(
        latitude=52.0,
        longitude=-1.0,
        representative_date=datetime.date(2025, 7, 15),
        dni_monthly_mean=200.0,
        dhi_monthly_mean=80.0,
        tilt=35,
        azimuth=180,
    )
    assert poa > 0


def test_zero_dni_and_dhi_gives_zero_poa() -> None:
    poa = reconstruct_daily_mean_poa(
        latitude=52.0,
        longitude=-1.0,
        representative_date=datetime.date(2025, 7, 15),
        dni_monthly_mean=0.0,
        dhi_monthly_mean=0.0,
        tilt=35,
        azimuth=180,
    )
    assert poa == pytest.approx(0.0, abs=1e-6)


def test_higher_dni_gives_higher_poa() -> None:
    poa_low = reconstruct_daily_mean_poa(
        52.0, -1.0, datetime.date(2025, 7, 15), 100.0, 50.0, 35, 180
    )
    poa_high = reconstruct_daily_mean_poa(
        52.0, -1.0, datetime.date(2025, 7, 15), 300.0, 50.0, 35, 180
    )
    assert poa_high > poa_low


def test_summer_poa_higher_than_winter() -> None:
    poa_summer = reconstruct_daily_mean_poa(
        52.0, -1.0, datetime.date(2025, 7, 15), 200.0, 80.0, 35, 180
    )
    poa_winter = reconstruct_daily_mean_poa(
        52.0, -1.0, datetime.date(2025, 1, 15), 50.0, 20.0, 35, 180
    )
    assert poa_summer > poa_winter
