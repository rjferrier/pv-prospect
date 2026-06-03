"""Tests for compute_poa_irradiance."""

from decimal import Decimal

import numpy as np
import pandas as pd
import pytest
from pv_prospect.common.domain import Location, PanelGeometry
from pv_prospect.physics import compute_poa_irradiance


@pytest.fixture
def location() -> Location:
    return Location(latitude=Decimal('52.0'), longitude=Decimal('0.5'))


@pytest.fixture
def panel_geometry() -> PanelGeometry:
    return PanelGeometry(azimuth=180, tilt=35, area_fraction=1.0)


def _diurnal_inputs(
    n_hours: int = 24,
) -> tuple[pd.DatetimeIndex, np.ndarray, np.ndarray]:
    """An hourly day with a simple solar diurnal shape (zero at night)."""
    times = pd.DatetimeIndex(
        pd.date_range('2026-06-21 00:00:00', periods=n_hours, freq='h')
    )
    hours = np.array([t.hour for t in times])
    solar_factor = np.clip(np.sin(np.pi * (hours - 6) / 12), 0, 1)
    dni = 800.0 * solar_factor
    dhi = 100.0 * solar_factor
    return times, dni, dhi


def test_has_no_meaningfully_negative_values(
    location: Location, panel_geometry: PanelGeometry
) -> None:
    """pvlib's isotropic model can emit negligible negative values at night
    from floating-point arithmetic; tolerate noise < 0.1 W/m2 but nothing more."""
    times, dni, dhi = _diurnal_inputs()

    poa = compute_poa_irradiance(times, dni, dhi, location, panel_geometry)

    assert (poa >= -0.1).all()


def test_zero_irradiance_gives_zero_poa(
    location: Location, panel_geometry: PanelGeometry
) -> None:
    times, _, _ = _diurnal_inputs()
    zeros = np.zeros(len(times))

    poa = compute_poa_irradiance(times, zeros, zeros, location, panel_geometry)

    assert np.allclose(poa, 0.0, atol=1e-9)


def test_is_linear_in_irradiance(
    location: Location, panel_geometry: PanelGeometry
) -> None:
    """The isotropic model is homogeneous in (DNI, DHI): scaling both inputs
    scales POA by the same factor. This pins the calculation cheaply."""
    times, dni, dhi = _diurnal_inputs()

    poa = compute_poa_irradiance(times, dni, dhi, location, panel_geometry)
    poa_doubled = compute_poa_irradiance(
        times, 2 * dni, 2 * dhi, location, panel_geometry
    )

    assert np.allclose(poa_doubled, 2 * poa)


def test_tz_naive_times_treated_as_utc(
    location: Location, panel_geometry: PanelGeometry
) -> None:
    times, dni, dhi = _diurnal_inputs()

    poa_naive = compute_poa_irradiance(times, dni, dhi, location, panel_geometry)
    poa_utc = compute_poa_irradiance(
        times.tz_localize('UTC'), dni, dhi, location, panel_geometry
    )

    assert np.allclose(poa_naive, poa_utc)


def test_returns_array_aligned_with_times(
    location: Location, panel_geometry: PanelGeometry
) -> None:
    times, dni, dhi = _diurnal_inputs(n_hours=12)

    poa = compute_poa_irradiance(times, dni, dhi, location, panel_geometry)

    assert poa.shape == (12,)


def test_matches_reference_values() -> None:
    """Golden regression: pin POA for fixed inputs so a future numerical drift
    is caught. The property tests above (non-negativity, linearity) would all
    still pass if a factor silently changed; this would not. Reference values
    were captured from the implementation verified equivalent to the original
    ``prepare_pv`` POA calculation."""
    times = pd.DatetimeIndex(
        ['2026-06-21 10:00', '2026-06-21 12:00', '2026-06-21 14:00']
    )
    dni = np.array([600.0, 850.0, 700.0])
    dhi = np.array([120.0, 160.0, 130.0])
    location = Location(latitude=Decimal('51.5'), longitude=Decimal('-0.12'))
    panel_geometry = PanelGeometry(azimuth=180, tilt=35, area_fraction=1.0)

    poa = compute_poa_irradiance(times, dni, dhi, location, panel_geometry)

    assert np.allclose(poa, [645.023366, 1009.830364, 749.464818], rtol=1e-4)
