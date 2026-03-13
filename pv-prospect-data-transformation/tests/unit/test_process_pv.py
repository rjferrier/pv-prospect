"""Tests for process_pv."""

from decimal import Decimal

import numpy as np
import pandas as pd
import pytest
from pv_prospect.common import Location, PanelGeometry, PVSite, Shading, System
from pv_prospect.data_transformation.process_pv import process_pv


def _make_pv_site(
    latitude=52.0,
    longitude=0.5,
    tilt=35,
    azimuth=180,
):
    """Create a minimal PVSite for testing."""
    return PVSite(
        pvo_sys_id=12345,
        name='Test Site',
        location=Location(
            latitude=Decimal(str(latitude)),
            longitude=Decimal(str(longitude)),
        ),
        shading=Shading.NONE,
        panel_system=System(brand='Test', capacity=4000),
        panel_geometries=[
            PanelGeometry(azimuth=azimuth, tilt=tilt, area_fraction=1.0),
        ],
        inverter_system=System(brand='Test', capacity=3600),
    )


def _make_hourly_weather_df(n_hours=24):
    """Create an hourly cleaned weather DataFrame spanning n_hours from noon on a summer day."""
    times = pd.date_range('2026-06-21 00:00:00', periods=n_hours, freq='h')
    rng = np.random.default_rng(42)

    # Simulate a simple diurnal pattern
    hours = np.array([t.hour for t in times])
    solar_factor = np.clip(np.sin(np.pi * (hours - 6) / 12), 0, 1)

    return pd.DataFrame(
        {
            'time': times,
            'temperature': 15.0 + 10.0 * solar_factor + rng.normal(0, 0.5, n_hours),
            'direct_normal_irradiance': np.clip(
                800.0 * solar_factor + rng.normal(0, 10, n_hours), 0, None
            ),
            'diffuse_radiation': np.clip(
                100.0 * solar_factor + rng.normal(0, 5, n_hours), 0, None
            ),
        }
    )


def _make_matching_pvoutput_df(weather_df):
    """Create a PV output DataFrame with the same timestamps as the weather data."""
    rng = np.random.default_rng(99)
    n = len(weather_df)
    return pd.DataFrame(
        {
            'time': weather_df['time'],
            'power': np.clip(rng.normal(2000, 500, n), 0, None),
        }
    )


@pytest.fixture
def pv_site():
    return _make_pv_site()


@pytest.fixture
def weather_df():
    return _make_hourly_weather_df()


@pytest.fixture
def pvoutput_df(weather_df):
    return _make_matching_pvoutput_df(weather_df)


# --- Column selection ---


def test_output_contains_default_columns(pv_site, weather_df, pvoutput_df):
    """Result should contain time + default keep columns."""
    result = process_pv(weather_df, pvoutput_df, pv_site, timescale_days=None)

    assert 'time' in result.columns
    assert 'temperature' in result.columns
    assert 'plane_of_array_irradiance' in result.columns
    assert 'power' in result.columns


def test_drops_non_selected_columns(pv_site, weather_df, pvoutput_df):
    """Columns not in keep_columns should be absent."""
    result = process_pv(weather_df, pvoutput_df, pv_site, timescale_days=None)

    assert 'direct_normal_irradiance' not in result.columns
    assert 'diffuse_radiation' not in result.columns


def test_custom_keep_columns(pv_site, weather_df, pvoutput_df):
    """Should honour custom keep_columns."""
    result = process_pv(
        weather_df,
        pvoutput_df,
        pv_site,
        keep_columns=('temperature', 'power'),
        timescale_days=None,
    )

    assert list(result.columns) == ['time', 'temperature', 'power']


# --- POA irradiance ---


def test_poa_irradiance_has_no_meaningfully_negative_values(
    pv_site, weather_df, pvoutput_df
):
    """POA irradiance should not have meaningfully negative values.

    Note: pvlib's isotropic model can produce negligible negative values at
    night due to floating-point arithmetic; we tolerate noise < 0.1 W/m².
    """
    result = process_pv(
        weather_df,
        pvoutput_df,
        pv_site,
        keep_columns=('plane_of_array_irradiance',),
        timescale_days=None,
    )

    assert (result['plane_of_array_irradiance'] >= -0.1).all()


# --- Join behaviour ---


def test_inner_join_drops_unmatched_weather_rows(pv_site, weather_df):
    """If PV output has fewer timestamps, weather-only rows should be dropped."""
    # Only provide PV output for the first 12 hours
    pvoutput_df = _make_matching_pvoutput_df(weather_df.iloc[:12])

    result = process_pv(weather_df, pvoutput_df, pv_site, timescale_days=None)

    assert len(result) <= 12


def test_drops_nan_rows(pv_site, weather_df):
    """Rows where any kept column is NaN should be dropped."""
    pvoutput_df = _make_matching_pvoutput_df(weather_df)
    pvoutput_df.loc[0, 'power'] = float('nan')

    result = process_pv(weather_df, pvoutput_df, pv_site, timescale_days=None)

    assert not result.isna().any().any()


# --- Non-mutation ---


def test_does_not_mutate_inputs(pv_site, weather_df, pvoutput_df):
    """The original DataFrames should not be modified."""
    weather_cols = list(weather_df.columns)
    pvo_cols = list(pvoutput_df.columns)
    weather_len = len(weather_df)
    pvo_len = len(pvoutput_df)

    process_pv(weather_df, pvoutput_df, pv_site, timescale_days=None)

    assert list(weather_df.columns) == weather_cols
    assert list(pvoutput_df.columns) == pvo_cols
    assert len(weather_df) == weather_len
    assert len(pvoutput_df) == pvo_len


# --- Multi-panel weighting ---


def test_multi_panel_poa_differs_from_single_panel(weather_df, pvoutput_df):
    """A site with two differently-oriented panels should produce different POA
    from a site with a single panel."""
    single = _make_pv_site(tilt=35, azimuth=180)
    multi = PVSite(
        pvo_sys_id=12345,
        name='Multi-panel Site',
        location=Location(latitude=Decimal('52'), longitude=Decimal('0.5')),
        shading=Shading.NONE,
        panel_system=System(brand='Test', capacity=4000),
        panel_geometries=[
            PanelGeometry(azimuth=180, tilt=35, area_fraction=0.5),
            PanelGeometry(azimuth=90, tilt=20, area_fraction=0.5),
        ],
        inverter_system=System(brand='Test', capacity=3600),
    )

    result_single = process_pv(
        weather_df,
        pvoutput_df,
        single,
        keep_columns=('plane_of_array_irradiance',),
        timescale_days=None,
    )
    result_multi = process_pv(
        weather_df,
        pvoutput_df,
        multi,
        keep_columns=('plane_of_array_irradiance',),
        timescale_days=None,
    )

    # The POA values should differ between single and multi-panel configs
    assert not np.allclose(
        result_single['plane_of_array_irradiance'].values,
        result_multi['plane_of_array_irradiance'].values,
    )
