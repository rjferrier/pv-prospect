"""Tests for prepare_pv."""

from decimal import Decimal

import numpy as np
import pandas as pd
import pytest
from pv_prospect.common.domain import Location, PanelGeometry, PVSite, Shading, System
from pv_prospect.data_transformation.transformations import prepare_pv


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


def _make_matching_pv_df(weather_df):
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
def pv_df(weather_df):
    return _make_matching_pv_df(weather_df)


# --- Column selection ---


def test_output_contains_default_columns(pv_site, weather_df, pv_df):
    """Result should contain time + default keep columns."""
    result = prepare_pv(weather_df, pv_df, pv_site, timescale_days=None)

    assert 'time' in result.columns
    assert 'temperature' in result.columns
    assert 'plane_of_array_irradiance' in result.columns
    assert 'power' in result.columns


def test_drops_non_selected_columns(pv_site, weather_df, pv_df):
    """Columns not in keep_columns should be absent."""
    result = prepare_pv(weather_df, pv_df, pv_site, timescale_days=None)

    assert 'direct_normal_irradiance' not in result.columns
    assert 'diffuse_radiation' not in result.columns


def test_custom_keep_columns(pv_site, weather_df, pv_df):
    """Should honour custom keep_columns."""
    result = prepare_pv(
        weather_df,
        pv_df,
        pv_site,
        keep_columns=('temperature', 'power'),
        timescale_days=None,
    )

    assert list(result.columns) == ['time', 'temperature', 'power']


# --- Join behaviour ---


def test_inner_join_drops_unmatched_weather_rows(pv_site, weather_df):
    """If PV output has fewer timestamps, weather-only rows should be dropped."""
    # Only provide PV output for the first 12 hours
    pv_df = _make_matching_pv_df(weather_df.iloc[:12])

    result = prepare_pv(weather_df, pv_df, pv_site, timescale_days=None)

    assert len(result) <= 12


def test_drops_nan_rows(pv_site, weather_df):
    """Rows where any kept column is NaN should be dropped."""
    pv_df = _make_matching_pv_df(weather_df)
    pv_df.loc[0, 'power'] = float('nan')

    result = prepare_pv(weather_df, pv_df, pv_site, timescale_days=None)

    assert not result.isna().any().any()


# --- Non-mutation ---


def test_does_not_mutate_inputs(pv_site, weather_df, pv_df):
    """The original DataFrames should not be modified."""
    weather_cols = list(weather_df.columns)
    pvo_cols = list(pv_df.columns)
    weather_len = len(weather_df)
    pvo_len = len(pv_df)

    prepare_pv(weather_df, pv_df, pv_site, timescale_days=None)

    assert list(weather_df.columns) == weather_cols
    assert list(pv_df.columns) == pvo_cols
    assert len(weather_df) == weather_len
    assert len(pv_df) == pvo_len


# --- power_max ---


def test_output_contains_power_max_by_default(
    pv_site: PVSite, weather_df: pd.DataFrame, pv_df: pd.DataFrame
) -> None:
    """power_max ships in the default keep_columns set."""
    result = prepare_pv(weather_df, pv_df, pv_site, timescale_days=None)

    assert 'power_max' in result.columns


def test_power_max_excluded_when_not_in_keep_columns(
    pv_site: PVSite, weather_df: pd.DataFrame, pv_df: pd.DataFrame
) -> None:
    """Opting out of power_max via keep_columns omits the column."""
    result = prepare_pv(
        weather_df,
        pv_df,
        pv_site,
        keep_columns=('temperature', 'power'),
        timescale_days=None,
    )

    assert 'power_max' not in result.columns


def test_power_max_at_no_aggregation_is_daily_max(pv_site: PVSite) -> None:
    """With timescale_days=None each row carries the max over its calendar day."""
    weather_df = _make_hourly_weather_df(n_hours=24)
    times = pd.date_range('2026-06-21 00:00:00', periods=24, freq='h')
    powers = [100.0] * 24
    powers[12] = 5500.0  # noon spike
    pv_df = pd.DataFrame({'time': times, 'power': powers})

    result = prepare_pv(
        weather_df,
        pv_df,
        pv_site,
        keep_columns=('power', 'power_max'),
        timescale_days=None,
    )

    assert (result['power_max'] == 5500.0).all()


def test_power_max_at_daily_timescale_equals_native_daily_max(pv_site: PVSite) -> None:
    """timescale_days=1 produces one row per day; power_max is that day's max
    over the native-cadence pv_df."""
    weather_df = _make_hourly_weather_df(n_hours=48)
    times = pd.date_range('2026-06-21 00:00:00', periods=48, freq='h')
    powers = [100.0] * 48
    powers[12] = 4500.0  # day 1 peak
    powers[36] = 5200.0  # day 2 peak
    pv_df = pd.DataFrame({'time': times, 'power': powers})

    result = prepare_pv(
        weather_df,
        pv_df,
        pv_site,
        keep_columns=('power', 'power_max'),
        timescale_days=1,
    )

    by_day = dict(zip(result['time'].dt.normalize(), result['power_max'], strict=True))
    assert by_day[pd.Timestamp('2026-06-21')] == 4500.0
    assert by_day[pd.Timestamp('2026-06-22')] == 5200.0


def test_power_max_uses_native_cadence_not_reduced(pv_site: PVSite) -> None:
    """Regression: a sub-hour spike must survive into power_max even though
    reduce_rows time-weighted-averages it away in the joined frame.

    Without computing power_max from the pre-reduce pv_df, an inverter
    clipping that happens for (say) 12 minutes inside an hour gets smeared
    down toward the hour's mean, and power_max would falsely report a value
    well below the inverter cap. The training-time censoring filter would
    then keep censored rows.
    """
    weather_df = _make_hourly_weather_df(n_hours=24)
    # Native cadence: 5-minute samples across one full day (288 samples)
    times = pd.date_range('2026-06-21 00:00:00', periods=288, freq='5min')
    powers = [0.0] * 288
    # One single 5-minute spike of 6000 W at noon (the inverter cap).
    # In the hourly time-weighted average this is 6000 * 5/60 + 0 * 55/60 = 500 W —
    # far below the cap. power_max must still see the 6000 W.
    noon_idx = next(
        i for i, t in enumerate(times) if t == pd.Timestamp('2026-06-21 12:00:00')
    )
    powers[noon_idx] = 6000.0
    pv_df = pd.DataFrame({'time': times, 'power': powers})

    result = prepare_pv(
        weather_df,
        pv_df,
        pv_site,
        keep_columns=('power', 'power_max'),
        timescale_days=1,
    )

    assert len(result) == 1
    assert result['power_max'].iloc[0] == 6000.0
    # Daily-mean power is far below the spike (sanity-check the bias the
    # clipping flag is there to catch).
    assert result['power'].iloc[0] < 100.0


def test_power_max_empty_pv_df(pv_site: PVSite, weather_df: pd.DataFrame) -> None:
    """An empty pv_df shouldn't crash; the result is empty after dropna."""
    pv_df = pd.DataFrame({'time': pd.to_datetime([]), 'power': pd.Series(dtype=float)})

    result = prepare_pv(weather_df, pv_df, pv_site, timescale_days=None)

    assert result.empty


# --- Multi-panel weighting ---


def test_multi_panel_poa_differs_from_single_panel(weather_df, pv_df):
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

    result_single = prepare_pv(
        weather_df,
        pv_df,
        single,
        keep_columns=('plane_of_array_irradiance',),
        timescale_days=None,
    )
    result_multi = prepare_pv(
        weather_df,
        pv_df,
        multi,
        keep_columns=('plane_of_array_irradiance',),
        timescale_days=None,
    )

    # The POA values should differ between single and multi-panel configs
    assert not np.allclose(
        result_single['plane_of_array_irradiance'].values,
        result_multi['plane_of_array_irradiance'].values,
    )
