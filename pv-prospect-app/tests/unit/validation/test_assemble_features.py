from datetime import date

from pv_prospect.app.validation import assemble_features
from pv_prospect.model.features import BINARY_FEATURES, CONTINUOUS_FEATURES

from tests.unit.validation.helpers import make_pv_site, make_window_df


def test_known_site_age_known_is_one() -> None:
    pv_site = make_pv_site(installation_date=date(2018, 1, 1))
    df = make_window_df(start='2025-01-01', n_days=3)
    result = assemble_features(df, pv_site, age_fill=0.0)
    assert list(result['age_known']) == [1, 1, 1]


def test_known_site_age_years_is_positive() -> None:
    pv_site = make_pv_site(installation_date=date(2018, 1, 1))
    df = make_window_df(start='2025-01-01', n_days=1)
    result = assemble_features(df, pv_site, age_fill=0.0)
    assert result['age_years'].iloc[0] > 6.0


def test_dateless_site_uses_age_fill() -> None:
    pv_site = make_pv_site(installation_date=None)
    df = make_window_df(start='2025-01-01', n_days=2)
    result = assemble_features(df, pv_site, age_fill=7.5)
    assert list(result['age_known']) == [0, 0]
    assert list(result['age_years']) == [7.5, 7.5]


def test_day_of_year_derived_from_time() -> None:
    pv_site = make_pv_site()
    df = make_window_df(start='2025-01-01', n_days=3)
    result = assemble_features(df, pv_site, age_fill=0.0)
    assert list(result['day_of_year']) == [1, 2, 3]


def test_temperature_and_poa_passed_through() -> None:
    pv_site = make_pv_site()
    df = make_window_df(n_days=2)
    result = assemble_features(df, pv_site, age_fill=0.0)
    assert list(result['temperature']) == [10.0, 10.0]
    assert list(result['plane_of_array_irradiance']) == [200.0, 200.0]


def test_all_feature_columns_present() -> None:
    pv_site = make_pv_site()
    df = make_window_df(n_days=2)
    result = assemble_features(df, pv_site, age_fill=0.0)
    for col in CONTINUOUS_FEATURES + BINARY_FEATURES:
        assert col in result.columns


def test_aux_columns_present() -> None:
    pv_site = make_pv_site(inverter_capacity=3600)
    df = make_window_df(n_days=2)
    result = assemble_features(df, pv_site, age_fill=0.0)
    assert 'power' in result.columns
    assert 'power_max' in result.columns
    assert 'inverter_capacity' in result.columns
    assert list(result['inverter_capacity']) == [3600, 3600]
