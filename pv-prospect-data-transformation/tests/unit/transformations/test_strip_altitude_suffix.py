"""Tests for strip_altitude_suffix."""

from pv_prospect.data_transformation.transformations import strip_altitude_suffix


def test_strips_2m_suffix() -> None:
    assert strip_altitude_suffix('temperature_2m') == 'temperature'


def test_strips_180m_suffix() -> None:
    assert strip_altitude_suffix('wind_speed_180m') == 'wind_speed'


def test_strips_80m_suffix() -> None:
    assert strip_altitude_suffix('wind_direction_80m') == 'wind_direction'


def test_no_altitude_suffix_is_unchanged() -> None:
    assert (
        strip_altitude_suffix('direct_normal_irradiance') == 'direct_normal_irradiance'
    )


def test_does_not_strip_non_trailing_altitude_pattern() -> None:
    assert strip_altitude_suffix('wind_speed_180m_extra') == 'wind_speed_180m_extra'
