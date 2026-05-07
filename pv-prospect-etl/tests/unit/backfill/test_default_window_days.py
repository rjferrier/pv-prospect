"""Tests for default_window_days."""

from pv_prospect.etl import BackfillScope, default_window_days


def test_pv_sites_window_is_28_days() -> None:
    assert default_window_days(BackfillScope.PV_SITES) == 28


def test_weather_grid_window_is_14_days() -> None:
    assert default_window_days(BackfillScope.WEATHER_GRID) == 14
