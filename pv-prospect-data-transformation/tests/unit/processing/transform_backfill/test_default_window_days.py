"""Tests for default_window_days."""

from pv_prospect.data_transformation.processing import (
    TransformBackfillScope,
    default_window_days,
)


def test_pv_sites_window_is_28_days() -> None:
    assert default_window_days(TransformBackfillScope.PV_SITES) == 28


def test_weather_grid_window_is_14_days() -> None:
    assert default_window_days(TransformBackfillScope.WEATHER_GRID) == 14
