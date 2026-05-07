"""Tests for parse_backfill_scope."""

import pytest
from pv_prospect.data_transformation.processing.entrypoint import (
    parse_backfill_scope,
)
from pv_prospect.etl import BackfillScope


def test_pv_sites_scope() -> None:
    assert parse_backfill_scope('pv_sites') is BackfillScope.PV_SITES


def test_weather_grid_scope() -> None:
    assert parse_backfill_scope('weather_grid') is BackfillScope.WEATHER_GRID


def test_unknown_value_raises_value_error() -> None:
    with pytest.raises(ValueError, match='BACKFILL_SCOPE must be one of'):
        parse_backfill_scope('not_a_scope')


def test_empty_string_raises_value_error() -> None:
    with pytest.raises(ValueError, match='BACKFILL_SCOPE must be one of'):
        parse_backfill_scope('')
