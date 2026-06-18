"""Unit tests for validation-site coordinate coarsening (anonymity)."""

from pv_prospect.app.main import SITE_COORD_DECIMALS, coarsen_site_coordinate


def test_coarsens_to_three_decimal_places() -> None:
    assert coarsen_site_coordinate(52.652413) == 52.652
    assert coarsen_site_coordinate(-1.234567) == -1.235


def test_matches_builtin_round_at_the_configured_precision() -> None:
    value = 0.778512
    assert coarsen_site_coordinate(value) == round(value, SITE_COORD_DECIMALS)


def test_value_already_within_precision_is_unchanged() -> None:
    assert coarsen_site_coordinate(54.123) == 54.123


def test_configured_precision_is_three_decimal_places() -> None:
    assert SITE_COORD_DECIMALS == 3
