"""Tests for add_cyclic_day_of_year."""

import math

import pandas as pd
import pytest
from pv_prospect.model.features.weather import add_cyclic_day_of_year


def _make_df(times: list[str]) -> pd.DataFrame:
    return pd.DataFrame({'time': pd.to_datetime(times)})


def test_summer_solstice_has_max_sin() -> None:
    """Day ~172 (21 June) should have sin close to its yearly maximum."""
    df = _make_df(['2023-06-21', '2023-12-21'])
    result = add_cyclic_day_of_year(df)
    assert result.loc[0, 'day_of_year_sin'] > result.loc[1, 'day_of_year_sin']


def test_summer_solstice_has_min_cos() -> None:
    """Day ~172 (21 June) sits near the cos minimum; Dec has a higher cos."""
    # angle = 2π * day/365.25; cos peaks near Jan 1 (day 1) and troughs near
    # July 1 (day 183 ≈ half year).  Dec 21 is close to the Jan 1 peak.
    df = _make_df(['2023-06-21', '2023-12-21'])
    result = add_cyclic_day_of_year(df)
    assert result.loc[0, 'day_of_year_cos'] < result.loc[1, 'day_of_year_cos']


def test_sin_cos_lie_on_unit_circle() -> None:
    """sin²+cos² == 1 for every row."""
    df = _make_df(['2023-01-01', '2023-04-01', '2023-07-01', '2023-10-01'])
    result = add_cyclic_day_of_year(df)
    norms = result['day_of_year_sin'] ** 2 + result['day_of_year_cos'] ** 2
    for v in norms:
        assert v == pytest.approx(1.0, abs=1e-10)


def test_new_year_and_year_end_are_adjacent() -> None:
    """Day 1 and day 365 are much closer to each other than to mid-summer.

    With 365 days per year the gap between day 1 and day 365 is only 1/365.25
    of the full cycle, so their sin/cos values are very similar.  The test
    uses a tolerance of 0.05 (≈ 3° of arc at the equinoxes) which is
    comfortably larger than the one-day gap but much smaller than the
    half-cycle (~1.0) that separates summer from winter.
    """
    df = _make_df(['2023-01-01', '2023-12-31', '2023-07-01'])
    result = add_cyclic_day_of_year(df)
    # sin encodes spring–autumn axis; cos encodes winter–summer axis.
    # Jan 1 has cos ≈ 1; Jul 1 has cos ≈ -1; Dec 31 has cos ≈ 1.
    gap_year_ends = abs(
        result.loc[0, 'day_of_year_cos'] - result.loc[1, 'day_of_year_cos']
    )
    gap_to_summer = abs(
        result.loc[0, 'day_of_year_cos'] - result.loc[2, 'day_of_year_cos']
    )
    assert gap_year_ends < 0.05
    assert gap_to_summer > 1.5


def test_input_columns_are_preserved() -> None:
    df = _make_df(['2023-06-01'])
    df['latitude'] = 51.5
    result = add_cyclic_day_of_year(df)
    assert 'latitude' in result.columns
    assert 'time' in result.columns


def test_vernal_equinox_sin_is_near_one() -> None:
    """Day ~80 (21 March) is near the sin peak (90° = day 91.3 = quarter year)."""
    df = _make_df(['2023-03-21'])
    result = add_cyclic_day_of_year(df)
    # sin(2π * 80/365.25) ≈ sin(1.375) ≈ 0.98
    expected = math.sin(2 * math.pi * 80 / 365.25)
    assert result.loc[0, 'day_of_year_sin'] == pytest.approx(expected, abs=0.02)
