"""Tests for apply_censoring_filter."""

import pandas as pd
from pv_prospect.model.features.pv import apply_censoring_filter


def _make_df(power_max: list[float], inverter_capacity: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            'power_max': power_max,
            'inverter_capacity': inverter_capacity,
        }
    )


def test_rows_above_threshold_are_dropped() -> None:
    """Rows where power_max > inverter * (1 - margin) are censored and removed."""
    df = _make_df([5950, 6100], inverter_capacity=6000.0)
    result = apply_censoring_filter(df, margin=0.01)
    # threshold = 6000 * 0.99 = 5940; 5950 > 5940 and 6100 > 5940, so both dropped
    assert len(result) == 0


def test_rows_below_threshold_are_kept() -> None:
    """Rows well below the inverter cap are not censored."""
    df = _make_df([3000, 4000], inverter_capacity=6000.0)
    result = apply_censoring_filter(df, margin=0.01)
    assert len(result) == 2


def test_row_at_exactly_threshold_is_kept() -> None:
    """The boundary condition: power_max == threshold is kept (<=, not <)."""
    threshold = 6000.0 * (1 - 0.01)
    df = _make_df([threshold], inverter_capacity=6000.0)
    result = apply_censoring_filter(df, margin=0.01)
    assert len(result) == 1


def test_mixed_rows_filtered_correctly() -> None:
    """Only the clamped rows are dropped; unclamped rows are preserved."""
    df = _make_df([3000, 5940, 6000], inverter_capacity=6000.0)
    result = apply_censoring_filter(df, margin=0.01)
    # threshold = 5940; 3000 ≤ 5940 kept, 5940 ≤ 5940 kept, 6000 > 5940 dropped
    assert len(result) == 2
    assert list(result['power_max']) == [3000, 5940]


def test_default_margin_is_one_percent() -> None:
    """Calling without margin argument uses the 1% default."""
    df = _make_df([5941], inverter_capacity=6000.0)
    result = apply_censoring_filter(df)
    # 5941 > 6000 * 0.99 = 5940 → dropped
    assert len(result) == 0
