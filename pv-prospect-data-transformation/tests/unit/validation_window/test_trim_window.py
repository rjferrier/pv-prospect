"""Tests for trim_window."""

import pandas as pd
from pv_prospect.data_transformation.processing.validation_window import (
    WINDOW_COLUMNS,
    trim_window,
)


def _make_window(rows: list[list]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=WINDOW_COLUMNS)
    df['time'] = pd.to_datetime(df['time'])
    return df


def test_rows_before_cutoff_are_dropped() -> None:
    # max=2026-03-31, cutoff = 2026-03-31 - 90d = 2025-12-31
    window = _make_window([
        [1, '2025-12-30', 10.0, 100.0, 500.0, 4500.0],  # before cutoff → dropped
        [1, '2025-12-31', 10.0, 100.0, 500.0, 4500.0],  # at cutoff → kept
        [1, '2026-03-31', 10.0, 100.0, 500.0, 4500.0],  # max → kept
    ])
    result = trim_window(window, 90)
    assert len(result) == 2
    assert result['time'].min() == pd.Timestamp('2025-12-31')


def test_offline_site_within_window_is_retained() -> None:
    # Site 2 only has old data; the global max comes from site 1.
    # Site 2's row within 90 days of that max is retained; the older one is dropped.
    window = _make_window([
        [1, '2026-03-31', 10.0, 100.0, 500.0, 4500.0],
        [2, '2026-01-15', 10.0, 100.0, 500.0, 4500.0],  # within 90d → kept
        [2, '2025-12-30', 10.0, 100.0, 500.0, 4500.0],  # outside 90d → dropped
    ])
    result = trim_window(window, 90)
    assert len(result) == 2
    assert set(result['system_id']) == {1, 2}


def test_empty_returns_empty_with_correct_columns() -> None:
    df = pd.DataFrame(columns=WINDOW_COLUMNS)
    result = trim_window(df, 90)
    assert result.empty
    assert list(result.columns) == WINDOW_COLUMNS
