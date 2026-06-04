"""Tests for assemble_window."""

import pandas as pd
from pv_prospect.data_transformation.processing.validation_window import (
    WINDOW_COLUMNS,
    assemble_window,
)


def _frame(rows: list[list]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=WINDOW_COLUMNS)


def test_merges_deduplicates_sorts_and_trims() -> None:
    prev = _frame([
        [1, '2026-01-01T00:00:00', 10.0, 100.0, 500.0, 4500.0],
        [1, '2026-01-02T00:00:00', 11.0, 110.0, 510.0, 4500.0],
    ])
    incr = _frame([
        [1, '2026-01-03T00:00:00', 12.0, 120.0, 520.0, 4500.0],
    ])
    result = assemble_window([prev, incr], 90)
    assert list(result.columns) == WINDOW_COLUMNS
    assert len(result) == 3
    # sorted by (system_id, time)
    assert list(result['time']) == sorted(result['time'])


def test_incr_overwrites_prev_on_same_key() -> None:
    # Same (system_id, time) in both frames. incr is second → keep='last' → incr wins.
    prev = _frame([[1, '2026-01-15T12:00:00', 10.0, 100.0, 500.0, 4500.0]])
    incr = _frame([[1, '2026-01-15T12:00:00', 10.0, 100.0, 750.0, 4500.0]])
    result = assemble_window([prev, incr], 90)
    assert len(result) == 1
    assert result.iloc[0]['power'] == 750.0


def test_trims_to_window_days() -> None:
    # max=2026-03-31, 2-day window → cutoff=2026-03-29
    prev = _frame([
        [1, '2026-03-28T00:00:00', 10.0, 100.0, 500.0, 4500.0],  # dropped
        [1, '2026-03-29T00:00:00', 10.0, 100.0, 500.0, 4500.0],  # kept
        [1, '2026-03-31T00:00:00', 10.0, 100.0, 500.0, 4500.0],  # kept (max)
    ])
    result = assemble_window([prev, _frame([])], 2)
    assert len(result) == 2
