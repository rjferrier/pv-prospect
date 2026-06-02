"""Tests for temporal_holdout_split."""

import pandas as pd
from pv_prospect.model.splits import temporal_holdout_split


def _make_df(n: int = 10) -> pd.DataFrame:
    """Return a DataFrame with n rows sorted by time."""
    dates = pd.date_range('2023-01-01', periods=n)
    return pd.DataFrame({'time': dates, 'val': range(n)})


def test_explicit_cutoff_splits_correctly() -> None:
    """Train < cutoff, test >= cutoff."""
    df = _make_df(5)
    cutoff = pd.Timestamp('2023-01-03')
    train, test, used_cutoff = temporal_holdout_split(df, cutoff=cutoff)
    assert all(train['time'] < cutoff)
    assert all(test['time'] >= cutoff)
    assert len(train) + len(test) == 5
    assert used_cutoff == cutoff


def test_quantile_cutoff_splits_non_trivially() -> None:
    """The quantile cutoff produces a non-empty train and test split."""
    df = _make_df(10)
    train, test, cutoff = temporal_holdout_split(df, cutoff_quantile=0.8)
    assert len(train) > 0
    assert len(test) > 0
    assert all(train['time'] < cutoff)
    assert all(test['time'] >= cutoff)


def test_returned_cutoff_matches_applied_cutoff() -> None:
    """The third return value is the actual cutoff used for the split."""
    df = _make_df(10)
    cutoff_in = pd.Timestamp('2023-01-05')
    _, _, cutoff_out = temporal_holdout_split(df, cutoff=cutoff_in)
    assert cutoff_out == cutoff_in


def test_custom_time_column_name() -> None:
    """The split respects a non-default time_col name."""
    dates = pd.date_range('2023-01-01', periods=5)
    df = pd.DataFrame({'ts': dates, 'val': range(5)})
    cutoff = pd.Timestamp('2023-01-04')
    train, test, _ = temporal_holdout_split(df, time_col='ts', cutoff=cutoff)
    assert len(train) == 3
    assert len(test) == 2
