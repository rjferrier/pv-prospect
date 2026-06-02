"""Temporal hold-out split and feature scaling."""

from __future__ import annotations

import pandas as pd
from sklearn.preprocessing import StandardScaler


def temporal_holdout_split(
    df: pd.DataFrame,
    time_col: str = 'time',
    cutoff_quantile: float = 0.8,
    cutoff: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    """Split rows by time. Default cutoff = 80th percentile of the date range.

    Returns ``(train, test, used_cutoff)``.  Train = rows with
    ``time < cutoff``; test = rows with ``time >= cutoff``.

    Temporal rather than random: PV-weather rows within a few days are
    autocorrelated, so a random split leaks information from train into test
    and inflates reported R².
    """
    if cutoff is None:
        cutoff = df[time_col].quantile(cutoff_quantile)
    train = df[df[time_col] < cutoff].copy()
    test = df[df[time_col] >= cutoff].copy()
    return train, test, cutoff


def fit_scaler(
    train_df: pd.DataFrame, continuous_features: list[str]
) -> StandardScaler:
    """Fit ``StandardScaler`` on the train rows' continuous features only."""
    scaler = StandardScaler()
    scaler.fit(train_df[continuous_features])
    return scaler


def scale_features(
    df: pd.DataFrame,
    scaler: StandardScaler,
    continuous_features: list[str],
    binary_features: list[str],
) -> pd.DataFrame:
    """Scale continuous columns, leave binary columns alone, preserve order."""
    continuous = pd.DataFrame(
        scaler.transform(df[continuous_features]),
        columns=continuous_features,
        index=df.index,
    )
    return pd.concat([continuous, df[binary_features]], axis=1)
