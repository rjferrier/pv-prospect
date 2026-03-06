import pandas as pd
from pytz import timezone


UKTZ = timezone('Europe/London')
UTC = timezone('UTC')


def clean_pvoutput(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw PVOutput data.

    Synthesises a UTC 'time' column from 'date' and 'time' columns (converting
    from UK local time), retains only 'time' and 'power', and drops rows where
    'power' is NaN.

    Args:
        df: Raw PVOutput DataFrame with 'date', 'time', and 'power' columns.

    Returns:
        Cleaned DataFrame with 'time' (UTC) and 'power' columns.
    """
    result = df.copy()

    # Synthesise UTC time from date + time columns
    result['time'] = (
        pd.to_datetime(result['date'].astype(str) + 'T' + result['time'])
        .apply(_undo_uk_daylight_savings)
    )
    result.drop('date', axis=1, inplace=True)

    # Keep only time and power
    result = result[['time', 'power']]

    # Drop rows where power is NaN
    result = result.dropna(subset=['power'])

    return result


def _undo_uk_daylight_savings(dt_local):
    """Convert UK local time to UTC, accounting for daylight savings."""
    return UKTZ.localize(dt_local).astimezone(UTC).replace(tzinfo=None)
