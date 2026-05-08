import pandas as pd

from pv_prospect.data_transformation.helpers import downsample_by_days

DEFAULT_KEEP_COLUMNS = ('temperature', 'direct_normal_irradiance', 'diffuse_radiation')


def prepare_weather(
    df: pd.DataFrame,
    keep_columns: tuple[str, ...] = DEFAULT_KEEP_COLUMNS,
    timescale_days: int | None = 1,
) -> pd.DataFrame:
    """
    Prepare cleaned weather data for weather-model training.

    Selects a subset of columns and optionally downsamples time resolution.

    Args:
        df: Cleaned weather DataFrame (output of clean_weather).
        keep_columns: Column names to retain alongside 'time'.
        timescale_days: If set, downsample to this many days using
            time-weighted averaging. None keeps original resolution.

    Returns:
        Prepared DataFrame with 'time' and the specified columns.
    """
    df['time'] = pd.to_datetime(df['time'])

    columns_to_keep = ['time'] + [col for col in keep_columns if col in df.columns]
    result = df[columns_to_keep].copy()

    if timescale_days is not None and timescale_days > 0 and not result.empty:
        result = downsample_by_days(result, timescale_days)

    return result
