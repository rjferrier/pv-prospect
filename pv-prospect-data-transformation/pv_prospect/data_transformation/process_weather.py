import pandas as pd

from pv_prospect.data_transformation.helpers.data_operations import reduce_rows


DEFAULT_KEEP_COLUMNS = ('temperature', 'direct_normal_irradiance', 'diffuse_radiation')


def process_weather(
    df: pd.DataFrame,
    keep_columns: tuple[str, ...] = DEFAULT_KEEP_COLUMNS,
    timescale_days: int | None = 1,
) -> pd.DataFrame:
    """
    Process cleaned weather data for weather-model training.

    Selects a subset of columns and optionally downsamples time resolution.

    Args:
        df: Cleaned weather DataFrame (output of clean_weather).
        keep_columns: Column names to retain alongside 'time'.
        timescale_days: If set, downsample to this many days using
            time-weighted averaging. None keeps original resolution.

    Returns:
        Processed DataFrame with 'time' and the specified columns.
    """
    df['time'] = pd.to_datetime(df['time'])

    columns_to_keep = ['time'] + [col for col in keep_columns if col in df.columns]
    result = df[columns_to_keep].copy()

    if timescale_days is not None and timescale_days > 0 and not result.empty:
        result = _downsample(result, timescale_days)

    return result


def _downsample(df: pd.DataFrame, timescale_days: int) -> pd.DataFrame:
    """Downsample a DataFrame using time-weighted averaging."""
    start_time = df['time'].min().normalize()
    end_time = df['time'].max()
    ref_times = pd.date_range(
        start=start_time + pd.Timedelta(days=timescale_days),
        end=end_time + pd.Timedelta(days=timescale_days),
        freq=f'{timescale_days}D',
    )
    return reduce_rows(df, ref_times.to_series())
