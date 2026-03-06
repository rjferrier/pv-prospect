import pandas as pd

from pv_prospect.data_transformation.helpers.data_operations import reduce_rows


# Default columns to exclude from output
DEFAULT_EXCLUDED_COLUMNS = {
    'pressure_msl',
    'wind_direction_80m',
    'wind_direction_180m',
}


def clean_weather(
    df: pd.DataFrame,
    weather_model: str = 'best_match',
    excluded_columns: set[str] | None = None,
    timescale_days: int | None = None,
) -> pd.DataFrame:
    """
    Clean raw OpenMeteo weather data.

    Selects columns matching the given weather model suffix, strips the suffix
    to produce clean names, drops excluded columns, and optionally downsamples
    time resolution.

    Args:
        df: Raw weather DataFrame with model-suffixed column names
            (e.g. 'temperature_best_match').
        weather_model: Weather model name whose columns to select
            (default: 'best_match').
        excluded_columns: Column names (without model suffix) to drop.
            Defaults to DEFAULT_EXCLUDED_COLUMNS if None.
        timescale_days: If set, downsample to this many days using
            time-weighted averaging. None keeps original resolution.

    Returns:
        Cleaned DataFrame with 'time' and selected weather columns.
    """
    if excluded_columns is None:
        excluded_columns = DEFAULT_EXCLUDED_COLUMNS

    df['time'] = pd.to_datetime(df['time'])

    result = pd.DataFrame()
    result['time'] = df['time']

    suffix = f'_{weather_model}'
    for col in df.columns:
        if col.endswith(suffix):
            clean_name = col[:-len(suffix)]
            if clean_name not in excluded_columns:
                result[clean_name] = df[col]

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
