import pandas as pd


def reduce_rows(df: pd.DataFrame, ref_times: pd.Series) -> pd.DataFrame:
    """
    Reduce rows of a dataframe to match reference times using time-weighted averaging.

    Each value in the input dataframe is assumed to represent the time-averaged value
    over the interval from the previous timestamp to the current timestamp.

    Args:
        df: DataFrame with 'time' column and other numeric columns to average
        ref_times: Series of target timestamps to aggregate to

    Returns:
        DataFrame with rows at reference_times, with time-weighted averaged values
        for all non-time columns. Rows are omitted where there's insufficient data.
    """
    if df.empty or ref_times.empty:
        return pd.DataFrame(columns=df.columns)

    # Ensure time column is datetime with timezone
    if 'time' not in df.columns:
        raise ValueError("DataFrame must have a 'time' column")

    # Sort both by time
    df = df.sort_values('time').reset_index(drop=True)
    ref_times = ref_times.sort_values().reset_index(drop=True)

    # Get value columns (all except 'time')
    value_columns = [col for col in df.columns if col != 'time']

    result_rows = []

    for ref_time in ref_times:
        # Find all intervals that contribute to this reference time
        # We need rows where the interval ends at or before ref_time
        # and starts after the previous reference time (if any)

        # Get the previous reference time (or earliest time in data)
        prev_ref_idx = ref_times[ref_times < ref_time]
        if len(prev_ref_idx) > 0:
            start_time = prev_ref_idx.iloc[-1]
        else:
            # This is the first reference time, use earliest data time
            start_time = df['time'].iloc[0]

        # Find rows in the interval (start_time, ref_time]
        # The interval for each row is from previous row time to current row time
        mask = (df['time'] > start_time) & (df['time'] <= ref_time)
        interval_rows = df[mask]

        if interval_rows.empty:
            # Not enough data for this reference time, skip it
            continue

        # Calculate time-weighted average for each value column
        row_data = {'time': ref_time}

        # Build list of interval start and end times
        interval_starts = []
        interval_ends = []

        for i, idx in enumerate(interval_rows.index):
            # End time is the current row's time
            end_time = df.loc[idx, 'time']
            interval_ends.append(end_time)

            # Start time is the previous row's time
            if idx > 0:
                start_time = df.loc[idx - 1, 'time']
            else:
                # First row - infer time step
                if len(df) > 1:
                    time_step = df['time'].iloc[1] - df['time'].iloc[0]
                    start_time = df.loc[idx, 'time'] - time_step
                else:
                    # Only one row total, skip this reference time
                    break
            interval_starts.append(start_time)

        # If we broke out of the loop, skip this reference time
        if len(interval_starts) != len(interval_ends):
            continue

        # Calculate durations for each interval
        durations = []
        for start, end in zip(interval_starts, interval_ends):
            duration = (end - start).total_seconds()
            durations.append(duration)

        total_duration = sum(durations)

        if total_duration == 0:
            continue

        # Calculate weighted average for each value column
        for col in value_columns:
            values = interval_rows[col].values

            # Check if any value is NaN
            if pd.isna(values).any():
                row_data[col] = float('nan')
            else:
                # Time-weighted average
                weighted_sum = sum(val * dur for val, dur in zip(values, durations))
                row_data[col] = weighted_sum / total_duration

        result_rows.append(row_data)

    if not result_rows:
        return pd.DataFrame(columns=df.columns)

    result_df = pd.DataFrame(result_rows)
    return result_df
