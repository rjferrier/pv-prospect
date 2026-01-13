from dataclasses import dataclass
import re

import pandas as pd
from pv_prospect.common.domain.bounding_box import Vertex, VertexLabel
from pv_prospect.common.domain.location import Location


# Pattern for columns that should not be interpolated (use nearest neighbor instead)
NON_INTERPOLABLE_COLUMN_PATTERN = re.compile(r'weather_code')


@dataclass(frozen=True)
class VertexData:
    """Data for a single vertex of a grid cell, containing location metadata and timeseries data."""
    vertex: Vertex
    dataframe: pd.DataFrame


@dataclass(frozen=True)
class CellData:
    """Data for a grid cell with four vertices (SW, SE, NW, NE), used for spatial interpolation."""
    sw: VertexData
    se: VertexData
    nw: VertexData
    ne: VertexData

    @classmethod
    def from_vertices(cls, vertex_data_list: list[VertexData]) -> 'CellData':
        """
        Create a CellData object from a dictionary of labeled VertexData objects.

        This preserves the original corner labels from the bounding box,
        which is important because cell vertices may not be exactly orthogonal.

        Args:
            vertex_data_list: List of VertexData objects

        Returns:
            CellData object with properly assigned corners

        Raises:
            ValueError: If required corners are missing
        """
        required_corners = set(VertexLabel)
        provided_corners = set(v.vertex.label for v in vertex_data_list)

        if not required_corners.issubset(provided_corners):
            missing = required_corners - provided_corners
            raise ValueError(f"Missing required corners: {missing}")

        def select_vertex_data(vertex_label: VertexLabel) -> VertexData:
            return next(v for v in vertex_data_list if v.vertex.label is vertex_label)

        return cls(
            sw=select_vertex_data(VertexLabel.SW),
            se=select_vertex_data(VertexLabel.SE),
            nw=select_vertex_data(VertexLabel.NW),
            ne=select_vertex_data(VertexLabel.NE)
        )

    def bilinear_interpolate(
        self,
        target_location: Location,
        columns_to_interpolate: list[str]
    ) -> pd.DataFrame:
        """
        Perform bilinear interpolation on dataframes from four bounding box vertices.

        Args:
            target_location: Location object with target latitude and longitude
            columns_to_interpolate: List of column names to interpolate

        Returns:
            DataFrame with bilinearly interpolated values at the target location
        """
        # Get corner locations and dataframes
        sw_loc = self.sw.vertex.location
        se_loc = self.se.vertex.location
        nw_loc = self.nw.vertex.location
        ne_loc = self.ne.vertex.location

        df_sw = self.sw.dataframe
        df_se = self.se.dataframe
        df_nw = self.nw.dataframe
        df_ne = self.ne.dataframe

        # Ensure all dataframes have the same time index
        if not (df_sw['time'].equals(df_se['time']) and
                df_sw['time'].equals(df_nw['time']) and
                df_sw['time'].equals(df_ne['time'])):
            raise ValueError("All vertex dataframes must have the same time index")

        # Get bounding box coordinates
        lat_min = min(sw_loc.latitude, se_loc.latitude, nw_loc.latitude, ne_loc.latitude)
        lat_max = max(sw_loc.latitude, se_loc.latitude, nw_loc.latitude, ne_loc.latitude)
        lon_min = min(sw_loc.longitude, se_loc.longitude, nw_loc.longitude, ne_loc.longitude)
        lon_max = max(sw_loc.longitude, se_loc.longitude, nw_loc.longitude, ne_loc.longitude)

        # Calculate normalized coordinates (0 to 1)
        if lat_max != lat_min:
            x = float((target_location.latitude - lat_min) / (lat_max - lat_min))
        else:
            x = 0.5

        if lon_max != lon_min:
            y = float((target_location.longitude - lon_min) / (lon_max - lon_min))
        else:
            y = 0.5

        # Bilinear interpolation weights
        w_sw = (1 - x) * (1 - y)
        w_se = (1 - x) * y
        w_nw = x * (1 - y)
        w_ne = x * y

        # Create result dataframe starting with time column
        result = pd.DataFrame({'time': df_sw['time'].copy()})

        # Interpolate each column
        for col in columns_to_interpolate:
            if col == 'time':
                continue

            # Check if column exists in all dataframes
            if not all(col in df.columns for df in [df_sw, df_se, df_nw, df_ne]):
                continue

            result[col] = (
                w_sw * df_sw[col] +
                w_se * df_se[col] +
                w_nw * df_nw[col] +
                w_ne * df_ne[col]
            )

        return result

    def select_nearest_non_interpolables(
        self,
        target_location: Location,
        non_interpolable_pattern
    ) -> pd.DataFrame:
        """
        Select non-interpolable columns from the nearest vertex using nearest-neighbor lookup.

        Args:
            target_location: Location object with target latitude and longitude
            non_interpolable_pattern: Regex pattern to identify non-interpolable columns

        Returns:
            DataFrame with time and non-interpolable columns from the nearest vertex
        """
        # Find nearest vertex using Euclidean distance
        vertices = [
            (self.sw.vertex.location, self.sw.dataframe),
            (self.se.vertex.location, self.se.dataframe),
            (self.nw.vertex.location, self.nw.dataframe),
            (self.ne.vertex.location, self.ne.dataframe)
        ]

        min_distance = float('inf')
        nearest_df = None

        for vertex_loc, vertex_df in vertices:
            distance = vertex_loc.euclidean_distance(target_location)

            if distance < min_distance:
                min_distance = distance
                nearest_df = vertex_df

        # Find all non-interpolable columns
        non_interpolable_cols = [
            col for col in nearest_df.columns
            if non_interpolable_pattern.search(col)
        ]

        # Return time column plus non-interpolable columns
        result_cols = ['time'] + non_interpolable_cols
        return nearest_df[result_cols].copy()


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


