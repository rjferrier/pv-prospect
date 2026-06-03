import numpy as np
import pandas as pd

from pv_prospect.common.domain import PVSite
from pv_prospect.data_transformation.helpers import downsample_by_days
from pv_prospect.data_transformation.helpers.data_operations import reduce_rows
from pv_prospect.physics import compute_poa_irradiance

DEFAULT_KEEP_COLUMNS = (
    'temperature',
    'plane_of_array_irradiance',
    'power',
    'power_max',
)


def prepare_pv(
    weather_df: pd.DataFrame,
    pv_df: pd.DataFrame,
    pv_site: PVSite,
    keep_columns: tuple[str, ...] = DEFAULT_KEEP_COLUMNS,
    timescale_days: int | None = 1,
) -> pd.DataFrame:
    """
    Join cleaned weather and PV-output data, calculate POA irradiance, and
    select the final feature set for PV-model training.

    The ``power_max`` column, when requested, is the maximum of the
    native-cadence ``pv_df['power']`` over each output row's period. It is
    derived *before* PV is time-weighted-averaged onto weather cadence —
    otherwise sub-hour clipping is smeared out and the column would
    systematically under-report the true peak. Downstream model training uses
    it as a censoring flag: a row whose ``power_max`` reaches the inverter
    capacity has a biased daily-mean ``power`` and must be excluded.

    Args:
        weather_df: Cleaned weather DataFrame (output of clean_weather).
        pv_df: Cleaned PV-output DataFrame (output of clean_pv).
        pv_site: PVSite containing location and panel geometries.
        keep_columns: Column names to retain alongside 'time'.
        timescale_days: If set, downsample to this many days using
            time-weighted averaging. None keeps original resolution.

    Returns:
        Prepared DataFrame with 'time' and the specified columns.
    """
    weather_df = weather_df.copy()
    weather_df['time'] = pd.to_datetime(weather_df['time'])

    pv_df = pv_df.copy()
    pv_df['time'] = pd.to_datetime(pv_df['time'])

    # Align PV-output times to weather times and inner-join
    pvo_reduced = reduce_rows(pv_df[['time', 'power']], weather_df['time'])
    joined = weather_df.join(pvo_reduced.set_index('time'), on='time', how='inner')

    # Calculate POA irradiance
    joined['plane_of_array_irradiance'] = _calculate_poa_irradiance(joined, pv_site)

    # Select columns (excluding power_max — that's merged in post-downsample
    # from the native-cadence pv_df)
    join_columns = [col for col in keep_columns if col != 'power_max']
    columns_to_keep = ['time'] + [col for col in join_columns if col in joined.columns]
    result = joined[columns_to_keep].copy()

    # Downsample if requested
    if timescale_days is not None and timescale_days > 0 and not result.empty:
        result = downsample_by_days(result, timescale_days)

    # Merge in native-cadence per-period max (computed from pv_df before reduce,
    # so it captures sub-hour peaks that the time-weighted average would hide).
    if 'power_max' in keep_columns and not result.empty:
        result['power_max'] = _lookup_period_max(result['time'], pv_df, timescale_days)

    # Drop rows with any NaN
    result = result.dropna()

    return result


def _lookup_period_max(
    times: pd.Series,
    pv_df: pd.DataFrame,
    timescale_days: int | None,
) -> pd.Series:
    """
    For each timestamp in ``times``, return the max of ``pv_df['power']`` over
    the period beginning at that timestamp.

    When ``timescale_days`` is set, the period is ``[t, t + timescale_days)``
    — matching the period-start labelling of ``downsample_by_days``. When it
    is ``None`` (no aggregation), the period is the calendar day containing
    ``t``.
    """
    if pv_df.empty:
        return pd.Series([float('nan')] * len(times), index=times.index)

    if timescale_days is None:
        target_dates = times.dt.normalize()
        per_day = (
            pv_df.assign(date=pv_df['time'].dt.normalize())
            .groupby('date')['power']
            .max()
        )
        return target_dates.map(per_day)

    period = pd.Timedelta(days=timescale_days)
    return pd.Series(
        [
            pv_df.loc[
                (pv_df['time'] >= t) & (pv_df['time'] < t + period), 'power'
            ].max()
            for t in times
        ],
        index=times.index,
    )


def _calculate_poa_irradiance(df: pd.DataFrame, pv_site: PVSite) -> pd.Series:
    """
    Calculate plane-of-array irradiance for each row in the dataframe.

    Sums the shared POA calculation (``pv_prospect.physics``) over the site's
    panel geometries, weighted by ``area_fraction``.

    Args:
        df: DataFrame containing 'time', 'direct_normal_irradiance', and
            'diffuse_radiation' columns.
        pv_site: PVSite with location and panel_geometries.

    Returns:
        Series with POA irradiance values.
    """
    # Subtract 30 minutes because data is right-labelled hourly intervals
    times = pd.DatetimeIndex(df['time'] - pd.Timedelta('30min'))
    dni = df['direct_normal_irradiance'].values
    dhi = df['diffuse_radiation'].values

    poa_total = np.zeros(len(df))
    for panel_geometry in pv_site.panel_geometries:
        poa_total += (
            compute_poa_irradiance(times, dni, dhi, pv_site.location, panel_geometry)
            * panel_geometry.area_fraction
        )

    return pd.Series(poa_total, index=df.index)
