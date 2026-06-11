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

    POA irradiance and temperature are computed over the **full 24 h** weather
    day, decoupled from PV-power coverage. When ``timescale_days`` is set, the
    daily mean covers all hours — matching the 24 h-mean convention used by
    ``reconstruct_daily_mean_poa`` at inference. Power is likewise computed on
    an energy basis (total W·h / 24 h), so capacity factor aligns with the
    integration step in ``chain.py`` (CF × capacity × 24 h).

    For ``timescale_days=None`` (no aggregation), POA is computed per weather
    timestamp and PV is inner-joined onto weather cadence (daytime rows only),
    matching the original hourly behaviour.

    The ``power_max`` column, when requested, is the maximum of the
    native-cadence ``pv_df['power']`` over each output row's period. It is
    derived before any time-averaging so sub-hour clipping events survive into
    the censoring flag. Downstream model training uses it to identify rows
    where ``power`` is a biased low estimate due to inverter clipping.

    Args:
        weather_df: Cleaned weather DataFrame (output of clean_weather).
        pv_df: Cleaned PV-output DataFrame (output of clean_pv).
        pv_site: PVSite containing location and panel geometries.
        keep_columns: Column names to retain alongside 'time'.
        timescale_days: If set, aggregate to this many days using the 24 h
            convention. None keeps original resolution.

    Returns:
        Prepared DataFrame with 'time' and the specified columns.
    """
    weather_df = weather_df.copy()
    weather_df['time'] = pd.to_datetime(weather_df['time'])

    pv_df = pv_df.copy()
    pv_df['time'] = pd.to_datetime(pv_df['time'])

    # Compute POA for all weather hours (including night zeros)
    weather_df['plane_of_array_irradiance'] = _calculate_poa_irradiance(weather_df, pv_site)

    if timescale_days is not None and timescale_days > 0:
        # Aggregate weather over the full 24 h day, decoupled from PV coverage
        weather_feature_cols = ['time'] + [
            c
            for c in keep_columns
            if c not in ('power', 'power_max') and c in weather_df.columns
        ]
        weather_agg = downsample_by_days(weather_df[weather_feature_cols], timescale_days)

        if weather_agg.empty:
            return pd.DataFrame(columns=['time'] + list(keep_columns))

        # Energy-based 24 h-mean power: total W·s in period / period seconds
        power_agg = _compute_24h_mean_power(pv_df, weather_agg['time'], timescale_days)

        # Inner-join: keep only periods where PV data exists
        result = weather_agg.merge(power_agg, on='time', how='inner')
    else:
        # No aggregation: inner-join PV power onto weather cadence (daytime rows)
        pvo_reduced = reduce_rows(pv_df[['time', 'power']], weather_df['time'])
        result = weather_df.join(pvo_reduced.set_index('time'), on='time', how='inner')

    # Merge in native-cadence per-period max (computed from pv_df before any
    # time-averaging so sub-hour clipping events survive into the flag)
    if 'power_max' in keep_columns and not result.empty:
        result['power_max'] = _lookup_period_max(result['time'], pv_df, timescale_days)

    # Select and order final columns
    final_cols = ['time'] + [c for c in keep_columns if c in result.columns]
    result = result[final_cols].copy()

    return result.dropna()


def _compute_24h_mean_power(
    pv_df: pd.DataFrame,
    period_starts: pd.Series,
    timescale_days: int,
) -> pd.DataFrame:
    """
    Return 24 h-mean power for each period: total W·s in period / period seconds.

    Uses the right-labelled interval convention (same as ``reduce_rows``): for
    each PV reading the interval begins at the previous reading, or is inferred
    from the next reading for the first in a period. Night hours implicitly
    contribute 0 W·h. Returns a DataFrame with ``'time'`` and ``'power'``.
    """
    period_s = float(timescale_days * 24 * 3600)
    period_delta = pd.Timedelta(days=timescale_days)

    pv = pv_df[['time', 'power']].copy()
    pv = pv.sort_values('time').reset_index(drop=True)

    rows: list[dict] = []
    for t_start in period_starts:
        t_end = t_start + period_delta
        mask = (pv['time'] > t_start) & (pv['time'] <= t_end)
        chunk = pv[mask].reset_index(drop=True)
        n = len(chunk)
        if n < 2:
            continue

        times = chunk['time'].values
        powers = chunk['power'].values

        total_ws = 0.0
        for i in range(n):
            if i == 0:
                dt = (times[1] - times[0]) / np.timedelta64(1, 's')
            else:
                dt = (times[i] - times[i - 1]) / np.timedelta64(1, 's')
            total_ws += float(powers[i]) * dt

        rows.append({'time': t_start, 'power': total_ws / period_s})

    if not rows:
        return pd.DataFrame(columns=['time', 'power'])
    return pd.DataFrame(rows)


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
