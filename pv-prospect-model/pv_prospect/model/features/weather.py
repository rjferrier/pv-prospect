"""Weather feature building: load prepared partitions, monthly aggregation, cyclic encoding.

Walks per-grid prepared CSV partitions under ``{data_root}/weather/``,
concatenates them, collapses daily rows to monthly means (discards the
unlearnable within-month cloud-driven noise — see ``downsample_to_monthly``),
and adds cyclic ``day_of_year`` features.

Ported from ``pv-prospect-instance/data-exploration/main_models/common_cross_grid.py``,
validated against the prepared weather corpus (spatial-block evaluation 2026-06-02).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

FEATURE_COLUMNS = [
    'latitude',
    'longitude',
    'elevation',
    'day_of_year_sin',
    'day_of_year_cos',
]
TARGET_COLUMNS = [
    'temperature',
    'direct_normal_irradiance',
    'diffuse_radiation',
]

# 365.25 keeps the sin/cos pair aligned across leap years without a year-boundary
# discontinuity.
_DAYS_PER_YEAR = 365.25


def load_prepared_weather_partitions(data_root: Path) -> pd.DataFrame:
    """Concat every ``weather_*.csv`` partition under ``{data_root}/weather/``.

    Each partition already carries ``latitude``, ``longitude``, and ``elevation``
    (attached at assemble time in the transformation pipeline), so no metadata
    join is needed.

    Raises ``FileNotFoundError`` if the directory is absent or contains no
    matching files.
    """
    weather_dir = Path(data_root) / 'weather'
    if not weather_dir.is_dir():
        raise FileNotFoundError(f'No prepared weather directory: {weather_dir}')
    files = sorted(weather_dir.glob('weather_*.csv'))
    if not files:
        raise FileNotFoundError(f'No prepared weather partitions under {weather_dir}')
    frames = [pd.read_csv(f, parse_dates=['time']) for f in files]
    df = pd.concat(frames, ignore_index=True)
    return df.sort_values(['time', 'latitude', 'longitude']).reset_index(drop=True)


def downsample_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse each grid point's daily rows to one monthly-mean row.

    The native prepared cadence is daily. The feature set ``(lat, lon,
    elevation, day_of_year)`` has no access to within-month variance — for
    DNI that variance is almost entirely cloud-driven noise the model cannot
    explain. Averaging each grid point's calendar month keeps the
    seasonal/geographic signal the features *can* express and discards the
    unlearnable high-frequency component.

    Duplicate ``(grid, day)`` rows (produced by overlapping sliding-window
    extracts in the most recent month) are de-duplicated first so no day is
    double-weighted. The representative ``time`` is set to the month midpoint
    so the downstream cyclic ``day_of_year`` encoding lands in the centre of
    the averaged period.
    """
    deduped = df.drop_duplicates(subset=['latitude', 'longitude', 'time'])
    month = deduped['time'].dt.to_period('M').rename('month')
    monthly = (
        deduped.groupby(['latitude', 'longitude', month])
        .agg(
            elevation=('elevation', 'first'),
            temperature=('temperature', 'mean'),
            direct_normal_irradiance=('direct_normal_irradiance', 'mean'),
            diffuse_radiation=('diffuse_radiation', 'mean'),
        )
        .reset_index()
    )
    monthly['time'] = monthly['month'].dt.to_timestamp() + pd.Timedelta(days=14)
    monthly = monthly.drop(columns='month')
    return monthly.sort_values(['time', 'latitude', 'longitude']).reset_index(drop=True)


def add_cyclic_day_of_year(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``day_of_year_sin`` and ``day_of_year_cos`` derived from ``time``.

    A naive integer ``day_of_year`` would put day 365 and day 1 maximally
    apart in feature space. The sin/cos pair makes the encoding cyclic so the
    model learns a smooth seasonal response around the year boundary.
    """
    out = df.copy()
    day = out['time'].dt.dayofyear
    angle = 2 * np.pi * day / _DAYS_PER_YEAR
    out['day_of_year_sin'] = np.sin(angle)
    out['day_of_year_cos'] = np.cos(angle)
    return out


def build_weather_features(data_root: Path) -> pd.DataFrame:
    """Load → monthly downsample → cyclic features → return feature frame.

    Returns a DataFrame sorted by ``(time, latitude, longitude)`` containing
    FEATURE_COLUMNS, TARGET_COLUMNS, and the auxiliary ``time`` column needed
    for the temporal split and block-climatology evaluation.
    """
    df = load_prepared_weather_partitions(data_root)
    df = downsample_to_monthly(df)
    df = add_cyclic_day_of_year(df)
    df = df.dropna(subset=FEATURE_COLUMNS + TARGET_COLUMNS).reset_index(drop=True)
    return df
