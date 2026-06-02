"""PV feature building: load prepared partitions, apply censoring, compute targets.

Walks per-site prepared CSV partitions under ``{data_root}/pv/{system_id}/``,
joins each site's data with capacities and installation_date from
``pv_sites.csv``, applies the inverter-clipping censoring filter using
``power_max`` (emitted by ``prepare_pv``), computes the target
``capacity_factor = power / panels_capacity``, and augments ``day_of_year``,
``age_years``, and ``age_known`` features.

Ported from ``pv-prospect-instance/data-exploration/main_models/common_cross_site.py``,
which was validated against the prepared data (smoke run 2026-05-26).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

CONTINUOUS_FEATURES = [
    'day_of_year',
    'temperature',
    'plane_of_array_irradiance',
    'age_years',
]
BINARY_FEATURES = ['age_known']
TARGET_COLUMN = 'capacity_factor'

DEFAULT_CENSORING_MARGIN = 0.01


def load_prepared_pv_partitions(data_root: Path, system_id: int) -> pd.DataFrame:
    """Concatenate every ``pv_*.csv`` partition under ``{data_root}/pv/{system_id}/``.

    Raises ``FileNotFoundError`` if the directory is absent or contains no
    matching files.
    """
    site_dir = Path(data_root) / 'pv' / str(system_id)
    if not site_dir.is_dir():
        raise FileNotFoundError(
            f'No prepared PV directory for site {system_id}: {site_dir}'
        )
    files = sorted(site_dir.glob('pv_*.csv'))
    if not files:
        raise FileNotFoundError(f'No prepared PV partitions under {site_dir}')
    frames = [pd.read_csv(f, parse_dates=['time']) for f in files]
    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values('time').reset_index(drop=True)
    df['system_id'] = system_id
    return df


def load_site_metadata(pv_sites_csv: Path) -> pd.DataFrame:
    """Read ``pv_sites.csv`` and keep only the columns the PV model uses."""
    sites = pd.read_csv(pv_sites_csv)
    sites['installation_date'] = pd.to_datetime(
        sites['installation_date'], errors='coerce'
    )
    return sites[
        [
            'pvoutput_system_id',
            'panels_capacity',
            'inverter_capacity',
            'installation_date',
        ]
    ]


def attach_site_metadata(pv_df: pd.DataFrame, sites_df: pd.DataFrame) -> pd.DataFrame:
    """Left-join site capacities and installation_date onto the PV frame."""
    return pv_df.merge(
        sites_df,
        how='left',
        left_on='system_id',
        right_on='pvoutput_system_id',
    ).drop(columns=['pvoutput_system_id'])


def apply_censoring_filter(
    df: pd.DataFrame, margin: float = DEFAULT_CENSORING_MARGIN
) -> pd.DataFrame:
    """Drop rows whose daily ``power_max`` indicates inverter clipping.

    A row is censored when within-day instantaneous power reached the inverter
    cap; ``power_max > inverter_capacity * (1 - margin)`` detects this cheaply.
    The 1% default margin absorbs measurement noise around the cap (verified
    against site 82517: the cap is sharp at 6000 W, max observed 6004 W).
    """
    threshold = df['inverter_capacity'] * (1 - margin)
    return df[df['power_max'] <= threshold].copy()


def compute_age_years(
    times: pd.Series,
    install_dates: pd.Series,
    fill_with: float | None = None,
) -> tuple[pd.Series, pd.Series]:
    """Return ``(age_years, age_known)`` aligned to ``times``.

    Missing ``installation_date`` → ``age_known = 0``; ``age_years`` gets
    ``fill_with`` (or the global median of known ages if ``fill_with`` is
    ``None``). Using age-at-row-time rather than installation year means
    a 2014 install in 2020 vs 2026 contributes the correct degradation
    signal in both rows.
    """
    delta_days = (times - install_dates).dt.days
    age_years = delta_days / 365.25
    age_known = age_years.notna().astype(int)
    if fill_with is None:
        known = age_years.dropna()
        fill_with = float(known.median()) if not known.empty else 0.0
    age_years = age_years.fillna(fill_with)
    return age_years, age_known


def augment_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``day_of_year``, ``age_years``, ``age_known``, ``capacity_factor``.

    Expects columns: ``time``, ``power``, ``panels_capacity``,
    ``installation_date`` (all present after ``attach_site_metadata``).
    """
    out = df.copy()
    out['day_of_year'] = out['time'].dt.dayofyear
    out['age_years'], out['age_known'] = compute_age_years(
        out['time'], out['installation_date']
    )
    out[TARGET_COLUMN] = out['power'] / out['panels_capacity']
    return out


def build_pv_features(
    data_root: Path,
    pv_sites_csv: Path,
    system_ids: list[int] | None = None,
    censoring_margin: float = DEFAULT_CENSORING_MARGIN,
) -> pd.DataFrame:
    """Walk loaders → feature engineering → censoring → return feature frame.

    ``system_ids = None`` reads every site listed in ``pv_sites.csv`` whose
    prepared directory exists under ``data_root``; missing sites are skipped
    with a printed warning rather than raising.

    Returns a DataFrame sorted by ``time`` with feature columns
    (CONTINUOUS_FEATURES + BINARY_FEATURES + TARGET_COLUMN) plus auxiliary
    columns (``system_id``, ``power``, ``panels_capacity``,
    ``inverter_capacity``) needed for evaluation.
    """
    sites_df = load_site_metadata(pv_sites_csv)
    if system_ids is None:
        system_ids = sites_df['pvoutput_system_id'].astype(int).tolist()

    per_site: list[pd.DataFrame] = []
    for sid in system_ids:
        try:
            per_site.append(load_prepared_pv_partitions(data_root, sid))
        except FileNotFoundError as e:
            print(f'  ! Skipping site {sid}: {e}')
    if not per_site:
        raise RuntimeError(f'No prepared PV partitions found under {data_root}/pv/')

    df = pd.concat(per_site, ignore_index=True)
    df = attach_site_metadata(df, sites_df)
    # Sort globally by time: the training pipeline carves a temporal val slice
    # off the tail of train, which only makes sense if the tail is latest dates.
    df = df.sort_values('time', kind='mergesort').reset_index(drop=True)

    n_before = len(df)
    df = apply_censoring_filter(df, margin=censoring_margin)
    n_after = len(df)
    dropped_pct = 100.0 * (n_before - n_after) / n_before if n_before else 0.0
    print(
        f'Censoring filter dropped {n_before - n_after} of {n_before} rows '
        f'({dropped_pct:.1f}%) at margin={censoring_margin}'
    )

    df = augment_features(df)
    df = df.dropna(
        subset=CONTINUOUS_FEATURES + BINARY_FEATURES + [TARGET_COLUMN]
    ).reset_index(drop=True)

    return df
