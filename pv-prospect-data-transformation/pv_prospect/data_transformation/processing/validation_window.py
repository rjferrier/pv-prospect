"""Validation window producer — rolling 90-day serving artifact."""

import json
import logging
from datetime import datetime, timezone

import pandas as pd

from pv_prospect.etl.storage.base import FileSystem

from .core import (
    PV_COLUMNS,
    PV_PREPARED_PREFIX,
    merge_prepared_frames,
    read_csv,
    write_csv,
)

logger = logging.getLogger(__name__)

WINDOW_CSV = 'window.csv'
WINDOW_MANIFEST = 'manifest.json'
WINDOW_COLUMNS = ['system_id'] + PV_COLUMNS
_WINDOW_KEYS = ['system_id', 'time']


def _parse_system_id(name: str) -> int | None:
    """Parse system_id from a partition filename like pv_{id}_{start}_{end}.csv."""
    if not name.startswith('pv_') or not name.endswith('.csv'):
        return None
    stem = name.removeprefix('pv_').removesuffix('.csv')
    parts = stem.split('_')
    if len(parts) != 3:
        return None
    try:
        return int(parts[0])
    except ValueError:
        return None


def read_prepared_pv(prepared_fs: FileSystem) -> pd.DataFrame:
    """Read all PV partitions from prepared_fs into a single DataFrame.

    Returns a DataFrame with WINDOW_COLUMNS. Skips stray aggregate files
    whose names don't match the three-part partition pattern.
    """
    entries = prepared_fs.list_files(PV_PREPARED_PREFIX, 'pv_*.csv', recursive=True)
    frames = []
    for entry in entries:
        system_id = _parse_system_id(entry.name)
        if system_id is None:
            continue
        df = read_csv(prepared_fs, entry.path)
        df.insert(0, 'system_id', system_id)
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=WINDOW_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def trim_window(df: pd.DataFrame, days: int) -> pd.DataFrame:
    """Drop rows older than `days` before the most-recent timestamp in df."""
    if df.empty:
        return df.reset_index(drop=True)
    cutoff = df['time'].max() - pd.Timedelta(days=days)
    return df[df['time'] >= cutoff].reset_index(drop=True)


def assemble_window(frames: list[pd.DataFrame], days: int) -> pd.DataFrame:
    """Merge frames, deduplicate, trim to the window, and order columns.

    Pass frames as [prev, incr] so that when the same (system_id, time) key
    appears in both, the incr row wins (keep='last').
    """
    merged = merge_prepared_frames(frames, _WINDOW_KEYS)
    trimmed = trim_window(merged, days)
    return trimmed[WINDOW_COLUMNS]


def build_manifest(
    window: pd.DataFrame,
    days: int,
    now: datetime | None = None,
) -> dict:
    """Build the manifest dict describing the current window."""
    moment = (now or datetime.now(timezone.utc)).replace(microsecond=0)
    manifest: dict = {
        'updated_at': moment.isoformat(),
        'window_days': days,
    }
    if window.empty:
        manifest['window_start'] = None
        manifest['window_end'] = None
        manifest['row_counts'] = {}
    else:
        manifest['window_start'] = window['time'].min().date().isoformat()
        manifest['window_end'] = window['time'].max().date().isoformat()
        manifest['row_counts'] = {
            str(k): int(v) for k, v in window.groupby('system_id').size().items()
        }
    return manifest


def read_window(window_fs: FileSystem) -> pd.DataFrame | None:
    """Read the current window artifact; returns None if not yet seeded."""
    if not window_fs.exists(WINDOW_CSV):
        return None
    return read_csv(window_fs, WINDOW_CSV)


def write_window(
    window_fs: FileSystem,
    window: pd.DataFrame,
    manifest: dict,
) -> None:
    if list(window.columns) != WINDOW_COLUMNS:
        raise ValueError(
            f'Expected columns {WINDOW_COLUMNS}, got {list(window.columns)}'
        )
    write_csv(window_fs, window, WINDOW_CSV)
    window_fs.write_text(WINDOW_MANIFEST, json.dumps(manifest, indent=2))


def seed_validation_window(
    prepared_fs: FileSystem,
    window_fs: FileSystem,
    days: int,
    now: datetime | None = None,
) -> None:
    """Bootstrap the validation window from a locally-pulled prepared corpus.

    Reads all PV partitions from prepared_fs, trims to the last `days` days,
    and writes the artifact to window_fs. Safe to re-run — it overwrites any
    existing artifact.
    """
    incr = read_prepared_pv(prepared_fs)
    window = assemble_window([incr], days)
    manifest = build_manifest(window, days, now)
    write_window(window_fs, window, manifest)
    logger.info(
        'Validation window seeded: %s rows across %d sites',
        len(window),
        window['system_id'].nunique() if not window.empty else 0,
    )


class ValidationWindowNotSeededError(RuntimeError):
    pass


def run_maintain_validation_window(
    prepared_fs: FileSystem,
    window_fs: FileSystem,
    days: int,
    now: datetime | None = None,
) -> None:
    """Merge today's prepared PV rows into the rolling validation window.

    Raises ValidationWindowNotSeededError if the window artifact is absent —
    the daily step never bootstraps from scratch.
    """
    prev = read_window(window_fs)
    if prev is None:
        raise ValidationWindowNotSeededError(
            'Validation window has not been seeded; run seed_validation_window first.'
        )
    incr = read_prepared_pv(prepared_fs)
    window = assemble_window([prev, incr], days)
    manifest = build_manifest(window, days, now)
    write_window(window_fs, window, manifest)
    logger.info(
        'Validation window updated: %s rows across %d sites',
        len(window),
        window['system_id'].nunique(),
    )
