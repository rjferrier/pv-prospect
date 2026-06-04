"""One-time seed for the validation window artifact.

Reads partition CSV files from a locally-pulled prepared PV corpus and writes
a rolling N-day window to the specified destination (local path or GCS URI).

Run once at launch before enabling the daily maintain_validation_window job.
See the top-level README's "Seeding the Validation Window" section for the
full procedure including the targeted DVC pull.

Usage::

    poetry run seed-validation-window \\
        --prepared-dir /path/to/instance-repo/data/prepared \\
        --window-dest gs://pv-prospect-staging/data/served/validation-window \\
        [--days 90]
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

WINDOW_CSV = 'window.csv'
WINDOW_MANIFEST = 'manifest.json'
WINDOW_COLUMNS = [
    'system_id',
    'time',
    'temperature',
    'plane_of_array_irradiance',
    'power',
    'power_max',
]


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


def _read_prepared(prepared_dir: Path) -> pd.DataFrame:
    frames = []
    for csv_path in sorted(prepared_dir.glob('pv/*/pv_*.csv')):
        system_id = _parse_system_id(csv_path.name)
        if system_id is None:
            continue
        df = pd.read_csv(csv_path, parse_dates=['time'])
        df.insert(0, 'system_id', system_id)
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=WINDOW_COLUMNS)
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=['system_id', 'time'], keep='last')
    combined = combined.sort_values(['system_id', 'time']).reset_index(drop=True)
    return combined[WINDOW_COLUMNS]


def _trim(df: pd.DataFrame, days: int) -> pd.DataFrame:
    if df.empty:
        return df
    cutoff = df['time'].max() - pd.Timedelta(days=days)
    return df[df['time'] >= cutoff].reset_index(drop=True)


def _build_manifest(window: pd.DataFrame, days: int) -> dict:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    manifest: dict = {'updated_at': now.isoformat(), 'window_days': days}
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


def _write_local(window: pd.DataFrame, manifest: dict, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    window.to_csv(dest / WINDOW_CSV, index=False)
    (dest / WINDOW_MANIFEST).write_text(json.dumps(manifest, indent=2))
    print(f'Written to {dest}')


def _write_gcs(
    window: pd.DataFrame, manifest: dict, bucket_name: str, prefix: str
) -> None:
    from google.cloud import storage  # noqa: PLC0415

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    def _blob(name: str) -> 'storage.Blob':
        path = f'{prefix}/{name}' if prefix else name
        return bucket.blob(path)

    _blob(WINDOW_CSV).upload_from_string(
        window.to_csv(index=False), content_type='text/csv'
    )
    _blob(WINDOW_MANIFEST).upload_from_string(
        json.dumps(manifest, indent=2), content_type='application/json'
    )
    print(f'Written to gs://{bucket_name}/{prefix}')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--prepared-dir',
        required=True,
        metavar='DIR',
        help='Local path to the prepared data directory (e.g. data/prepared).',
    )
    parser.add_argument(
        '--window-dest',
        required=True,
        metavar='DEST',
        help='Destination: gs://bucket/prefix or a local directory path.',
    )
    parser.add_argument(
        '--days',
        type=int,
        default=90,
        metavar='N',
        help='Window size in days (default: 90).',
    )
    args = parser.parse_args()

    prepared_dir = Path(args.prepared_dir)
    if not prepared_dir.exists():
        print(f'ERROR: --prepared-dir does not exist: {prepared_dir}', file=sys.stderr)
        sys.exit(1)

    window = _trim(_read_prepared(prepared_dir), args.days)
    manifest = _build_manifest(window, args.days)

    dest: str = args.window_dest
    if dest.startswith('gs://'):
        uri = dest.removeprefix('gs://')
        bucket_name, _, prefix = uri.partition('/')
        _write_gcs(window, manifest, bucket_name, prefix.rstrip('/'))
    else:
        _write_local(window, manifest, Path(dest))

    n_sites = window['system_id'].nunique() if not window.empty else 0
    print(f'Seeded {len(window)} rows across {n_sites} sites')
    print(f'Window: {manifest.get("window_start")} – {manifest.get("window_end")}')


if __name__ == '__main__':
    main()
