"""Local runner — replaces the Cloud Run Job entrypoint for local development.

Orchestrates transformation steps using :class:`concurrent.futures.ThreadPoolExecutor`
instead of Cloud Run Jobs.

Usage::

    python -m pv_prospect.data_transformation.runner \
        clean_weather,process_weather \
        --start-date 2025-06-01 --end-date 2025-06-30 \
        --local-dir ./out --workers 4
"""

import io
from argparse import ArgumentParser, RawTextHelpFormatter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any

import pandas as pd

from pv_prospect.common import (
    DateRange,
    Period,
    build_location_mapping_repo,
    build_pv_site_repo,
    get_all_pv_system_ids,
    get_config,
    get_pv_site_by_system_id,
)
from pv_prospect.data_sources import (
    LOCATION_MAPPING_CSV_FILE,
    PV_SITES_CSV_FILE,
    SourceDescriptor,
)
from pv_prospect.data_transformation.config import DataTransformationConfig
from pv_prospect.data_transformation.transformations import (
    clean_pvoutput,
    clean_weather,
    process_pv,
    process_weather,
)
from pv_prospect.etl import TIMESERIES_FOLDER, Extractor, get_config_dir
from pv_prospect.etl.storage import FileSystem, get_filesystem
from pv_prospect.etl.storage.backends import LocalStorageConfig

CLEANED_PREFIX = 'cleaned'
PROCESSED_PREFIX = 'processed'

STEPS = ['clean_weather', 'clean_pvoutput', 'process_weather', 'process_pv']
STEPS_NEEDING_PV_ID: frozenset[str] = frozenset({'clean_pvoutput', 'process_pv'})


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def _parse_args() -> Any:
    parser = ArgumentParser(
        prog='data-transformation-runner',
        formatter_class=lambda prog: RawTextHelpFormatter(prog, width=120),
    )
    parser.add_argument(
        'steps',
        help='transformation step(s), comma-separated from: {}'.format(
            ', '.join(STEPS)
        ),
    )
    parser.add_argument(
        'system_ids',
        nargs='?',
        default=None,
        help='system ID or comma-separated list of system IDs (e.g. 123 or 123,456). '
        'Required for steps: {}. If omitted, all systems are processed.'.format(
            ', '.join(sorted(STEPS_NEEDING_PV_ID))
        ),
    )
    parser.add_argument(
        '-d',
        '--start-date',
        type=str,
        default=None,
        help="start date: 'today', 'yesterday', YYYY-MM-DD, or YYYY-MM (default: yesterday)",
    )
    parser.add_argument(
        '-e',
        '--end-date',
        type=str,
        default=None,
        help="end date: 'today', 'yesterday', YYYY-MM-DD, or YYYY-MM (default: start date + 1 day)",
    )
    parser.add_argument(
        '-l',
        '--local-dir',
        type=str,
        default=None,
        help='local directory for both raw and model data (instead of GCS)',
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='max parallel threads (default: 4)',
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------


def _parse_date(date_str: str) -> date:
    if date_str.lower() == 'today':
        return date.today()
    elif date_str.lower() == 'yesterday':
        return date.today() - timedelta(days=1)
    if len(date_str) == 7 and date_str[4] == '-':
        try:
            year, month = date_str.split('-')
            return date(int(year), int(month), 1)
        except (ValueError, IndexError):
            pass
    return date.fromisoformat(date_str)


def _is_month_format(date_str: str) -> bool:
    return len(date_str) == 7 and date_str[4] == '-' and date_str.count('-') == 1


def _get_last_day_of_month(d: date) -> date:
    next_m = d.month % 12 + 1
    y = d.year + (d.month // 12)
    return date(y, next_m, 1) - timedelta(days=1)


def _get_complete_date_range(args: Any) -> DateRange:
    yesterday = date.today() - timedelta(days=1)
    if args.start_date is None:
        start, start_is_month = yesterday, False
    else:
        start_is_month = _is_month_format(args.start_date)
        start = _parse_date(args.start_date)

    if args.end_date is None:
        end = (
            _get_last_day_of_month(start)
            if start_is_month
            else start + timedelta(days=1)
        )
    else:
        if _is_month_format(args.end_date):
            end = _get_last_day_of_month(_parse_date(args.end_date))
        else:
            end = _parse_date(args.end_date)

    return DateRange(start, end)


def _parse_pv_system_ids(s: str) -> list[int]:
    return [int(x.strip()) for x in s.split(',') if x.strip()]


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def _read_csv(fs: FileSystem, path: str) -> pd.DataFrame | None:
    if not fs.exists(path):
        return None
    return pd.read_csv(io.BytesIO(fs.read_bytes(path)), encoding='utf-8')


def _read_parquet(fs: FileSystem, path: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(fs.read_bytes(path)))


def _write_parquet(fs: FileSystem, df: pd.DataFrame, path: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, engine='pyarrow', index=False)
    fs.write_bytes(path, buf.getvalue())
    print(f'    Written to: {path}')


def _staging_to_cleaned(path: str) -> str:
    return f'{CLEANED_PREFIX}/{path}'.replace('.csv', '.parquet')


# ---------------------------------------------------------------------------
# Repo initialisation
# ---------------------------------------------------------------------------


def _init_repos(raw_fs: FileSystem) -> None:
    extractor = Extractor(raw_fs)
    build_pv_site_repo(extractor.read_file(PV_SITES_CSV_FILE))
    build_location_mapping_repo(extractor.read_file(LOCATION_MAPPING_CSV_FILE))


# ---------------------------------------------------------------------------
# Step functions (explicit params — no os.environ reads)
# ---------------------------------------------------------------------------


def _run_clean_weather(raw_fs: FileSystem, date_str: str) -> None:
    raw_extractor = Extractor(raw_fs)
    weather_prefix = f'{TIMESERIES_FOLDER}/{SourceDescriptor.OPENMETEO_HISTORICAL}'
    for entry in raw_extractor.list_files(weather_prefix, pattern='*.csv'):
        if date_str not in entry.name:
            continue
        print(f'    [clean_weather] Processing {entry.path}')
        df = _read_csv(raw_fs, entry.path)
        if df is not None and not df.empty:
            _write_parquet(raw_fs, clean_weather(df), _staging_to_cleaned(entry.path))


def _run_clean_pvoutput(raw_fs: FileSystem, pv_system_id: int, date_str: str) -> None:
    pv_prefix = f'{TIMESERIES_FOLDER}/{SourceDescriptor.PVOUTPUT}/{pv_system_id}'
    in_path = f'{pv_prefix}/pvoutput_{pv_system_id}_{date_str}.csv'
    print(f'    [clean_pvoutput] Processing {in_path}')
    df = _read_csv(raw_fs, in_path)
    if df is not None and not df.empty:
        _write_parquet(raw_fs, clean_pvoutput(df), _staging_to_cleaned(in_path))


def _run_process_weather(
    raw_fs: FileSystem, model_fs: FileSystem, date_str: str
) -> None:
    raw_extractor = Extractor(raw_fs)
    cleaned_weather_prefix = f'{CLEANED_PREFIX}/{TIMESERIES_FOLDER}/{SourceDescriptor.OPENMETEO_QUARTERHOURLY}'
    for entry in raw_extractor.list_files(cleaned_weather_prefix, pattern='*.parquet'):
        if date_str not in entry.name:
            continue
        print(f'    [process_weather] Processing {entry.path}')
        cleaned_df = _read_parquet(raw_fs, entry.path)
        out_path = (
            f'{PROCESSED_PREFIX}/{TIMESERIES_FOLDER}/'
            f'{SourceDescriptor.OPENMETEO_QUARTERHOURLY}/{entry.name}'
        )
        _write_parquet(model_fs, process_weather(cleaned_df), out_path)


def _run_process_pv(
    raw_fs: FileSystem, model_fs: FileSystem, pv_system_id: int, date_str: str
) -> None:
    pv_site = get_pv_site_by_system_id(pv_system_id)

    cleaned_pv_prefix = f'{CLEANED_PREFIX}/{TIMESERIES_FOLDER}/{SourceDescriptor.PVOUTPUT}/{pv_system_id}'
    in_pv_path = f'{cleaned_pv_prefix}/pvoutput_{pv_system_id}_{date_str}.parquet'
    if not raw_fs.exists(in_pv_path):
        print(f'    [process_pv] Cleaned PV data not found: {in_pv_path}')
        return

    cleaned_weather_prefix = f'{CLEANED_PREFIX}/{TIMESERIES_FOLDER}/{SourceDescriptor.OPENMETEO_QUARTERHOURLY}'
    raw_extractor = Extractor(raw_fs)
    weather_entry = next(
        (
            e
            for e in raw_extractor.list_files(
                cleaned_weather_prefix, pattern='*.parquet'
            )
            if date_str in e.name
        ),
        None,
    )
    if not weather_entry:
        print(f'    [process_pv] Cleaned weather data not found for date {date_str}')
        return

    print(f'    [process_pv] Joining weather={weather_entry.path} with pv={in_pv_path}')
    processed_df = process_pv(
        weather_df=_read_parquet(raw_fs, weather_entry.path),
        pvoutput_df=_read_parquet(raw_fs, in_pv_path),
        pv_site=pv_site,
    )
    out_path = (
        f'{PROCESSED_PREFIX}/{TIMESERIES_FOLDER}/{SourceDescriptor.PVOUTPUT}/'
        f'{pv_system_id}/processed_pv_{pv_system_id}_{date_str}.parquet'
    )
    _write_parquet(model_fs, processed_df, out_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _main() -> None:
    args = _parse_args()
    config = get_config(DataTransformationConfig, base_config_dirs=[get_config_dir()])

    # --- validate steps ---------------------------------------------------
    steps = [s.strip() for s in args.steps.split(',')]
    invalid = [s for s in steps if s not in STEPS]
    if invalid:
        raise ValueError(
            f'Invalid step(s): {", ".join(invalid)}. Valid: {", ".join(STEPS)}'
        )

    # --- resolve storage backends -----------------------------------------
    if args.local_dir:
        local_config = LocalStorageConfig(prefix=args.local_dir)
        raw_fs = get_filesystem(local_config)
        model_fs = get_filesystem(local_config)
    else:
        raw_fs = get_filesystem(config.staged_raw_data_storage)
        model_fs = get_filesystem(config.staged_model_data_storage)

    # --- initialise in-memory repos ---------------------------------------
    _init_repos(raw_fs)

    # --- resolve PV system IDs --------------------------------------------
    needs_pv_id = any(s in STEPS_NEEDING_PV_ID for s in steps)
    pv_system_ids = (
        _parse_pv_system_ids(args.system_ids)
        if args.system_ids
        else (get_all_pv_system_ids() if needs_pv_id else [])
    )
    if needs_pv_id:
        print(f'Processing {len(pv_system_ids)} PV site(s).\n')

    # --- build work items -------------------------------------------------
    complete_date_range = _get_complete_date_range(args)
    date_strings = [
        dr.start.strftime('%Y%m%d') for dr in complete_date_range.split_by(Period.DAY)
    ]

    work_items: list[tuple[str, str, int | None]] = []
    for date_str in date_strings:
        for step in steps:
            if step in STEPS_NEEDING_PV_ID:
                for pv_id in pv_system_ids:
                    work_items.append((step, date_str, pv_id))
            else:
                work_items.append((step, date_str, None))

    if not work_items:
        print('No tasks to process.')
        return

    print(f'Submitting {len(work_items)} task(s) with {args.workers} worker(s).\n')

    # --- fan-out with ThreadPoolExecutor ----------------------------------
    errors = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {}
        for step, date_str, pv_id in work_items:
            if step == 'clean_weather':
                future = pool.submit(_run_clean_weather, raw_fs, date_str)
            elif step == 'clean_pvoutput':
                if pv_id is None:
                    raise ValueError(f'pv_id required for step {step}')
                future = pool.submit(_run_clean_pvoutput, raw_fs, pv_id, date_str)
            elif step == 'process_weather':
                future = pool.submit(_run_process_weather, raw_fs, model_fs, date_str)
            else:
                if pv_id is None:
                    raise ValueError(f'pv_id required for step {step}')
                future = pool.submit(_run_process_pv, raw_fs, model_fs, pv_id, date_str)
            futures[future] = (step, date_str, pv_id)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                step, date_str, pv_id = futures[future]
                label = f'{step}/{pv_id}/{date_str}' if pv_id else f'{step}/{date_str}'
                print(f'    UNHANDLED ERROR for {label}: {exc}')
                errors += 1

    print(f'\nDone. {len(work_items) - errors} succeeded, {errors} failed.')


if __name__ == '__main__':
    _main()
