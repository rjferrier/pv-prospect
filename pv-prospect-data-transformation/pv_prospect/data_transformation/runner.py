"""Local runner — replaces the Cloud Run Job entrypoint for local development.

Orchestrates transformation steps using :class:`concurrent.futures.ThreadPoolExecutor`
instead of Cloud Run Jobs.

Usage::

    python -m pv_prospect.data_transformation.runner \
        clean_weather,prepare_weather \
        --start-date 2025-06-01 --end-date 2025-06-30 \
        --local-dir ./out --workers 4
"""

from argparse import ArgumentParser, RawTextHelpFormatter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any

from pv_prospect.common import (
    DateRange,
    Period,
    build_location_mapping_repo,
    build_pv_site_repo,
    get_all_pv_system_ids,
    get_config,
)
from pv_prospect.data_sources import (
    LOCATION_MAPPING_CSV_FILE,
    PV_SITES_CSV_FILE,
)
from pv_prospect.data_sources import (
    get_config_dir as get_ds_config_dir,
)
from pv_prospect.data_transformation.config import DataTransformationConfig
from pv_prospect.data_transformation.core import (
    run_clean_pv,
    run_clean_weather,
    run_prepare_pv,
    run_prepare_weather,
)
from pv_prospect.etl import Extractor
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import get_filesystem
from pv_prospect.etl.storage.backends import LocalStorageConfig

STEPS = ['clean_weather', 'clean_pv', 'prepare_weather', 'prepare_pv']
STEPS_NEEDING_PV_ID: frozenset[str] = frozenset({'clean_pv', 'prepare_pv'})


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
# Main
# ---------------------------------------------------------------------------

_STEP_DISPATCH = {
    'clean_weather': lambda ctx, date_str, _pv_id: run_clean_weather(
        ctx['raw_fs'], ctx['intermediate_fs'], ctx['weather'], date_str
    ),
    'clean_pv': lambda ctx, date_str, pv_id: run_clean_pv(
        ctx['raw_fs'], ctx['intermediate_fs'], ctx['pv'], pv_id, date_str
    ),
    'prepare_weather': lambda ctx, date_str, _pv_id: run_prepare_weather(
        ctx['intermediate_fs'], ctx['model_fs'], ctx['weather'], date_str
    ),
    'prepare_pv': lambda ctx, date_str, pv_id: run_prepare_pv(
        ctx['intermediate_fs'],
        ctx['model_fs'],
        ctx['pv'],
        ctx['weather'],
        pv_id,
        date_str,
    ),
}


def _main() -> None:
    args = _parse_args()
    config = get_config(
        DataTransformationConfig,
        base_config_dirs=[get_etl_config_dir(), get_ds_config_dir()],
    )

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

    intermediate_fs = get_filesystem(config.intermediate_data_storage)

    ctx = {
        'raw_fs': raw_fs,
        'intermediate_fs': intermediate_fs,
        'model_fs': model_fs,
        'pv': config.data_sources.pv,
        'weather': config.data_sources.weather,
    }

    # --- initialise in-memory repos ---------------------------------------
    extractor = Extractor(raw_fs)
    build_pv_site_repo(extractor.read_file(PV_SITES_CSV_FILE))
    build_location_mapping_repo(extractor.read_file(LOCATION_MAPPING_CSV_FILE))

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
        futures = {
            pool.submit(_STEP_DISPATCH[step], ctx, date_str, pv_id): (
                step,
                date_str,
                pv_id,
            )
            for step, date_str, pv_id in work_items
        }

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
