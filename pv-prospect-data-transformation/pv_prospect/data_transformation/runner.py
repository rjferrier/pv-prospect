"""Local runner — replaces the Cloud Run Job entrypoint for local development.

Orchestrates transformation steps using :class:`concurrent.futures.ThreadPoolExecutor`
instead of Cloud Run Jobs.

Usage::

    python -m pv_prospect.data_transformation.runner \
        clean_weather,prepare_weather \
        --location 504900_-35400 \
        --start-date 2025-06-01 --end-date 2025-06-30 \
        --local-dir ./out --workers 4

    python -m pv_prospect.data_transformation.runner \
        clean_pv,prepare_pv \
        89665,12345 \
        --start-date 2025-06-01 --end-date 2025-06-30
"""

import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from pv_prospect.common import (
    Period,
    build_location_mapping_repo,
    build_pv_site_repo,
    get_all_pv_system_ids,
    get_config,
    get_location_by_pv_system_id,
)
from pv_prospect.data_sources import (
    LOCATION_MAPPING_CSV_FILE,
    PV_SITES_CSV_FILE,
    OpenMeteoTimeSeriesDescriptor,
    PVOutputTimeSeriesDescriptor,
)
from pv_prospect.data_sources import (
    get_config_dir as get_ds_config_dir,
)
from pv_prospect.data_transformation.config import DataTransformationConfig
from pv_prospect.data_transformation.core import (
    assemble_prepared_pv,
    assemble_prepared_weather,
    run_clean_pv,
    run_clean_weather,
    run_prepare_pv,
    run_prepare_weather,
)
from pv_prospect.data_transformation.resources import (
    get_config_dir as get_dt_config_dir,
)
from pv_prospect.etl import DegenerateDateRange, Extractor, build_date_range
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import get_filesystem
from pv_prospect.etl.storage.backends import LocalStorageConfig

STEPS = ['clean_weather', 'clean_pv', 'prepare_weather', 'prepare_pv']
STEPS_NEEDING_PV_ID: frozenset[str] = frozenset({'clean_pv', 'prepare_pv'})
STEPS_NEEDING_LOCATION: frozenset[str] = frozenset({'clean_weather', 'prepare_weather'})


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
        '--location',
        type=str,
        default=None,
        help='comma-separated lat_lon values (e.g. 504900_-35400). '
        'Required for weather steps when no system_ids are given.',
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
        help="end date (exclusive): 'today', 'yesterday', YYYY-MM-DD, or YYYY-MM (default: start date + 1 day)",
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


def _parse_pv_system_ids(s: str) -> list[int]:
    return [int(x.strip()) for x in s.split(',') if x.strip()]


def _parse_locations(s: str) -> list[OpenMeteoTimeSeriesDescriptor]:
    return [
        OpenMeteoTimeSeriesDescriptor.from_str(x.strip())
        for x in s.split(',')
        if x.strip()
    ]


def _get_weather_location(pv_system_id: int) -> OpenMeteoTimeSeriesDescriptor:
    loc = get_location_by_pv_system_id(pv_system_id)
    return OpenMeteoTimeSeriesDescriptor.from_coordinates(loc.latitude, loc.longitude)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

_PV_STEP_DISPATCH = {
    'clean_pv': lambda ctx, date_str, pv_ts: run_clean_pv(
        ctx['raw_fs'], ctx['cleaned_fs'], ctx['pv'], pv_ts, date_str
    ),
    'prepare_pv': lambda ctx, date_str, pv_ts: run_prepare_pv(
        ctx['cleaned_fs'],
        ctx['batches_fs'],
        ctx['pv'],
        ctx['weather'],
        pv_ts,
        _get_weather_location(pv_ts.pv_system_id),
        date_str,
    ),
}

_WEATHER_STEP_DISPATCH = {
    'clean_weather': lambda ctx, date_str, loc: run_clean_weather(
        ctx['raw_fs'], ctx['cleaned_fs'], ctx['weather'], loc, date_str
    ),
    'prepare_weather': lambda ctx, date_str, loc: run_prepare_weather(
        ctx['cleaned_fs'], ctx['batches_fs'], ctx['weather'], loc, date_str
    ),
}


def _main() -> None:
    args = _parse_args()
    config = get_config(
        DataTransformationConfig,
        base_config_dirs=[
            get_etl_config_dir(),
            get_ds_config_dir(),
            get_dt_config_dir(),
        ],
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
        batches_fs = get_filesystem(local_config)
        prepared_fs = get_filesystem(local_config)
    else:
        raw_fs = get_filesystem(config.staged_raw_data_storage)
        batches_fs = get_filesystem(config.staged_prepared_batches_data_storage)
        prepared_fs = get_filesystem(config.staged_prepared_data_storage)

    cleaned_fs = get_filesystem(config.staged_cleaned_data_storage)

    ctx = {
        'raw_fs': raw_fs,
        'cleaned_fs': cleaned_fs,
        'batches_fs': batches_fs,
        'prepared_fs': prepared_fs,
        'pv': config.data_sources.pv,
        'weather': config.data_sources.weather,
    }

    # --- initialise in-memory repos ---------------------------------------
    resources_fs = get_filesystem(config.resources_storage)
    resources_extractor = Extractor(resources_fs)
    build_pv_site_repo(resources_extractor.read_file(PV_SITES_CSV_FILE))
    build_location_mapping_repo(
        resources_extractor.read_file(LOCATION_MAPPING_CSV_FILE)
    )

    # --- resolve PV system IDs and weather locations ----------------------
    needs_pv_id = any(s in STEPS_NEEDING_PV_ID for s in steps)
    needs_location = any(s in STEPS_NEEDING_LOCATION for s in steps)

    pv_system_ids = (
        _parse_pv_system_ids(args.system_ids)
        if args.system_ids
        else (get_all_pv_system_ids() if needs_pv_id else [])
    )

    if args.location:
        locations = _parse_locations(args.location)
    elif needs_location:
        all_ids = pv_system_ids or get_all_pv_system_ids()
        locations = list({_get_weather_location(sid) for sid in all_ids})
    else:
        locations = []

    if needs_pv_id:
        print(f'Processing {len(pv_system_ids)} PV site(s).')
    if needs_location:
        print(f'Processing {len(locations)} weather location(s).')
    print()

    # --- build work items -------------------------------------------------
    try:
        complete_date_range = build_date_range(args.start_date, args.end_date)
    except DegenerateDateRange as e:
        print(str(e))
        sys.exit(1)

    date_strings = [
        dr.start.strftime('%Y%m%d') for dr in complete_date_range.split_by(Period.DAY)
    ]

    WorkItem = tuple[
        str, str, PVOutputTimeSeriesDescriptor | OpenMeteoTimeSeriesDescriptor
    ]
    work_items: list[WorkItem] = []
    for date_str in date_strings:
        for step in steps:
            if step in STEPS_NEEDING_PV_ID:
                for pv_id in pv_system_ids:
                    work_items.append(
                        (step, date_str, PVOutputTimeSeriesDescriptor(pv_id))
                    )
            elif step in STEPS_NEEDING_LOCATION:
                for loc in locations:
                    work_items.append((step, date_str, loc))

    if not work_items:
        print('No tasks to process.')
        return

    print(f'Submitting {len(work_items)} task(s) with {args.workers} worker(s).\n')

    # --- fan-out with ThreadPoolExecutor ----------------------------------
    dispatch = {**_PV_STEP_DISPATCH, **_WEATHER_STEP_DISPATCH}
    errors = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(dispatch[step], ctx, date_str, desc): (
                step,
                date_str,
                desc,
            )
            for step, date_str, desc in work_items
        }

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                step, date_str, desc = futures[future]
                label = f'{step}/{desc}/{date_str}'
                print(f'    UNHANDLED ERROR for {label}: {exc}')
                errors += 1

    if 'prepare_weather' in steps:
        print('\nAssembling prepared weather data...')
        assemble_prepared_weather(batches_fs, prepared_fs)

    if 'prepare_pv' in steps:
        print('\nAssembling prepared PV data...')
        for pv_id in pv_system_ids:
            assemble_prepared_pv(batches_fs, prepared_fs, pv_id)

    print(f'\nDone. {len(work_items) - errors} succeeded, {errors} failed.')


if __name__ == '__main__':
    _main()
