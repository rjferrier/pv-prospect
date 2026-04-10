"""Local runner — replaces the Cloud Run Job entrypoint for local development.

Orchestrates transformation steps using :class:`concurrent.futures.ThreadPoolExecutor`
instead of Cloud Run Jobs.

Usage::

    python -m pv_prospect.data_transformation.runner \
        clean_weather,prepare_weather \
        --locations 50.49,-3.54 \
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
from typing import Any, Callable

from pv_prospect.common import (
    build_location_mapping_repo,
    build_pv_site_repo,
    get_all_pv_system_ids,
    get_config,
    get_location_by_pv_system_id,
    get_pv_site_by_system_id,
)
from pv_prospect.common.domain import (
    DateRange,
    GridPoint,
    Location,
    PVSite,
)
from pv_prospect.data_sources import (
    LOCATION_MAPPING_CSV_FILE,
    PV_SITES_CSV_FILE,
    DataSource,
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
from pv_prospect.data_transformation.transformation import (
    ALL_TRANSFORMATIONS,
    CLEANING_TRANSFORMATIONS,
    PREPARING_TRANSFORMATIONS,
    TRANSFORMATIONS_NEEDING_GRID_POINT,
    TRANSFORMATIONS_NEEDING_PV_SITE,
    Transformation,
)
from pv_prospect.etl import DegenerateDateRange, Extractor, build_date_range
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import FileSystem, get_filesystem
from pv_prospect.etl.storage.backends import LocalStorageConfig

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
        nargs='?',
        default=None,
        help='transformation step(s), comma-separated from: {} . '
        'Defaults to all steps.'.format(
            ', '.join(t.value for t in ALL_TRANSFORMATIONS)
        ),
    )
    parser.add_argument(
        '-p',
        '--pv-system-ids',
        type=str,
        default=None,
        help="PV system ID or comma-separated list (e.g. 123 or 123,456), or 'all'. "
        'Required for steps: {}.'.format(
            ', '.join(sorted(t.value for t in TRANSFORMATIONS_NEEDING_PV_SITE))
        ),
    )
    parser.add_argument(
        '--locations',
        type=str,
        default=None,
        help='comma-separated lat,lon pairs (e.g. 50.49,-3.54 or 50.49,-3.54,51.50,-0.12). '
        'Required for weather steps when no pv-system-ids are given.',
    )
    parser.add_argument(
        '-d',
        '--start-date',
        '--date',
        type=str,
        default=None,
        dest='start_date',
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
        '-s',
        '--split-by',
        choices=['day', 'week'],
        default=None,
        dest='split_by',
        help='split date range by day or week; when week, raw weather files span a full week '
        '(read weekly, write per-day cleaned files)',
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


def _parse_locations(s: str) -> list[GridPoint]:
    parts = [x.strip() for x in s.split(',') if x.strip()]
    if len(parts) % 2 != 0:
        raise ValueError(
            f'Expected pairs of lat,lon values but got {len(parts)} values.'
        )
    return [
        GridPoint(Location.from_coordinates(parts[i], parts[i + 1]))
        for i in range(0, len(parts), 2)
    ]


# ---------------------------------------------------------------------------
# Step dispatch
# ---------------------------------------------------------------------------


def _make_step_fn(
    step: Transformation,
    raw_fs: FileSystem,
    cleaned_fs: FileSystem,
    batches_fs: FileSystem,
    pv_data_source: DataSource,
    weather_data_source: DataSource,
    date_range: DateRange,
    split_by: str | None,
) -> Callable[[GridPoint, PVSite | None], None]:
    """Return a callable that runs *step* for a single entity."""
    if step == Transformation.CLEAN_WEATHER:

        def fn_clean_weather(grid_point: GridPoint, _: PVSite | None) -> None:
            run_clean_weather(
                raw_fs,
                cleaned_fs,
                weather_data_source,
                grid_point,
                date_range,
                split_by == 'week',
            )

        return fn_clean_weather  # type: ignore[return-value]

    if step == Transformation.PREPARE_WEATHER:

        def fn_prepare_weather(grid_point: GridPoint, _: PVSite | None) -> None:
            run_prepare_weather(
                cleaned_fs, batches_fs, weather_data_source, grid_point, date_range
            )

        return fn_prepare_weather  # type: ignore[return-value]

    if step == Transformation.CLEAN_PV:

        def fn_clean_pv(_: GridPoint, pv_site: PVSite | None) -> None:
            if pv_site is None:
                raise ValueError('pv_site must be set for clean_pv')
            run_clean_pv(raw_fs, cleaned_fs, pv_data_source, pv_site, date_range)

        return fn_clean_pv  # type: ignore[return-value]

    if step == Transformation.PREPARE_PV:

        def fn_prepare_pv(grid_point: GridPoint, pv_site: PVSite | None) -> None:
            if pv_site is None:
                raise ValueError('pv_site must be set for prepare_pv')
            run_prepare_pv(
                cleaned_fs,
                batches_fs,
                pv_data_source,
                weather_data_source,
                pv_site,
                grid_point,
                date_range,
                get_pv_site_by_system_id,
            )

        return fn_prepare_pv  # type: ignore[return-value]

    raise ValueError(f'Unknown step: {step}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _build_work_items(
    steps: list[Transformation],
    grid_points: list[GridPoint],
    pv_sites: list[PVSite],
) -> list[tuple[Transformation, GridPoint, PVSite | None]]:
    work_items: list[tuple[Transformation, GridPoint, PVSite | None]] = []
    for step in steps:
        if step in TRANSFORMATIONS_NEEDING_PV_SITE:
            for pv_site in pv_sites:
                grid_point = GridPoint(get_location_by_pv_system_id(pv_site.pvo_sys_id))
                work_items.append((step, grid_point, pv_site))
        elif step in TRANSFORMATIONS_NEEDING_GRID_POINT:
            for grid_point in grid_points:
                work_items.append((step, grid_point, None))
    return work_items


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
    if args.steps:
        all_transformations = [Transformation(s.strip()) for s in args.steps.split(',')]
        invalid = [s for s in all_transformations if s not in ALL_TRANSFORMATIONS]
        if invalid:
            raise ValueError(
                f'Invalid step(s): {", ".join(str(s) for s in invalid)}. '
                f'Valid: {", ".join(t.value for t in ALL_TRANSFORMATIONS)}'
            )
    else:
        all_transformations = list(ALL_TRANSFORMATIONS)

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

    # --- initialise in-memory repos ---------------------------------------
    resources_fs = get_filesystem(config.resources_storage)
    resources_extractor = Extractor(resources_fs)
    build_pv_site_repo(resources_extractor.read_file(PV_SITES_CSV_FILE))
    build_location_mapping_repo(
        resources_extractor.read_file(LOCATION_MAPPING_CSV_FILE)
    )

    # --- resolve date range -----------------------------------------------
    try:
        date_range = build_date_range(args.start_date, args.end_date)
    except DegenerateDateRange as e:
        print(str(e))
        sys.exit(1)

    # --- resolve PV system IDs and weather grid points --------------------
    needs_pv_id = any(s in TRANSFORMATIONS_NEEDING_PV_SITE for s in all_transformations)
    needs_grid_point = any(
        s in TRANSFORMATIONS_NEEDING_GRID_POINT for s in all_transformations
    )

    pv_system_ids = (
        get_all_pv_system_ids()
        if args.pv_system_ids == 'all'
        else _parse_pv_system_ids(args.pv_system_ids)
        if args.pv_system_ids
        else []
    )
    pv_sites = [get_pv_site_by_system_id(sid) for sid in pv_system_ids]
    grid_points = _parse_locations(args.locations) if args.locations else []

    if needs_pv_id:
        print(f'Processing {len(pv_system_ids)} PV site(s).')
    if needs_grid_point:
        print(f'Processing {len(grid_points)} weather location(s).')
    print()

    def run(transformations_filter: frozenset[Transformation]) -> None:
        transformations = [
            t for t in all_transformations if t in transformations_filter
        ]

        work_items = _build_work_items(transformations, grid_points, pv_sites)

        if not work_items:
            return

        print(f'Submitting {len(work_items)} task(s) with {args.workers} worker(s).\n')

        errors = 0
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {}
            for step, grid_point, pv_site in work_items:
                step_fn = _make_step_fn(
                    step,
                    raw_fs,
                    cleaned_fs,
                    batches_fs,
                    config.data_sources.pv,
                    config.data_sources.weather,
                    date_range,
                    args.split_by,
                )
                futures[pool.submit(step_fn, grid_point, pv_site)] = (
                    step,
                    grid_point,
                    pv_site,
                )

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    step, grid_point, pv_site = futures[future]
                    print(
                        f'    UNHANDLED ERROR for {step}/{grid_point or pv_site}: {exc}'
                    )
                    errors += 1

            print(
                f'\nDone with {[t.value for t in transformations]}. '
                f'{len(work_items) - errors} succeeded, {errors} failed.'
            )

    run(CLEANING_TRANSFORMATIONS)
    run(PREPARING_TRANSFORMATIONS)

    if Transformation.ASSEMBLE_WEATHER in all_transformations:
        print('\nAssembling prepared weather data...')
        assemble_prepared_weather(batches_fs, prepared_fs)

    if Transformation.ASSEMBLE_PV in all_transformations:
        print('\nAssembling prepared PV data...')
        for pv_id in pv_system_ids:
            assemble_prepared_pv(batches_fs, prepared_fs, pv_id)

    print('\nDone.')


if __name__ == '__main__':
    _main()
