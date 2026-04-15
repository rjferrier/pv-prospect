"""Local runner — replaces the Celery-based task_producer for local development.

Orchestrates ``preprocess`` and ``extract_and_load`` using
:class:`concurrent.futures.ThreadPoolExecutor` instead of a Celery queue.

Usage::

    python -m pv_prospect.data_extraction.processing.runner \\
        pv,weather-om-15 \\
        --locations 50.49,-3.54 \\
        --start-date 2025-06-01 --end-date 2025-06-30 \\
        --local-dir ./out --workers 4
"""

import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from pv_prospect.common import (
    build_pv_site_repo,
    get_all_pv_system_ids,
    get_config,
    get_pv_site_by_system_id,
)
from pv_prospect.common.domain import (
    AnySite,
    DateRange,
    Period,
)
from pv_prospect.data_extraction import (
    DataSource,
    default_split_period,
    get_extractor,
    supports_multi_date,
)
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.processing import ProcessingStats, Result, core
from pv_prospect.data_extraction.resources import get_config_dir as get_de_config_dir
from pv_prospect.data_sources import (
    PV_SITES_CSV_FILE,
    DataSourceType,
    resolve_location_strings,
    resolve_pv_system_ids,
    resolve_site,
)
from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
from pv_prospect.etl import DegenerateDateRange, Extractor, build_date_range
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import FileSystem, get_filesystem
from pv_prospect.etl.storage.backends import LocalStorageConfig

# ---------------------------------------------------------------------------
# Argument parsing (mirrors task_producer.py)
# ---------------------------------------------------------------------------


def _parse_args() -> 'Any':
    parser = ArgumentParser(
        prog='data-extraction-runner',
        formatter_class=lambda prog: RawTextHelpFormatter(prog, width=120),
    )
    parser.add_argument(
        'source',
        nargs='?',
        default=None,
        help='data source (comma-separated from: {} ). '
        'Defaults to all data sources.'.format(
            ', '.join(s.value for s in DataSourceType)
        ),
    )
    parser.add_argument(
        '-p',
        '--pv-system-ids',
        type=str,
        default=None,
        help="PV system ID or comma-separated list (e.g. 123 or 123,456), or 'all'.",
    )
    parser.add_argument(
        '--locations',
        type=str,
        default=None,
        help='comma-separated lat,lon pairs (e.g. 50.49,-3.54 or 50.49,-3.54,51.50,-0.12). '
        'For weather sources, combined with any grid points derived from --pv-systems.',
    )
    parser.add_argument(
        '-d',
        '--start-date',
        '--date',
        type=str,
        default=None,
        dest='start_date',
        help="start date: 'today', 'yesterday', YYYY-MM-DD, or YYYY-MM format (default: yesterday)",
    )
    parser.add_argument(
        '-e',
        '--end-date',
        type=str,
        default=None,
        help="end date (exclusive): 'today', 'yesterday', YYYY-MM-DD, or YYYY-MM format (default: start date + 1 day)",
    )
    parser.add_argument(
        '-r', '--reverse', action='store_true', help='process dates in reverse order'
    )
    parser.add_argument(
        '-n',
        '--dry-run',
        action='store_true',
        help='show what would be done without writing',
    )
    parser.add_argument(
        '-s',
        '--split-by',
        choices=['day', 'week'],
        default=None,
        dest='split_by',
        help='split date range by day or week (default: day for PV, unsplit for weather)',
    )
    parser.add_argument(
        '-l',
        '--local-dir',
        type=str,
        default=None,
        help='local directory instead of GCS',
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='max parallel threads (default: 4)',
    )
    return parser.parse_args()


def _parse_locations(s: str) -> list[str]:
    locations = resolve_location_strings(location_strings=s)
    return locations


def _init_repos(resources_fs: FileSystem) -> None:
    """Initialize in-memory repositories."""
    resources_extractor = Extractor(resources_fs)
    build_pv_site_repo(resources_extractor.read_file(PV_SITES_CSV_FILE))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _main() -> None:
    args = _parse_args()
    config = get_config(
        DataExtractionConfig,
        base_config_dirs=[
            get_etl_config_dir(),
            get_ds_config_dir(),
            get_de_config_dir(),
        ],
    )

    # --- resolve sources ----------------------------------------------------
    if args.source:
        sources = [DataSourceType(s.strip()) for s in args.source.split(',')]
        data_sources = [config.data_sources.get_data_source(s) for s in sources]
    else:
        data_sources = [config.data_sources.get_data_source(s) for s in DataSourceType]

    # --- resolve storage backends -------------------------------------------
    staging_location_config = (
        LocalStorageConfig(prefix=args.local_dir)
        if args.local_dir
        else config.staged_raw_data_storage
    )
    staging_fs = get_filesystem(staging_location_config)
    resources_fs = get_filesystem(config.resources_storage)

    # --- preprocess (sequential, one per source) ----------------------------
    for sd in data_sources:
        core.preprocess(staging_fs, sd)

    # --- initialise in-memory repos ----------------------------------------
    _init_repos(resources_fs)

    # --- resolve targets ----------------------------------------------------
    pv_system_ids = resolve_pv_system_ids(get_all_pv_system_ids, args.pv_system_ids)

    pv_sites = [
        resolve_site(DataSourceType.PV, get_pv_site_by_system_id, pv_system_id=sid)
        for sid in pv_system_ids
    ]

    # Grid points = those resolved from PV sites + any explicit --location values.
    # Only needed when at least one weather source is requested.
    has_weather_source = any(sd.type == DataSourceType.WEATHER for sd in data_sources)
    arbitrary_sites: list[AnySite] = []

    if has_weather_source and args.locations:
        loc_strs = resolve_location_strings(location_strings=args.locations)
        arbitrary_sites.extend(
            resolve_site(
                DataSourceType.WEATHER, get_pv_site_by_system_id, location_str=loc
            )
            for loc in loc_strs
        )

    if pv_sites:
        print(f'Processing {len(pv_sites)} PV site(s).')
    if arbitrary_sites:
        print(f'Processing {len(arbitrary_sites)} arbitrary site(s).')
    print()

    # --- build work items ---------------------------------------------------
    try:
        complete_date_range = build_date_range(args.start_date, args.end_date)
    except DegenerateDateRange as e:
        print(str(e))
        sys.exit(1)

    explicit_split_period = Period[args.split_by.upper()] if args.split_by else None

    work_items: list[tuple[DataSource, AnySite, DateRange]] = []
    for sd in data_sources:
        split_period = explicit_split_period or default_split_period(sd)
        if split_period:
            sub_date_ranges = complete_date_range.split_by(split_period)
            if split_period == Period.WEEK and not supports_multi_date(sd):
                final_ranges = []
                for dr in sub_date_ranges:
                    final_ranges.extend(dr.split_by(Period.DAY))
            else:
                final_ranges = sub_date_ranges
        else:
            final_ranges = [complete_date_range]
        if args.reverse:
            final_ranges.reverse()
        entities: list[AnySite] = (
            pv_sites + arbitrary_sites
            if sd.type == DataSourceType.WEATHER
            else pv_sites
        )
        for dr in final_ranges:
            for ent in entities:
                work_items.append((sd, ent, dr))

    if not work_items:
        print('No tasks to process.')
        return

    print(f'Submitting {len(work_items)} tasks with {args.workers} workers.\n')

    # --- fan-out with ThreadPoolExecutor ------------------------------------
    stats = ProcessingStats()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(
                core.extract_and_load,
                get_extractor,
                sd,
                staging_fs,
                ent,
                dr,
                args.dry_run,
            ): (sd, ent, dr)
            for sd, ent, dr in work_items
        }
        for future in as_completed(futures):
            try:
                result: Result = future.result()
                stats.record(result)
            except Exception as exc:
                sd, ent, dr = futures[future]
                print(f'    UNHANDLED ERROR for {sd}/{ent}/{dr}: {exc}')

    stats.print_summary()


if __name__ == '__main__':
    _main()
