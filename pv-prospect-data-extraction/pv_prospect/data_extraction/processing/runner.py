"""Local runner — replaces the Celery-based task_producer for local development.

Orchestrates ``preprocess`` and ``extract_and_load`` using
:class:`concurrent.futures.ThreadPoolExecutor` instead of a Celery queue.

Usage::

    python -m pv_prospect.data_extraction.processing.runner \\
        pv,weather-om-15 \\
        --start-date 2025-06-01 --end-date 2025-06-30 \\
        --local-dir ./out --workers 4
"""

import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from pv_prospect.common import (
    DateRange,
    Period,
    build_location_mapping_repo,
    build_pv_site_repo,
    get_all_pv_system_ids,
    get_config,
    get_pv_site_by_system_id,
)
from pv_prospect.data_extraction import (
    SourceDescriptor,
    get_extractor,
    supports_multi_date,
)
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.processing import ProcessingStats, Result, core
from pv_prospect.data_extraction.resources import get_config_dir as get_de_config_dir
from pv_prospect.data_sources import DataSource
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
        'Defaults to all data sources.'.format(', '.join(s.value for s in DataSource)),
    )
    parser.add_argument(
        'system_ids',
        nargs='?',
        default=None,
        help='system ID or comma-separated list of system IDs (e.g. 123 or 123,456). '
        'If omitted, all systems will be processed.',
    )
    parser.add_argument(
        '-d',
        '--start-date',
        type=str,
        default=None,
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
        '-w', '--by-week', action='store_true', help='process one week at a time'
    )
    parser.add_argument(
        '-l',
        '--local-dir',
        type=str,
        default=None,
        help='local directory instead of GCS',
    )
    parser.add_argument(
        '-o', '--overwrite', action='store_true', help='overwrite existing CSV files'
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


def _init_repos(staging_fs: FileSystem) -> None:
    extractor = Extractor(staging_fs)
    build_pv_site_repo(extractor.read_file(core.PV_SITES_CSV_FILE))
    build_location_mapping_repo(extractor.read_file(core.LOCATION_MAPPING_CSV_FILE))


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
        sources = [DataSource(s.strip()) for s in args.source.split(',')]
        source_descriptors = [config.data_sources.get_descriptor(s) for s in sources]
    else:
        source_descriptors = [config.data_sources.get_descriptor(s) for s in DataSource]

    # --- resolve storage backends -------------------------------------------
    staging_location_config = (
        LocalStorageConfig(prefix=args.local_dir)
        if args.local_dir
        else config.staged_raw_data_storage
    )
    staging_fs = get_filesystem(staging_location_config)
    resources_fs = get_filesystem(config.resources_storage)

    # --- preprocess (sequential, one per source) ----------------------------
    for sd in source_descriptors:
        core.preprocess(staging_fs, sd)

    # --- initialise in-memory repos ----------------------------------------
    _init_repos(resources_fs)

    # --- resolve PV system IDs ----------------------------------------------
    pv_system_ids = (
        _parse_pv_system_ids(args.system_ids)
        if args.system_ids
        else get_all_pv_system_ids()
    )
    print(f'Processing {len(pv_system_ids)} PV site(s).\n')

    # --- build work items ---------------------------------------------------
    try:
        complete_date_range = build_date_range(args.start_date, args.end_date)
    except DegenerateDateRange as e:
        print(str(e))
        sys.exit(1)

    sub_date_ranges = complete_date_range.split_by(
        Period.WEEK if args.by_week else Period.DAY
    )
    if args.reverse:
        sub_date_ranges.reverse()

    work_items: list[tuple[SourceDescriptor, int, DateRange]] = []
    for dr in sub_date_ranges:
        for sd in source_descriptors:
            if args.by_week and not supports_multi_date(sd):
                day_ranges = dr.split_by(Period.DAY)
            else:
                day_ranges = [dr]
            for day_dr in day_ranges:
                for pv_id in pv_system_ids:
                    work_items.append((sd, pv_id, day_dr))

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
                get_pv_site_by_system_id,
                get_extractor,
                sd,
                staging_fs,
                pv_id,
                dr,
                args.overwrite,
                args.dry_run,
            ): (sd, pv_id, dr)
            for sd, pv_id, dr in work_items
        }
        for future in as_completed(futures):
            try:
                result: Result = future.result()
                stats.record(result)
            except Exception as exc:
                sd, pv_id, dr = futures[future]
                print(f'    UNHANDLED ERROR for {sd}/{pv_id}/{dr}: {exc}')

    stats.print_summary()


if __name__ == '__main__':
    _main()
