"""Local runner — replaces the Celery-based task_producer for local development.

Orchestrates ``preprocess`` and ``extract_and_load`` using
:class:`concurrent.futures.ThreadPoolExecutor` instead of a Celery queue.

Usage::

    python -m pv_prospect.data_extraction.processing.runner \\
        pv,weather-om-15 \\
        --start-date 2025-06-01 --end-date 2025-06-30 \\
        --local-dir ./out --workers 4
"""
from argparse import ArgumentParser, RawTextHelpFormatter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

from pv_prospect.common import DateRange, Period
from pv_prospect.common.pv_site_repo import get_all_pv_system_ids, build_pv_site_repo
from pv_prospect.data_extraction.extractors import SourceDescriptor, supports_multi_date
from pv_prospect.etl.factory import get_extractor as get_storage_extractor
from pv_prospect.etl.extractors.protocol import Extractor

from pv_prospect.data_extraction.processing import core
from pv_prospect.data_extraction.processing.processing_stats import ProcessingStats
from pv_prospect.data_extraction.processing.value_objects import Result

SOURCE_DESCRIPTORS = {
    'pv': SourceDescriptor.PVOUTPUT,
    'weather-om-15': SourceDescriptor.OPENMETEO_QUARTERHOURLY,
    'weather-om-60': SourceDescriptor.OPENMETEO_HOURLY,
    'weather-om-satellite': SourceDescriptor.OPENMETEO_SATELLITE,
    'weather-om-historical': SourceDescriptor.OPENMETEO_HISTORICAL,
    'weather-om-15-v0': SourceDescriptor.OPENMETEO_V0_QUARTERHOURLY,
    'weather-om-60-v0': SourceDescriptor.OPENMETEO_V0_HOURLY,
}


# ---------------------------------------------------------------------------
# Argument parsing (mirrors task_producer.py)
# ---------------------------------------------------------------------------

def _parse_args():
    parser = ArgumentParser(
        prog='data-extraction-runner',
        formatter_class=lambda prog: RawTextHelpFormatter(prog, width=120),
    )
    parser.add_argument(
        'source',
        help="data source (comma-separated from: {} )".format(', '.join(SOURCE_DESCRIPTORS.keys())),
    )
    parser.add_argument(
        'system_ids', nargs='?', default=None,
        help="system ID or comma-separated list of system IDs (e.g. 123 or 123,456). "
             "If omitted, all systems will be processed.",
    )
    parser.add_argument(
        '-d', '--start-date', type=str, default=None,
        help="start date: 'today', 'yesterday', YYYY-MM-DD, or YYYY-MM format (default: yesterday)",
    )
    parser.add_argument(
        '-e', '--end-date', type=str, default=None,
        help="end date: 'today', 'yesterday', YYYY-MM-DD, or YYYY-MM format (default: start date + 1 day)",
    )
    parser.add_argument('-r', '--reverse', action='store_true', help="process dates in reverse order")
    parser.add_argument('-n', '--dry-run', action='store_true', help="show what would be done without writing")
    parser.add_argument('-w', '--by-week', action='store_true', help="process one week at a time")
    parser.add_argument('-l', '--local-dir', type=str, default=None, help="local directory instead of GCS")
    parser.add_argument('-o', '--overwrite', action='store_true', help="overwrite existing CSV files")
    parser.add_argument(
        '--workers', type=int, default=4,
        help="max parallel threads (default: 4)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Date helpers (shared with task_producer.py)
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


def _get_complete_date_range(args) -> DateRange:
    yesterday = date.today() - timedelta(days=1)
    if args.start_date is None:
        start, start_is_month = yesterday, False
    else:
        start_is_month = _is_month_format(args.start_date)
        start = _parse_date(args.start_date)

    if args.end_date is None:
        end = _get_last_day_of_month(start) if start_is_month else start + timedelta(days=1)
    else:
        if _is_month_format(args.end_date):
            end = _get_last_day_of_month(_parse_date(args.end_date))
        else:
            end = _parse_date(args.end_date)

    return DateRange(start, end)


def _parse_pv_system_ids(s: str) -> list[int]:
    return [int(x.strip()) for x in s.split(',') if x.strip()]


def _get_all_pv_system_ids(storage_extractor: Extractor) -> list[int]:
    build_pv_site_repo(storage_extractor.read_file(core.PV_SITES_CSV_FILE))
    return get_all_pv_system_ids()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _main() -> None:
    args = _parse_args()

    # --- validate sources ---------------------------------------------------
    sources = [s.strip() for s in args.source.split(',')]
    invalid = [s for s in sources if s not in SOURCE_DESCRIPTORS]
    if invalid:
        raise ValueError(
            f"Invalid source(s): {', '.join(invalid)}. "
            f"Valid: {', '.join(SOURCE_DESCRIPTORS.keys())}"
        )
    source_descriptors = [SOURCE_DESCRIPTORS[s] for s in sources]

    # --- preprocess (sequential, one per source) ----------------------------
    for sd in source_descriptors:
        core.preprocess(sd, args.local_dir)

    # --- resolve PV system IDs ----------------------------------------------
    pv_system_ids = (
        _parse_pv_system_ids(args.system_ids)
        if args.system_ids
        else _get_all_pv_system_ids(get_storage_extractor(args.local_dir))
    )
    print(f"Processing {len(pv_system_ids)} PV site(s).\n")

    # --- build work items ---------------------------------------------------
    complete_date_range = _get_complete_date_range(args)
    sub_date_ranges = complete_date_range.split_by(Period.WEEK if args.by_week else Period.DAY)
    if args.reverse:
        sub_date_ranges.reverse()

    work_items: list[tuple[SourceDescriptor, int, DateRange]] = []
    for dr in sub_date_ranges:
        for source in sources:
            sd = SOURCE_DESCRIPTORS[source]
            if args.by_week and not supports_multi_date(sd):
                day_ranges = dr.split_by(Period.DAY)
            else:
                day_ranges = [dr]
            for day_dr in day_ranges:
                for pv_id in pv_system_ids:
                    work_items.append((sd, pv_id, day_dr))

    if not work_items:
        print("No tasks to process.")
        return

    print(f"Submitting {len(work_items)} tasks with {args.workers} workers.\n")

    # --- fan-out with ThreadPoolExecutor ------------------------------------
    stats = ProcessingStats()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(
                core.extract_and_load,
                sd, pv_id, dr,
                args.local_dir,
                args.overwrite, args.dry_run,
            ): (sd, pv_id, dr)
            for sd, pv_id, dr in work_items
        }
        for future in as_completed(futures):
            try:
                result: Result = future.result()
                stats.record(result)
            except Exception as exc:
                sd, pv_id, dr = futures[future]
                print(f"    UNHANDLED ERROR for {sd}/{pv_id}/{dr}: {exc}")

    stats.print_summary()


if __name__ == '__main__':
    _main()
