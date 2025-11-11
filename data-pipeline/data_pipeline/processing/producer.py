from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import date, timedelta

from config import EtlConfig
from domain import DateRange
from domain.date_range import Period
from extractors import SourceDescriptor, supports_multi_date
from loaders import get_storage_client
from loaders.factory import StorageClient
from processing.pv_site_repo import get_all_pv_system_ids, build_pv_site_repo
from processing.task_queuer import TaskQueuer

SOURCE_DESCRIPTORS = {
    'pv': SourceDescriptor.PVOUTPUT,
    'weather-om-15': SourceDescriptor.OPENMETEO_QUARTERHOURLY,
    'weather-om-60': SourceDescriptor.OPENMETEO_HOURLY,
    'weather-om-satellite': SourceDescriptor.OPENMETEO_SATELLITE,
    'weather-om-historical': SourceDescriptor.OPENMETEO_HISTORICAL,
    'weather-om-15-v0': SourceDescriptor.OPENMETEO_V0_QUARTERHOURLY,
    'weather-om-60-v0': SourceDescriptor.OPENMETEO_V0_HOURLY,
    'weather-vc-15': SourceDescriptor.VISUALCROSSING_QUARTERHOURLY,
    'weather-vc-60': SourceDescriptor.VISUALCROSSING_HOURLY,
}


def _parse_args():
    parser = ArgumentParser(
        prog='etl', formatter_class=lambda prog: RawTextHelpFormatter(prog, width=120)
    )
    parser.format_help()
    parser.add_argument(
        'source',
        help="data source (comma-separated from: {} )".format(', '.join(SOURCE_DESCRIPTORS.keys()))
    )
    parser.add_argument('system_ids',
                        nargs='?',
                        default=None,
                        help="system ID or comma-separated list of system IDs (e.g. 123 or 123,456). If omitted, all systems will be processed.")
    parser.add_argument(
        '-d', '--start-date',
        type=str,
        help="start date: 'today', 'yesterday', YYYY-MM-DD, or YYYY-MM format (default: yesterday)",
        default=None
    )
    parser.add_argument(
        '-e', '--end-date',
        type=str,
        help="end date: 'today', 'yesterday', YYYY-MM-DD, or YYYY-MM format (default: start date plus one day)"
    )
    parser.add_argument(
        '-r', '--reverse',
        action="store_true",
        help="process dates in reverse order"
    )
    parser.add_argument(
        '-n', '--dry-run',
        action='store_true',
        help='Show what would be done, but do not upload or modify any files.'
    )
    parser.add_argument(
        '-w', '--by-week',
        action='store_true',
        help="Process one week at a time instead of one day at a time."
    )
    parser.add_argument(
        '-x', '--write-metadata',
        action='store_true',
        help='Write extractor metadata as a JSON file next to the CSV when present.'
    )
    parser.add_argument(
        '-l', '--local-dir',
        type=str,
        default=None,
        help='Write files to a local directory instead of uploading to Google Drive. Specify the directory path.'
    )
    parser.add_argument(
        '-o', '--overwrite',
        action='store_true',
        help='Overwrite existing CSV files. By default, existing files are skipped.'
    )
    return parser.parse_args()


def _parse_date(date_str: str) -> date:
    """Parse date string supporting 'today', 'yesterday', YYYY-MM-DD, or YYYY-MM format."""
    if date_str.lower() == 'today':
        return date.today()
    elif date_str.lower() == 'yesterday':
        return date.today() - timedelta(days=1)

    # Try YYYY-MM format (month specification)
    if len(date_str) == 7 and date_str[4] == '-':
        try:
            year, month = date_str.split('-')
            year = int(year)
            month = int(month)
            # Return the first day of the month
            return date(year, month, 1)
        except (ValueError, IndexError):
            pass

    # Default to ISO format (YYYY-MM-DD)
    return date.fromisoformat(date_str)


def _is_month_format(date_str: str) -> bool:
    """Check if a date string is in YYYY-MM format."""
    return len(date_str) == 7 and date_str[4] == '-' and date_str.count('-') == 1


def _get_last_day_of_month(year_month_date: date) -> date:
    """Given a date in YYYY-MM format, return the last day of that month."""
    next_month = year_month_date.month % 12 + 1
    year = year_month_date.year + (year_month_date.month // 12)
    first_of_next_month = date(year, next_month, 1)
    last_day_of_month = first_of_next_month - timedelta(days=1)
    return last_day_of_month


def _get_complete_date_range(args) -> DateRange:
    """
    Parse and convert the command-line date arguments into a DateRange.

    Args:
        args: Parsed command-line arguments with start_date and end_date as strings

    Returns:
        DateRange with parsed start and end dates
    """
    yesterday = date.today() - timedelta(days=1)

    # Parse start date
    if args.start_date is None:
        start = yesterday
        start_is_month = False
    else:
        start_is_month = _is_month_format(args.start_date)
        start = _parse_date(args.start_date)

    # Parse end date
    if args.end_date is None:
        if start_is_month:
            # If start date was a month, default end date to end of that month
            end = _get_last_day_of_month(start)
        else:
            end = start + timedelta(days=1)
    else:
        if _is_month_format(args.end_date):
            # Parse as first day of month, then convert to last day
            temp_date = _parse_date(args.end_date)
            end = _get_last_day_of_month(temp_date)
        else:
            end = _parse_date(args.end_date)

    return DateRange(start, end)


def _parse_pv_system_ids(system_ids_str: str):
    return [int(s.strip()) for s in system_ids_str.split(',') if s.strip()]


def _get_all_pv_system_ids(storage_client: StorageClient) -> list[int]:
    build_pv_site_repo(storage_client)
    return get_all_pv_system_ids()


def _main(config, args):
    task_queuer = TaskQueuer.from_config(config)

    # Parse comma-separated sources
    sources = [s.strip() for s in args.source.split(',')]
    # Validate sources
    source_descriptor_keys = SOURCE_DESCRIPTORS.keys()
    invalid = [s for s in sources if s not in source_descriptor_keys]
    if invalid:
        raise ValueError(f"Invalid source(s): {', '.join(invalid)}. Valid options: {', '.join(source_descriptor_keys)}")

    source_descriptors = [SOURCE_DESCRIPTORS[source] for source in sources]
    task_queuer.create_folders(
        source_descriptors, args.local_dir, args.write_metadata
    ).wait_for_completion()

    pv_system_ids = (
        _parse_pv_system_ids(args.system_ids) if args.system_ids else
        _get_all_pv_system_ids(get_storage_client(args.local_dir))
    )

    print(f"Processing {len(pv_system_ids)} PV site(s).\n")

    complete_date_range = _get_complete_date_range(args)

    # Split into date ranges first (by week or by day)
    sub_date_ranges = complete_date_range.split_by(Period.WEEK if args.by_week else Period.DAY)
    if args.reverse:
        sub_date_ranges.reverse()

    # Loop over dates first, then sources
    for date_range in sub_date_ranges:
        print(f"Processing {date_range}")

        for source in sources:
            source_descriptor = SOURCE_DESCRIPTORS[source]
            print(f"  Processing {source_descriptor}")

            # Determine which date ranges to use for this source
            if args.by_week and not supports_multi_date(source_descriptor):
                # Extractor doesn't support multi-date, decompose week into single days
                daily_ranges = date_range.split_by(Period.DAY)
                print(f"  Decomposing week into {len(daily_ranges)} days")
                date_ranges_to_process = daily_ranges
            else:
                # Use the date range as-is
                date_ranges_to_process = [date_range]

            counter = 0

            for dr in date_ranges_to_process:
                for pv_system_id in pv_system_ids:
                    print(f"    Adding {source_descriptor} for System {pv_system_id}, {dr}")

                    task_queuer.extract_and_load(
                        source_descriptor,
                        pv_system_id,
                        dr,
                        args.local_dir,
                        args.write_metadata,
                        args.overwrite,
                        args.dry_run,
                        counter
                    )
                    counter += 1

            if counter == 0:
                print("No tasks/results were generated.")
                return


if __name__ == '__main__':
    config_ = EtlConfig.from_yaml()
    args_ = _parse_args()
    _main(config_, args_)
