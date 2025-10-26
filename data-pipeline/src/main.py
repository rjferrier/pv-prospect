from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import date, timedelta

from domain.data_source import DataSource
from domain.pv_site import PVSiteRepository, PVSite
from extractors.openmeteo import (
    OpenMeteoWeatherDataExtractor, Mode as OMMode, APISelector, TimeResolution, Fields, Models
)
from extractors.pvoutput import PVOutputExtractor
from extractors.visualcrossing import VCWeatherDataExtractor, Mode as VCMode
from loaders.gdrive import GDriveClient
from loaders.local import LocalStorageClient
from processing import ProcessingStats

ALLOW_DUPLICATE_FILES = False

DATA_SOURCES = {
    'pv': DataSource(
        descriptor='pvoutput',
        extractor_factory=lambda: PVOutputExtractor.from_env()
    ),
    'weather-om-15': DataSource(
        descriptor='openmeteo/quarterhourly',
        extractor_factory=lambda: OpenMeteoWeatherDataExtractor.from_components(
            api_selector=APISelector.FORECAST,
            time_resolution=TimeResolution.QUARTERHOURLY,
            fields=Fields.FORECAST,
            models=Models.ALL_FORECAST,
        )
    ),
    'weather-om-60': DataSource(
        descriptor='openmeteo/hourly',
        extractor_factory=lambda: OpenMeteoWeatherDataExtractor.from_components(
            api_selector=APISelector.FORECAST,
            time_resolution=TimeResolution.HOURLY,
            fields=Fields.FORECAST,
            models=Models.ALL_FORECAST,
        )
    ),
    'weather-om-satellite': DataSource(
        descriptor='openmeteo/satellite',
        extractor_factory=lambda: OpenMeteoWeatherDataExtractor.from_components(
            api_selector=APISelector.SATELLITE,
            time_resolution=TimeResolution.HOURLY,
            fields=Fields.SOLAR_RADIATION,
            models=Models.ALL_SATELLITE,
        )
    ),
    'weather-om-historical': DataSource(
        descriptor='openmeteo/historical',
        extractor_factory=lambda: OpenMeteoWeatherDataExtractor.from_components(
            api_selector=APISelector.HISTORICAL,
            time_resolution=TimeResolution.HOURLY,
            fields=Fields.FORECAST,
            models=Models.ALL_FORECAST,
        )
    ),
    'weather-om-15-v0': DataSource(
        descriptor='openmeteo/v0/quarterhourly',
        extractor_factory=lambda: OpenMeteoWeatherDataExtractor.from_mode(OMMode.QUARTERHOURLY)
    ),
    'weather-om-60-v0': DataSource(
        descriptor='openmeteo/v0/hourly',
        extractor_factory=lambda: OpenMeteoWeatherDataExtractor.from_mode(OMMode.HOURLY)
    ),
    'weather-vc-15': DataSource(
        descriptor='visualcrossing/quarterhourly',
        extractor_factory=lambda: VCWeatherDataExtractor.from_env(mode=VCMode.QUARTERHOURLY),
    ),
    'weather-vc-60': DataSource(
        descriptor='visualcrossing/hourly',
        extractor_factory=lambda: VCWeatherDataExtractor.from_env(mode=VCMode.HOURLY),
    ),
}


def parse_args():
    yesterday = date.today() - timedelta(days=1)

    parser = ArgumentParser(
        prog='etl', formatter_class=lambda prog: RawTextHelpFormatter(prog, width=120)
    )
    parser.format_help()
    parser.add_argument(
        'source',
        help="data source (comma-separated from: {} )".format(', '.join(DATA_SOURCES.keys()))
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
    args = parser.parse_args()

    # Parse start date
    if args.start_date is None:
        args.start_date = yesterday
        start_is_month = False
    else:
        start_is_month = _is_month_format(args.start_date)
        args.start_date = _parse_date(args.start_date)

    # Parse end date
    if args.end_date is None:
        if start_is_month:
            # If start date was a month, default end date to end of that month
            args.end_date = _get_last_day_of_month(args.start_date)
        else:
            args.end_date = args.start_date + timedelta(days=1)
    else:
        if _is_month_format(args.end_date):
            # Parse as first day of month, then convert to last day
            temp_date = _parse_date(args.end_date)
            args.end_date = _get_last_day_of_month(temp_date)
        else:
            args.end_date = _parse_date(args.end_date)

    return args


def main(args):
    # Parse comma-separated sources
    source_args = [s.strip() for s in args.source.split(',')]
    # Validate sources
    invalid = [s for s in source_args if s not in DATA_SOURCES]
    if invalid:
        raise ValueError(f"Invalid source(s): {', '.join(invalid)}. Valid options: {', '.join(DATA_SOURCES.keys())}")
    sources = source_args

    # Get the appropriate PV sites based on whether system IDs were specified
    pv_sites = _load_pv_sites(args.system_ids)
    print(f"Successfully loaded {len(pv_sites)} PV site(s).\n")

    # Track processing results
    stats = ProcessingStats()

    # Initialize client based on local-dir option
    if args.local_dir:
        print(f"Using local directory: {args.local_dir}\n")
        client = LocalStorageClient(args.local_dir)
    else:
        client = GDriveClient.build_service()

    for source in sources:
        data_source = DATA_SOURCES[source]
        extractor = data_source.extractor_factory()

        # Determine the iteration strategy based on period and extractor type
        if args.by_week:
            if not extractor.multi_date:
                raise ValueError(f"Extractor for source '{source}' does not support multi-date extraction "
                                 f"required for --by-week option.")

            # Process week by week
            for week_start, week_end in _get_week_ranges(args):
                print(f"Processing {data_source.descriptor} for week {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}")

                for pv_site in pv_sites:
                    print(f"  Processing site: {pv_site.name} (system_id={pv_site.pvo_sys_id})")
                    _process_extraction(
                        client, extractor, data_source, pv_site, week_start,
                        args, stats, end_date=week_end
                    )

        elif extractor.multi_date:
            # Multi-date extractor: call once with full date range
            print(f"Processing {data_source.descriptor} for date range {args.start_date} to {args.end_date}")

            # Process each PV site with the full date range
            for pv_site in pv_sites:
                print(f"  Processing site: {pv_site.name} (system_id={pv_site.pvo_sys_id})")
                _process_extraction(
                    client, extractor, data_source, pv_site, args.start_date,
                    args, stats, end_date=args.end_date
                )
        else:
            # Single-date extractor: iterate through dates (day by day)
            for date_ in _get_date_range(args):
                print(f"Processing {data_source.descriptor} for {date_}")

                # Process each PV site for this date
                for pv_site in pv_sites:
                    print(f"  Processing site: {pv_site.name} (system_id={pv_site.pvo_sys_id})")
                    _process_extraction(
                        client, extractor, data_source, pv_site, date_, args, stats
                    )

    # Print summary report
    stats.print_summary(dry_run=args.dry_run)


def _process_extraction(
        client,
        extractor,
        data_source: DataSource,
        pv_site: PVSite,
        date_: date,
        args,
        stats: ProcessingStats,
        end_date: date = None,
) -> None:
    """
    Process a single extraction for a given PV site and date (or date range).

    Args:
        client: Storage client (LocalStorageClient or GDriveClientWrapper)
        extractor: The data extractor instance
        data_source: Data source configuration
        pv_site: PV site to extract data for
        date_: Start date (or single date for single-date extractors)
        args: Command-line arguments
        stats: Statistics tracker
        end_date: End date (for multi-date extractors), None for single-date
    """
    # Use polymorphic method to get file path
    file_path = client.get_csv_file_path(data_source, pv_site, date_)

    # Check if file already exists using polymorphic method
    if client.file_exists(file_path) and not ALLOW_DUPLICATE_FILES:
        print(f"    File already exists: {file_path}")
        stats.record_skip_existing()
        return

    if args.dry_run:
        print(f"    Dry run - not writing: {file_path}")
        stats.record_skip_dry_run()
        return

    try:
        # Call extractor with appropriate arguments
        extraction_result = extractor.extract(pv_site, date_, end_date)

        # Write CSV data using polymorphic method
        client.write_csv(file_path, extraction_result.data)

        # Optionally write metadata JSON if requested and available
        if args.write_metadata and extraction_result.metadata:
            try:
                client.write_metadata(file_path, extraction_result.metadata)
            except Exception as meta_e:
                print(f"    WARNING: failed to write metadata for {file_path}: {meta_e}")

        stats.record_success()

    except Exception as e:
        print(f"    ERROR: {e}")
        stats.record_failure(data_source.descriptor, date_, pv_site.pvo_sys_id, pv_site.name, str(e))


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


def _get_date_range(args) -> list[date]:
    day_count = (args.end_date - args.start_date).days
    dates = [args.start_date + timedelta(days=i) for i in range(day_count)]

    if args.reverse:
        dates.reverse()

    return dates


def _get_week_ranges(args) -> list[tuple[date, date]]:
    """Generate week start and end dates between start_date and end_date.
    Only processes whole weeks (Monday to Sunday) that fall entirely within the user-specified date range."""

    # Find the first Monday on or after start_date
    start_offset = args.start_date.weekday()  # 0=Monday, 6=Sunday
    if start_offset == 0:
        # Already a Monday
        first_monday = args.start_date
    else:
        # Move to next Monday
        first_monday = args.start_date + timedelta(days=(7 - start_offset))

    # Find the last Sunday on or before end_date
    end_offset = args.end_date.weekday()  # 0=Monday, 6=Sunday
    if end_offset == 6:
        # Already a Sunday
        last_sunday = args.end_date
    else:
        # Move back to previous Sunday
        last_sunday = args.end_date - timedelta(days=(end_offset + 1))

    # Generate week ranges
    current = first_monday
    week_ranges = []

    while current <= last_sunday:
        # Each week runs from Monday (current) to Sunday (current + 6 days)
        week_end_date = current + timedelta(days=6)

        # Only include if the full week (ending on Sunday) fits within range
        if week_end_date <= last_sunday:
            week_ranges.append((current, week_end_date + timedelta(days=1)))  # +1 to make end exclusive

        # Move to next Monday
        current = current + timedelta(days=7)

    if args.reverse:
        week_ranges.reverse()

    return week_ranges


def _load_pv_sites(system_ids: str) -> list[PVSite]:
    repository = PVSiteRepository.from_csv()

    if not system_ids:
        # User didn't specify IDs, so we're processing all systems
        return repository.get_all()

    system_ids_list = [int(s.strip()) for s in system_ids.split(',') if s.strip()]
    return repository.get_by_system_ids(system_ids_list)


if __name__ == '__main__':
    args = parse_args()
    main(args)