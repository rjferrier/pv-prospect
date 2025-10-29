from argparse import ArgumentParser, RawTextHelpFormatter
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Callable

from domain import DateRange, PVSiteRepository, PVSite
from domain.date_range import Period
from extractors.openmeteo import (
    OpenMeteoWeatherDataExtractor, Mode as OMMode, APISelector, TimeResolution, Fields, Models
)
from extractors.visualcrossing import VCWeatherDataExtractor, Mode as VCMode
from extractors.pvoutput import PVOutputExtractor
from loaders.gdrive import GDriveClient
from loaders.local import LocalStorageClient
from processing import extract_and_load, ProcessingStats


@dataclass(frozen=True)
class DataSource:
    descriptor: str
    extractor_factory: Callable


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


def _parse_args():
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
        storage_client = LocalStorageClient(args.local_dir)
    else:
        storage_client = GDriveClient.build_service()

    complete_date_range = _get_complete_date_range(args)

    for source in sources:
        data_source = DATA_SOURCES[source]
        extractor = data_source.extractor_factory()

        if args.by_week and not extractor.multi_date:
            raise ValueError(f"Extractor for source '{source}' does not support multi-date extraction "
                             f"required for --by-week option.")

        sub_date_ranges = complete_date_range.split_by(Period.WEEK if args.by_week else Period.DAY)

        for date_range in sub_date_ranges:
            print(f"Processing {data_source.descriptor} for {date_range}")
            for pv_site in pv_sites:
                print(f"  Processing site: {pv_site.name} (system_id={pv_site.pvo_sys_id})")

                result = extract_and_load(
                    extractor,
                    storage_client,
                    data_source.descriptor,
                    pv_site,
                    date_range,
                    args.dry_run,
                    args.write_metadata,
                )

                stats.record(result)

    # Print summary report
    stats.print_summary(dry_run=args.dry_run)


def _load_pv_sites(system_ids: str) -> list[PVSite]:
    repository = PVSiteRepository.from_csv()

    if not system_ids:
        # User didn't specify IDs, so we're processing all systems
        return repository.get_all()

    system_ids_list = [int(s.strip()) for s in system_ids.split(',') if s.strip()]
    return repository.get_by_system_ids(system_ids_list)


if __name__ == '__main__':
    args = _parse_args()
    main(args)
