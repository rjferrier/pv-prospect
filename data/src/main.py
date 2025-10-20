from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import date, timedelta

from requests import HTTPError

from src.domain.data_source import DataSource
from src.domain.pv_site import PVSiteRepository, PVSite
from src.extractors.openmeteo import OpenMeteoWeatherDataExtractor, Mode as OMMode
from src.extractors.pvoutput import PVOutputExtractor
from src.extractors.visualcrossing import VCWeatherDataExtractor, Mode as VCMode
from src.loaders.csv_helpers import upload_csv, get_csv_file_path
from src.loaders.gdrive import GDriveClient, CSV_MIME_TYPE
from src.processing import ProcessingStats

ALLOW_DUPLICATE_FILES = False

DATA_SOURCES = {
    'pv': DataSource(
        descriptor='pvoutput',
        extractor_factory=lambda: PVOutputExtractor.from_env()
    ),
    'weather-om-15': DataSource(
        descriptor='openmeteo/quarterhourly',
        extractor_factory=lambda: OpenMeteoWeatherDataExtractor(OMMode.QUARTERHOURLY)
    ),
    'weather-om-60': DataSource(
        descriptor='openmeteo/hourly',
        extractor_factory=lambda: OpenMeteoWeatherDataExtractor(OMMode.HOURLY)
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
        type=_parse_date,
        help="start date: 'today', 'yesterday', or YYYY-MM-DD format (default: yesterday)",
        default=yesterday
    )
    parser.add_argument(
        '-e', '--end-date',
        type=_parse_date,
        help="end date: 'today', 'yesterday', or YYYY-MM-DD format (default: start date plus one day)"
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
    args = parser.parse_args()

    if not args.end_date:
        args.end_date = args.start_date + timedelta(days=1)

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

    # initialise client once
    client = GDriveClient.build_service()

    for source in sources:
        data_source = DATA_SOURCES[source]
        extractor = data_source.extractor_factory()

        # run ETL
        for date_ in _get_date_range(args):
            print(f"Processing {data_source.descriptor} for {date_}")

            # Innermost loop: iterate over each PV site
            for pv_site in pv_sites:
                print(f"  Processing site: {pv_site.name} (system_id={pv_site.pvo_sys_id})")

                # Check if file already exists
                file_path = get_csv_file_path(data_source, pv_site, date_)
                resolved_file_path = client.resolve_path(file_path)

                existing_files = client.search(resolved_file_path, mime_type=CSV_MIME_TYPE)

                if existing_files and not ALLOW_DUPLICATE_FILES:
                    print(f"    File already exists: {file_path}")
                    stats.record_skip_existing()
                    continue

                if args.dry_run:
                    print(f"    Dry run - not uploading: {file_path}")
                    stats.record_skip_dry_run()
                    continue

                # Extract and upload
                try:
                    entries = extractor.extract(pv_site, date_)
                    upload_csv(client, file_path, entries)
                    stats.record_success()
                except HTTPError as e:
                    error_msg = str(e)
                    print(f"    HTTP ERROR: {error_msg}")
                    stats.record_failure(data_source.descriptor, date_, pv_site.pvo_sys_id, pv_site.name, error_msg)
                except Exception as e:
                    error_msg = str(e)
                    print(f"    ERROR: {error_msg}")
                    stats.record_failure(data_source.descriptor, date_, pv_site.pvo_sys_id, pv_site.name, error_msg)

    # Print summary report
    stats.print_summary(dry_run=args.dry_run)


def _parse_date(date_str: str) -> date:
    """Parse date string supporting 'today', 'yesterday', or ISO format (YYYY-MM-DD)."""
    if date_str.lower() == 'today':
        return date.today()
    elif date_str.lower() == 'yesterday':
        return date.today() - timedelta(days=1)

    return date.fromisoformat(date_str)


def _get_date_range(args) -> list[date]:
    day_count = (args.end_date - args.start_date).days
    dates = [args.start_date + timedelta(days=i) for i in range(day_count)]

    if args.reverse:
        dates.reverse()

    return dates


def _load_pv_sites(system_ids: str) -> list[PVSite]:
    repository = PVSiteRepository.from_csv()

    if not system_ids:
        # User didn't specify IDs, so we're processing all systems
        return repository.get_all()

    system_ids_list = [int(s.strip()) for s in args.system_ids.split(',') if s.strip()]
    return repository.get_by_system_ids(system_ids_list)


if __name__ == '__main__':
    args = parse_args()
    main(args)
