import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from typing import Any

from pv_prospect.common import (
    build_pv_site_repo,
    get_all_pv_system_ids,
    get_config,
    get_pv_site_by_system_id,
)
from pv_prospect.common.domain import (
    Period,
)
from pv_prospect.data_extraction import DataSource, supports_multi_date
from pv_prospect.data_extraction.config import DataExtractionConfig
from pv_prospect.data_extraction.processing.task_queuer import TaskQueuer
from pv_prospect.data_extraction.processing.tasks import PV_SITES_CSV_FILE
from pv_prospect.data_extraction.resources import get_config_dir as get_de_config_dir
from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
from pv_prospect.etl import DegenerateDateRange, Extractor, build_date_range
from pv_prospect.etl import get_config_dir as get_etl_config_dir
from pv_prospect.etl.storage import get_filesystem
from pv_prospect.etl.storage.backends import LocalStorageConfig

DATA_SOURCES = {
    'pv': DataSource.PVOUTPUT,
    'weather-om-15': DataSource.OPENMETEO_QUARTERHOURLY,
    'weather-om-60': DataSource.OPENMETEO_HOURLY,
    'weather-om-satellite': DataSource.OPENMETEO_SATELLITE,
    'weather-om-historical': DataSource.OPENMETEO_HISTORICAL,
    'weather-om-15-v0': DataSource.OPENMETEO_V0_QUARTERHOURLY,
    'weather-om-60-v0': DataSource.OPENMETEO_V0_HOURLY,
}


def _parse_args() -> 'Any':
    parser = ArgumentParser(
        prog='data-extraction-task-producer',
        formatter_class=lambda prog: RawTextHelpFormatter(prog, width=120),
    )
    parser.format_help()
    parser.add_argument(
        'source',
        help='data source (comma-separated from: {} )'.format(
            ', '.join(DATA_SOURCES.keys())
        ),
    )
    parser.add_argument(
        'system_ids',
        nargs='?',
        default=None,
        help='system ID or comma-separated list of system IDs (e.g. 123 or 123,456). If omitted, all systems will be processed.',
    )
    parser.add_argument(
        '-d',
        '--start-date',
        type=str,
        help="start date: 'today', 'yesterday', YYYY-MM-DD, or YYYY-MM format (default: yesterday)",
        default=None,
    )
    parser.add_argument(
        '-e',
        '--end-date',
        type=str,
        help="end date (exclusive): 'today', 'yesterday', YYYY-MM-DD, or YYYY-MM format (default: start date plus one day)",
    )
    parser.add_argument(
        '-r', '--reverse', action='store_true', help='process dates in reverse order'
    )
    parser.add_argument(
        '-n',
        '--dry-run',
        action='store_true',
        help='Show what would be done, but do not upload or modify any files.',
    )
    parser.add_argument(
        '-w',
        '--by-week',
        action='store_true',
        help='Process one week at a time instead of one day at a time.',
    )

    parser.add_argument(
        '-l',
        '--local-dir',
        type=str,
        default=None,
        help='Write files to a local directory instead of uploading to Google Drive. Specify the directory path.',
    )
    return parser.parse_args()


def _parse_pv_system_ids(system_ids_str: str) -> list[int]:
    return [int(s.strip()) for s in system_ids_str.split(',') if s.strip()]


def _get_all_pv_system_ids(storage_extractor: Extractor) -> list[int]:
    csv_stream = storage_extractor.read_file(PV_SITES_CSV_FILE)
    build_pv_site_repo(csv_stream)
    return get_all_pv_system_ids()


def _main(config: DataExtractionConfig, args: 'Any') -> None:
    task_queuer = TaskQueuer.from_config(config)

    # Parse comma-separated sources
    sources = [s.strip() for s in args.source.split(',')]
    # Validate sources
    data_source_keys = DATA_SOURCES.keys()
    invalid = [s for s in sources if s not in data_source_keys]
    if invalid:
        raise ValueError(
            f'Invalid source(s): {", ".join(invalid)}. Valid options: {", ".join(data_source_keys)}'
        )

    data_sources = [DATA_SOURCES[source] for source in sources]
    task_queuer.preprocess(data_sources, args.local_dir).wait_for_completion()

    pv_system_ids = (
        _parse_pv_system_ids(args.system_ids)
        if args.system_ids
        else _get_all_pv_system_ids(
            Extractor(get_filesystem(LocalStorageConfig(prefix=args.local_dir or '')))
        )
    )

    print(f'Processing {len(pv_system_ids)} PV site(s).\n')

    try:
        complete_date_range = build_date_range(args.start_date, args.end_date)
    except DegenerateDateRange as e:
        print(str(e))
        sys.exit(1)

    # Split into date ranges first (by week or by day)
    sub_date_ranges = complete_date_range.split_by(
        Period.WEEK if args.by_week else Period.DAY
    )
    if args.reverse:
        sub_date_ranges.reverse()

    # Loop over dates first, then sources
    for date_range in sub_date_ranges:
        print(f'Processing {date_range}')

        for source in sources:
            data_source = DATA_SOURCES[source]
            print(f'  Processing {data_source}')

            # Determine which date ranges to use for this source
            if args.by_week and not supports_multi_date(data_source):
                # Extractor doesn't support multi-date, decompose week into single days
                daily_ranges = date_range.split_by(Period.DAY)
                print(f'  Decomposing week into {len(daily_ranges)} days')
                date_ranges_to_process = daily_ranges
            else:
                # Use the date range as-is
                date_ranges_to_process = [date_range]

            counter = 0

            for dr in date_ranges_to_process:
                for pv_system_id in pv_system_ids:
                    pv_site = get_pv_site_by_system_id(pv_system_id)
                    print(
                        f'    Adding {data_source} for {pv_site.name} ({pv_system_id}), {dr}'
                    )

                    task_queuer.extract_and_load(
                        data_source,
                        pv_site,
                        dr,
                        args.local_dir,
                        args.dry_run,
                        counter,
                    )
                    counter += 1

            if counter == 0:
                print('No tasks/results were generated.')
                return


if __name__ == '__main__':
    config_ = get_config(
        DataExtractionConfig,
        base_config_dirs=[
            get_etl_config_dir(),
            get_ds_config_dir(),
            get_de_config_dir(),
        ],
    )
    args_ = _parse_args()
    _main(config_, args_)
