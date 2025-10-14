from datetime import date, timedelta
from argparse import ArgumentParser, RawTextHelpFormatter
from dataclasses import dataclass
from typing import Callable

from src.extractors.pvoutput import PVOutputExtractor
from src.extractors.openmeteo import OpenMeteoWeatherDataExtractor, Mode as OMMode
from src.extractors.visualcrossing import VCWeatherDataExtractor, Mode as VCMode
from src.loaders.gdrive import DataLoader
from src.domain.pv_site import get_pv_site_by_system_id, PVSite


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
    parser.add_argument('source', choices=list(DATA_SOURCES.keys()), help="data source")
    parser.add_argument('system_id', type=int, help="system (plant) ID")
    parser.add_argument(
        '-d', '--start-date',
        type=lambda s: date.fromisoformat(s),
        help="start date in the format YYYY-MM-DD (default: yesterday)",
        default=yesterday
    )
    parser.add_argument(
        '-e', '--end-date',
        type=lambda s: date.fromisoformat(s),
        help="end date in the format YYYY-MM-DD (default: start date plus one day)"
    )
    parser.add_argument(
        '-r', '--reverse',
        action="store_true",
        help="process dates in reverse order"
    )
    args = parser.parse_args()

    if not args.end_date:
        args.end_date = args.start_date + timedelta(days=1)

    return args


def get_csv_file_name(data_source: DataSource, pv_site: PVSite, date_: date) -> str:
    """Generate CSV filename using site name and data source"""
    strings = [
        data_source.descriptor.replace('/', '-'),
        str(pv_site.pvoutput_system_id),
        "%04d%02d%02d" % (date_.year, date_.month, date_.day)
    ]
    return '_'.join(strings) + '.csv'


def get_date_range(args) -> list[date]:
    day_count = (args.end_date - args.start_date).days
    dates = [args.start_date + timedelta(days=i) for i in range(day_count)]

    if args.reverse:
        dates.reverse()

    return dates


if __name__ == '__main__':
    args = parse_args()

    # Get PV site information
    pv_site = get_pv_site_by_system_id(args.system_id)
    if pv_site is None:
        raise ValueError(f"No PV site found with system ID {args.system_id}")

    # initialise components
    data_source = DATA_SOURCES[args.source]
    print(f"Processing {data_source.descriptor} for site: {pv_site.name}")

    extractor = data_source.extractor_factory()
    loader = DataLoader.from_path(data_source.descriptor)

    # run ETL
    for date_ in get_date_range(args):
        print(f"Processing data for {date_}")
        entries = extractor.extract(args.system_id, date_)
        filename = get_csv_file_name(data_source, pv_site, date_)
        loader.load_csv(entries, filename)
