from datetime import date, timedelta
from argparse import ArgumentParser, RawTextHelpFormatter
from dataclasses import dataclass

from src.extractors.pvoutput import PVOutputExtractor, RETURNED_FIELDS as PV_RETURNED_FIELDS
from src.extractors.visualcrossing import VCWeatherDataExtractor
from src.loaders.gdrive import DataLoader
from src.domain.pv_site import get_pv_site_by_system_id, PVSite


@dataclass(frozen=True)
class DataSource:
    name: str
    extractor_cls: type
    returned_fields: list

PV_OUTPUT_PREFIX = 'pvoutput'
FIELDS_CSV_FILE_NAME = 'fields.csv'

# Weather data returned fields (based on the elements in visualcrossing.py)
# TODO: come back to this
WEATHER_RETURNED_FIELDS = [
    ['datetime'], ['temp'], ['humidity'], ['cloudcover'], ['visibility'],
    ['solarradiation'], ['windspeed'], ['winddir'], ['precip'], ['precipremote'],
    ['preciptype'], ['pressure'], ['source'], ['stations']
]

DATA_SOURCES = {
    'pv': DataSource(
        name='pvoutput',
        extractor_cls=PVOutputExtractor,
        returned_fields=PV_RETURNED_FIELDS,
    ),
    'weather': DataSource(
        name='visualcrossing',
        extractor_cls=VCWeatherDataExtractor,
        returned_fields=WEATHER_RETURNED_FIELDS,
    ),
}


def parse_args():
    yesterday = date.today() - timedelta(days=1)
    parser = ArgumentParser(
        prog='etl', formatter_class=lambda prog: RawTextHelpFormatter(prog, width=120)
    )
    parser.format_help()
    parser.add_argument('source', choices=['pv', 'weather'], help="data source")
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
    date_str = "%04d%02d%02d" % (date_.year, date_.month, date_.day)
    return f"{data_source.name}_{pv_site.pvoutput_system_id}_{date_str}.csv"


def get_folder_name(pv_site: PVSite) -> str:
    """Generate folder name based on site name"""
    return pv_site.name.replace(' ', '_').lower()


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
    print(f"Processing {data_source.name} data for site: {pv_site.name}")

    extractor = data_source.extractor_cls.from_env()
    loader = DataLoader.from_folder_name(get_folder_name(data_source.name))
    loader.load_csv_if_absent(data_source.returned_fields, FIELDS_CSV_FILE_NAME)

    # run ETL
    for date_ in get_date_range(args):
        print(f"Processing data for {date_}")
        entries = extractor.extract(args.system_id, date_)
        filename = get_csv_file_name(data_source, pv_site, date_)
        loader.load_csv(entries, filename)
