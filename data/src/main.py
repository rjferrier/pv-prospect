from datetime import date, timedelta
from argparse import ArgumentParser, RawTextHelpFormatter

from src.extractors.pvoutput import PVOutputExtractor, RETURNED_FIELDS
from src.loaders.gdrive import DataLoader


PV_OUTPUT_PREFIX = 'pvoutput'
FIELDS_CSV_FILE_NAME = 'fields.csv'

Extractor = PVOutputExtractor


def parse_args():
    yesterday = date.today() - timedelta(days=1)
    parser = ArgumentParser(
        prog='etl_pv_outputs',
        formatter_class=lambda prog: RawTextHelpFormatter(prog, width=120)
    )
    parser.format_help()
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


def get_csv_file_name(system_id: int, date_: date) -> str:
    date_str = "%04d%02d%02d" % (date_.year, date_.month, date_.day)
    return f"{PV_OUTPUT_PREFIX}_{system_id}_{date_str}.csv"


def get_date_range(args) -> list[date]:
    day_count = (args.end_date - args.start_date).days
    dates = [args.start_date + timedelta(days=i) for i in range(day_count)]

    if args.reverse:
        dates.reverse()

    return dates


if __name__ == '__main__':
    args = parse_args()

    # initialise components
    api_reader = Extractor.from_env()
    loader = DataLoader.from_folder_name(PV_OUTPUT_PREFIX)
    loader.load_csv_if_absent(RETURNED_FIELDS, FIELDS_CSV_FILE_NAME)

    # run ETL
    for date_ in get_date_range(args):
        print(f"Processing data for {date_}")
        entries = api_reader.extract(args.system_id, date_)
        loader.load_csv(entries, get_csv_file_name(args.system_id, date_))
