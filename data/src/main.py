from datetime import date, timedelta
from argparse import ArgumentParser

from src.extractors.pvoutput import PVOutputExtractor, RETURNED_FIELDS
from src.loaders.gdrive import DataLoader


PV_OUTPUT_PREFIX = 'pvoutput'
FIELDS_CSV_FILE_NAME = 'fields.csv'

Extractor = PVOutputExtractor


def parse_args():
    yesterday = date.today() - timedelta(days=1)
    parser = ArgumentParser(prog='etl_pv_outputs')
    parser.format_help()
    parser.add_argument('--system-id', '-s', type=int, help="system (plant) ID", required=True)
    parser.add_argument(
        '--start-date', '-d',
        type=lambda s: date.fromisoformat(s),
        help="start date in the format YYYY-MM-DD (default: yesterday)",
        default=yesterday
    )
    parser.add_argument(
        '--end-date', '-e',
        type=lambda s: date.fromisoformat(s),
        help="end date in the format YYYY-MM-DD (default: same as start date)"
    )
    args = parser.parse_args()

    if not args.end_date:
        args.end_date = args.start_date

        if args.date:
            args.start_date = args.date
        else:
            args.start_date = yesterday
        args.date = args.start_date  # for compatibility with the rest of the code

    return args


def get_csv_file_name(system_id: int, date_: date) -> str:
    date_str = "%04d%02d%02d" % (date_.year, date_.month, date_.day)
    return f"{PV_OUTPUT_PREFIX}_{system_id}_{date_str}.csv"


if __name__ == '__main__':
    args = parse_args()

    # initialise components
    api_reader = Extractor.from_env()
    loader = DataLoader.from_folder_name(PV_OUTPUT_PREFIX)
    loader.load_csv_if_absent(RETURNED_FIELDS, FIELDS_CSV_FILE_NAME)

    # run ETL
    date_range = (
        args.start_date + timedelta(n)
        for n in range((args.end_date - args.start_date).days + 1)
    )
    for date_ in date_range:
        print(f"Processing data for {date_}")
        entries = api_reader.extract(args.system_id, date_)
        loader.load_csv(entries, get_csv_file_name(args.system_id, date_))
