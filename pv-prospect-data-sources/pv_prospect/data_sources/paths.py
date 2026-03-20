import os
from datetime import date


def format_date(date_: date) -> str:
    """Format a date as YYYYMMDD."""
    return '%04d%02d%02d' % (date_.year, date_.month, date_.day)


def build_csv_file_path(
    time_series_folder: str,
    source_descriptor: str,
    time_series_descriptor: str,
    date_: date,
) -> str:
    """Build the CSV file path for a time series extraction.

    The source descriptor's forward slashes become the folder hierarchy,
    while hyphens replace slashes in the filename.
    """
    filename_parts = [
        str(source_descriptor).replace('/', '-'),
        str(time_series_descriptor),
        format_date(date_),
    ]
    filename = '_'.join(filename_parts) + '.csv'
    return os.path.join(
        time_series_folder,
        str(source_descriptor),
        str(time_series_descriptor),
        filename,
    )
