from datetime import date
import os
from typing import TYPE_CHECKING

from pv_prospect.data_extraction.extractors.base import TimeSeriesDescriptor

if TYPE_CHECKING:
    from pv_prospect.data_extraction.extractors import SourceDescriptor


def build_csv_file_path(
        time_series_folder: str,
        source_descriptor: 'SourceDescriptor',
        time_series_descriptor: 'TimeSeriesDescriptor',
        date_: date
) -> str:
    """
    Build the full CSV file path for a given data source, time series descriptor, and date.
    """
    filename = build_csv_filename(source_descriptor, time_series_descriptor, date_) + '.csv'
    return os.path.join(time_series_folder, source_descriptor, filename)


def build_csv_filename(
        source_descriptor: "SourceDescriptor", time_series_descriptor: str, date_: date
) -> str:
    """
    Build the CSV filename (without extension) for a given data source, time series descriptor, and date.
    """
    filename_parts = [
        str(source_descriptor).replace('/', '-'),
        str(time_series_descriptor),
        format_date(date_)
    ]
    return '_'.join(filename_parts)


def format_date(date_: date) -> str:
    """Format a date as YYYYMMDD string."""
    return "%04d%02d%02d" % (date_.year, date_.month, date_.day)
