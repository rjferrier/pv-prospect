from datetime import date
import os

from extractors import SourceDescriptor


def build_csv_file_path(source_descriptor: SourceDescriptor, pv_system_id: int, date_: date) -> str:
    """
    Build the full CSV file path for a given data source, site, and date.

    Args:
        source_descriptor: The data source descriptor string (e.g. 'openmeteo/hourly')
        pv_system_id: The PV system identifier (integer)
        date_: The date for the data

    Returns:
        The full CSV file path (e.g., 'openmeteo/hourly/openmeteo-hourly_12345_20231029.csv')
    """
    filename_parts = [
        str(source_descriptor).replace('/', '-'),
        str(pv_system_id),
        format_date(date_)
    ]
    filename = '_'.join(filename_parts) + '.csv'
    return os.path.join(source_descriptor, filename)


def format_date(date_: date) -> str:
    """Format a date as YYYYMMDD string."""
    return "%04d%02d%02d" % (date_.year, date_.month, date_.day)
