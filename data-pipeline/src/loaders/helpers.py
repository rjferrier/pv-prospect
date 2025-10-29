from datetime import date
import os

from domain.pv_site import PVSite

DATA_FOLDER_NAME = "data"


def build_csv_file_path(source_descriptor: str, pv_site: PVSite, date_: date) -> str:
    """
    Build the full CSV file path for a given data source, site, and date.

    Args:
        source_descriptor: The data source descriptor string
        pv_site: The PV site
        date_: The date for the data

    Returns:
        The full CSV file path (e.g., 'data/openmeteo-hourly/openmeteo-hourly_12345_20231029.csv')
    """
    filename_parts = [
        source_descriptor.replace('/', '-'),
        str(pv_site.pvo_sys_id),
        format_date(date_)
    ]
    filename = '_'.join(filename_parts) + '.csv'
    return os.path.join(DATA_FOLDER_NAME, source_descriptor, filename)


def format_date(date_: date) -> str:
    """Format a date as YYYYMMDD string."""
    return "%04d%02d%02d" % (date_.year, date_.month, date_.day)
