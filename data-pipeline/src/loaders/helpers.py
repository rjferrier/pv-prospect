from datetime import date

from domain.data_source import DataSource
from domain.pv_site import PVSite
from loaders.gdrive import DATA_FOLDER_NAME


def get_csv_file_path(data_source: DataSource, pv_site: PVSite, date_: date) -> str:
    """Generate CSV filename using site name and data source"""
    filename_parts = [
        data_source.descriptor.replace('/', '-'),
        str(pv_site.pvo_sys_id),
        _format_date(date_)
    ]
    filename = '_'.join(filename_parts) + '.csv'
    return '/'.join((DATA_FOLDER_NAME, data_source.descriptor, filename))


def _format_date(date_: date) -> str:
    return "%04d%02d%02d" % (date_.year, date_.month, date_.day)
