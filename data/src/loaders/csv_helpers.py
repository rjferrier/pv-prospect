import csv
from io import StringIO
from datetime import date
from tempfile import SpooledTemporaryFile
from typing import Iterable

from googleapiclient.http import MediaIoBaseUpload

from src.domain.data_source import DataSource
from src.domain.pv_site import PVSite
from src.loaders.gdrive import GDriveClient, DATA_FOLDER_NAME, CSV_MIME_TYPE


def upload_csv(client: GDriveClient, file_path: str, rows: Iterable[Iterable[str]]) -> None:
    """
    Upload CSV data to Google Drive.

    Args:
        client (GDriveClient): The Google Drive client instance.
        file_path (str): The full path to the file (e.g., 'pvoutput/12345.csv').
        rows (Iterable[Iterable[str]]): The CSV data as an iterable of rows.
    """
    with SpooledTemporaryFile(mode='w+b') as tmp:
        # Write CSV data as text first
        text_stream = StringIO()
        writer = csv.writer(text_stream)
        for row in rows:
            writer.writerow(row)

        # Convert to bytes and write to the binary temp file
        tmp.write(text_stream.getvalue().encode('utf-8'))
        tmp.seek(0)

        media_body = MediaIoBaseUpload(tmp, mimetype=CSV_MIME_TYPE, resumable=True)
        resolved_file_path = client.resolve_path(file_path)
        client.upload_file(media_body, resolved_file_path, CSV_MIME_TYPE)


def get_csv_file_path(data_source: DataSource, pv_site: PVSite, date_: date) -> str:
    """Generate CSV filename using site name and data source"""
    filename_parts = [
        data_source.descriptor.replace('/', '-'),
        str(pv_site.pvo_sys_id),
        "%04d%02d%02d" % (date_.year, date_.month, date_.day)
    ]
    filename = '_'.join(filename_parts) + '.csv'
    return '/'.join((DATA_FOLDER_NAME, data_source.descriptor, filename))
