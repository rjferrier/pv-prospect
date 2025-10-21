import csv
from io import StringIO
from datetime import date
from tempfile import SpooledTemporaryFile
from typing import Iterable
import json

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


def upload_metadata(client: GDriveClient, csv_file_path: str, metadata: dict) -> None:
    """
    Upload JSON metadata to Google Drive alongside the corresponding CSV file.

    The metadata filename is derived from the CSV filename by replacing the .csv extension with .json.

    Args:
        client (GDriveClient): The Google Drive client instance.
        csv_file_path (str): The CSV file path (used to derive the metadata filename and folder).
        metadata (dict): The metadata to write as JSON.
    """
    # Derive metadata filename from CSV path (replace .csv with .json)
    if csv_file_path.lower().endswith('.csv'):
        metadata_path = csv_file_path[:-4] + '.json'
    else:
        metadata_path = csv_file_path + '.json'

    with SpooledTemporaryFile(mode='w+b') as tmp:
        text = json.dumps(metadata, indent=2, ensure_ascii=False)
        tmp.write(text.encode('utf-8'))
        tmp.seek(0)

        media_body = MediaIoBaseUpload(tmp, mimetype='application/json', resumable=True)
        resolved_file_path = client.resolve_path(metadata_path)
        client.upload_file(media_body, resolved_file_path, 'application/json')


def get_csv_file_path(data_source: DataSource, pv_site: PVSite, date_: date, end_date: date = None) -> str:
    """Generate CSV filename using site name and data source"""
    filename_parts = [
        data_source.descriptor.replace('/', '-'),
        str(pv_site.pvo_sys_id),
        _format_date(date_)
    ]
    if end_date:
        filename_parts.append(_format_date(end_date))
    filename = '_'.join(filename_parts) + '.csv'
    return '/'.join((DATA_FOLDER_NAME, data_source.descriptor, filename))


def _format_date(date_: date) -> str:
    return "%04d%02d%02d" % (date_.year, date_.month, date_.day)
