import csv
import io
from loaders.gdrive import GDriveClient, DATA_FOLDER_NAME, ResolvedFilePath

HEADER = [
    'date', 'time', 'energy', 'efficiency', 'power',
    'average', 'normalised', 'energy_used', 'power_used',
    'temperature', 'voltage'
]

def get_pvoutput_folder_id(client):
    data_folder = ResolvedFilePath(name=DATA_FOLDER_NAME, parent_id=None)
    parent_id = client.create_or_get_folder(data_folder)
    pvoutput_folder = ResolvedFilePath(name='pvoutput', parent_id=parent_id)
    return client.create_or_get_folder(pvoutput_folder)

def list_csv_files(client, folder_id):
    search_path = ResolvedFilePath(name=None, parent_id=folder_id)
    return client.search(search_path, mime_type='text/csv')

def download_csv(client, file_id):
    request = client.service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = client.service._http.request
    response, content = downloader(request.uri, "GET")
    fh.write(content)
    fh.seek(0)
    return fh.read().decode('utf-8')

def upload_csv(client, folder_id, file_name, content):
    media = io.BytesIO(content.encode('utf-8'))
    from googleapiclient.http import MediaIoBaseUpload
    media_upload = MediaIoBaseUpload(media, mimetype='text/csv', resumable=False)
    client.upload_file(media_upload, file_name, folder_id, 'text/csv')

def process_csv_files():
    client = GDriveClient.build_service()
    folder_id = get_pvoutput_folder_id(client)
    files = list_csv_files(client, folder_id)
    for file in files:
        file_id = file['id']
        file_name = file['name']
        csv_content = download_csv(client, file_id)
        rows = list(csv.reader(io.StringIO(csv_content)))
        if not rows:
            continue
        # Check if header already present
        if rows[0] == HEADER:
            print(f"Skipping {file_name} - header already present")
            continue
        # Verify file has 11 columns
        if len(rows[0]) != 11:
            print(f"Skipping {file_name} - expected 11 columns, found {len(rows[0])}")
            continue
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(HEADER)
        writer.writerows(rows)
        upload_csv(client, folder_id, file_name, output.getvalue())
        client.trash_file(file_id)
        print(f"Updated header for {file_name} and moved old file to trash")

if __name__ == '__main__':
    process_csv_files()
