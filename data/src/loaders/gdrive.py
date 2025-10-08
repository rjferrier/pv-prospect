import csv
import json
import os
from tempfile import SpooledTemporaryFile
from typing import Iterable, Optional, Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from importlib_resources import files

import resources

CREDS_FILENAME = "gdrive_credentials.json"
TOKEN_FILENAME = "token.json"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

DATA_FOLDER_NAME = "data"
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
CSV_MIME_TYPE = 'text/csv'


def get_credentials() -> Credentials:
    creds = None
    if os.path.exists(TOKEN_FILENAME):
        creds = Credentials.from_authorized_user_file(TOKEN_FILENAME, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # noinspection PyTypeChecker
            source = files(resources).joinpath(CREDS_FILENAME)
            f = source.open()
            client_config = json.load(f)

            flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_FILENAME, "w") as token:
            token.write(creds.to_json())

    return creds


class GDriveClient:
    def __init__(self, service: Any) -> None:
        self.service = service

    @classmethod
    def build_service(cls) -> "GDriveClient":
        """Factory method to build a Google Drive service instance with credentials."""
        service = build("drive", "v3", credentials=get_credentials())
        return cls(service)

    def create_or_get_folder(self, name: str, *, parent_id: Optional[str] = None) -> str:
        """
        Creates a folder in Google Drive if it doesn't exist, or returns existing folder ID.
        Raises an error if more than one such folder exists.

        Args:
            name (str): The name of the folder to create or get.
            parent_id (str, optional): The ID of the parent folder. If None, searches/creates at root.

        Returns:
            str: ID of the folder

        Raises:
            HttpError: If the API request fails
            RuntimeError: If more than one folder with the same name and parent exists
        """

        existing_folders = self.search(name, mime_type=FOLDER_MIME_TYPE, parent_id=parent_id)
        if len(existing_folders) > 1:
            raise RuntimeError(f"More than one folder named '{name}' exists in the specified location.")
        if existing_folders:
            return existing_folders[0]["id"]

        return self._create_folder(name, parent_id)

    def upload_file(self, media_body: MediaIoBaseUpload, file_name: str, parent_id: str, mimetype: str) -> str:
        """
        Uploads a file to Google Drive in the specified parent folder.

        Args:
            media_body: A file-like object or MediaIoBaseUpload instance containing the file data.
            file_name (str): The name of the file to upload.
            parent_id (str): The ID of the parent folder.
            mimetype (str): The MIME type of the file (e.g., 'text/csv').

        Returns:
            str: The file ID of the uploaded file.
        """
        file_metadata = {"name": file_name, "parents": [parent_id]}
        try:
            file = self.service.files().create(
                body=file_metadata,
                media_body=media_body,
                fields="id"
            ).execute()
            return file.get("id")
        except HttpError as error:
            print(f"An error occurred while uploading {file_name}: {error}")
            raise

    def search(self, file_name: str, mime_type: str, parent_id: Optional[str] = None) -> list[dict]:
        """
        Common method to search for files/folders based on name, parent, and optional mime type.

        Args:
            file_name (str): The name of the file/folder to search for
            mime_type (Optional[str]): The MIME type to filter by (e.g., folder mime type)
            parent_id (Optional[str]): The ID of the parent folder. If None, searches at root level

        Returns:
            list[dict]: List of matching files/folders
        """
        query_conditions = [f"name='{file_name}'", f"mimeType='{mime_type}'", "trashed=false"]
        if parent_id:
            query_conditions.append(f"'{parent_id}' in parents")

        query_str = " and ".join(query_conditions)
        try:
            results = self.service.files().list(
                q=query_str,
                spaces="drive",
                fields="files(id, name, parents)"
            ).execute()
            return results.get("files", [])
        except HttpError as error:
            print(f"Error while searching files with query '{query_str}': {error}")
            raise

    def _create_folder(self, name: str, parent_id: Optional[str] = None) -> str:
        """
        Creates a folder in Google Drive and returns its ID.
        """
        folder_metadata = {
            "name": name,
            "mimeType": FOLDER_MIME_TYPE,
            "parents": [parent_id] if parent_id else []
        }
        try:
            folder = self.service.files().create(
                body=folder_metadata,
                fields="id"
            ).execute()
            return folder.get("id")
        except HttpError as error:
            print(f"Error while creating folder {name}: {error}")
            raise


class DataLoader:
    def __init__(self, client: GDriveClient, folder_id: str) -> None:
        self.client = client
        self.folder_id = folder_id

    @classmethod
    def from_folder_name(cls, folder_name: str) -> "DataLoader":
        """
        Factory method to build a DataLoader with a GDriveClient and a folder (created or retrieved).
        """
        client = GDriveClient.build_service()
        parent_id = client.create_or_get_folder(DATA_FOLDER_NAME)
        folder_id = client.create_or_get_folder(folder_name, parent_id=parent_id)
        return cls(client, folder_id)

    def load_csv(self, data: Iterable[Iterable[str]], file_name: str) -> str:
        """
        Uploads data as a CSV file to the associated Google Drive folder.

        Args:
            data (Iterable[Iterable[str]]): The data to write (e.g., CSV rows).
            file_name (str): The name of the file to upload.

        Returns:
            str: The file ID of the uploaded file.
        """
        with SpooledTemporaryFile(max_size=1_000_000, mode='w+', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(data)
            f.seek(0)
            media_upload = MediaIoBaseUpload(f, mimetype=CSV_MIME_TYPE, chunksize=-1)
            return self.client.upload_file(media_upload, file_name, self.folder_id, CSV_MIME_TYPE)

    def load_csv_if_absent(self, data: Iterable[Iterable[str]], file_name: str) -> Optional[str]:
        """
        Uploads data as a CSV file to the associated Google Drive folder only if the file doesn't already exist.

        Args:
            data (Iterable[Iterable[str]]): The data to write (e.g., CSV rows).
            file_name (str): The name of the file to upload.

        Returns:
            Optional[str]: The file ID of the uploaded file if it was uploaded, None if file already exists.
        """
        if self.client.search(file_name, CSV_MIME_TYPE, self.folder_id):
            return None

        return self.load_csv(data, file_name)
