import io
import json
import time
import csv
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from io import StringIO
from tempfile import SpooledTemporaryFile
from typing import Optional, Any, Callable, TypeVar, Iterator

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from importlib_resources import files

import pv_prospect.data_extraction.resources as resources


CREDS_FILENAME = "gdrive_credentials.json"
TOKEN_FILENAME = "gdrive_token.json"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
CSV_MIME_TYPE = 'text/csv'
DATA_FOLDER_NAME = "data"

MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

T = TypeVar('T')


def get_credentials() -> Credentials:
    creds = _read_creds_from_token_file()

    # If we have valid credentials, return them
    if creds and creds.valid:
        return creds

    # Try to refresh expired credentials
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except RefreshError:
            print("Credentials have been revoked or expired, need to re-authenticate.")
            creds = _get_new_creds()
    else:
        # No valid credentials exist, get new ones
        creds = _get_new_creds()

    # Save the credentials for the next run
    token_path = files(resources).joinpath(TOKEN_FILENAME)
    with token_path.open("w") as token:
        token.write(creds.to_json())

    return creds


def _read_creds_from_token_file() -> Optional[Credentials]:
    token_path = files(resources).joinpath(TOKEN_FILENAME)

    # Check if token file exists
    try:
        with token_path.open("r") as f:
            token_data = f.read()
    except (FileNotFoundError, OSError):
        return None

    if not token_data:
        return None

    try:
        return Credentials.from_authorized_user_info(json.loads(token_data), SCOPES)
    except (ValueError, KeyError, json.JSONDecodeError):
        # Token file exists but is empty or malformed
        return None


def _get_new_creds() -> Credentials:
    # noinspection PyTypeChecker
    source = files(resources).joinpath(CREDS_FILENAME)
    f = source.open()
    client_config = json.load(f)

    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    creds = flow.run_local_server(port=0)
    return creds


def retry_on_500(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to retry a function on HTTP 500 errors with exponential backoff"""
    def wrapper(*args, **kwargs) -> T:
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except HttpError as e:
                if e.resp.status == 500 and attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (attempt + 1)
                    print(f"HTTP 500 error in {func.__name__}, retrying in {delay}s (attempt {attempt + 1}/{MAX_RETRIES})")
                    time.sleep(delay)
                else:
                    raise
        # raise RuntimeError(f"Retry logic failed unexpectedly for {func.__name__}")
    return wrapper


@dataclass(frozen=True)
class ResolvedFilePath:
    name: str | None = None
    parent_id: str | None = None


class GDriveClient:
    def __init__(self, service: Any) -> None:
        self.service = service

    @classmethod
    def build_service(cls) -> "GDriveClient":
        """Factory method to build a Google Drive service instance with credentials."""
        service = build("drive", "v3", credentials=get_credentials())
        return cls(service)

    def create_folder(self, folder_path: str) -> str | None:
        """
        Creates a folder path in Google Drive (e.g., 'pvoutput/subfolder').
        All paths are automatically scoped under the DATA_FOLDER_NAME root folder.
        Creates all intermediate folders if they don't exist.

        Args:
            folder_path: The folder path as a string, relative to the data folder (e.g., 'pvoutput')

        Returns:
            str | None: The ID of the final folder in the path, or None if no folders were created

        Raises:
            HttpError: If the API request fails
        """
        parts = [p for p in folder_path.split('/') if p]
        # Start from the data folder
        parent_id = self._get_data_folder_id()
        created_any = False

        for part in parts:
            resolved_path = ResolvedFilePath(name=part, parent_id=parent_id)

            # Try to get existing folder, create if it doesn't exist
            try:
                parent_id = self.get_folder(resolved_path)
            except FileNotFoundError:
                parent_id = self._create_folder_internal(resolved_path)
                created_any = True
                print(f"    Created folder: {part}")

        return parent_id if created_any else None

    @retry_on_500
    def download_file(self, file_path: ResolvedFilePath, destination_path: str, mime_type: Optional[str] = None) -> None:
        """
        Downloads a file from Google Drive to the local file system.

        Args:
            file_path (ResolvedFilePath): The file path with name and parent_id.
            destination_path (str): The local file system path where the file should be saved.
            mime_type (Optional[str]): The MIME type to filter by. If None, any file type is matched.

        Raises:
            HttpError: If the API request fails
            FileNotFoundError: If the file is not found
        """
        file_id = self._get_file_id(file_path, mime_type)

        with io.FileIO(destination_path, 'wb') as fh:
            self._download_to_stream(file_id, fh)

    @contextmanager
    def download_to_stream(self, file_path: ResolvedFilePath, mime_type: Optional[str] = None) -> Iterator[io.BytesIO]:
        """
        Context manager that downloads a file from Google Drive into a BytesIO stream.

        Args:
            file_path (ResolvedFilePath): The file path with name and parent_id.
            mime_type (Optional[str]): The MIME type to filter by. If None, any file type is matched.

        Yields:
            io.BytesIO: A stream containing the downloaded file content.

        Raises:
            HttpError: If the API request fails
            FileNotFoundError: If the file is not found

        Example:
            file_path = client.resolve_path('pvoutput/data.csv')
            with client.download_to_stream(file_path) as stream:
                content = stream.read()
        """
        file_id = self._get_file_id(file_path, mime_type)

        stream = io.BytesIO()
        self._download_to_stream(file_id, stream)
        stream.seek(0)

        try:
            yield stream
        finally:
            stream.close()

    def file_exists(self, file_path: str) -> bool:
        """Check if a file exists in Google Drive."""
        resolved_file_path = self._resolve_path(file_path)
        existing_files = self.search(resolved_file_path, mime_type=CSV_MIME_TYPE)
        return len(existing_files) > 0

    def get_folder(self, folder_path: ResolvedFilePath) -> str:
        """
        Gets a folder ID from Google Drive.
        If folder_path.parent_id is None, the search starts from the DATA_FOLDER_NAME root folder.
        Raises an error if the folder doesn't exist or if more than one such folder exists.

        Args:
            folder_path (ResolvedFilePath): The folder path with name and parent_id.

        Returns:
            str: ID of the folder

        Raises:
            HttpError: If the API request fails
            RuntimeError: If more than one folder with the same name and parent exists
            FileNotFoundError: If the folder doesn't exist
        """
        # If no parent_id is specified, default to the data folder
        if folder_path.parent_id is None and folder_path.name != DATA_FOLDER_NAME:
            search_path = ResolvedFilePath(name=folder_path.name, parent_id=self._get_data_folder_id())
        else:
            search_path = folder_path

        existing_folders = self.search(search_path, mime_type=FOLDER_MIME_TYPE)
        if len(existing_folders) > 1:
            raise RuntimeError(f"More than one folder named '{folder_path.name}' exists in the specified location.")
        if not existing_folders:
            raise FileNotFoundError(f"Folder '{folder_path.name}' not found in the specified location.")

        return existing_folders[0]["id"]

    def list_files(self, folder_path: str | None = None, pattern: str = "*", recursive: bool = False) -> list[dict]:
        """
        List files in a Google Drive folder.

        Args:
            folder_path: The relative path to the folder (e.g., 'pvoutput'). If None, lists from data folder root.
            pattern: Pattern for matching files (e.g., '*.csv'). Only simple wildcards supported.
            recursive: If True, recursively list files in subdirectories

        Returns:
            List of dicts with 'id', 'name', 'path' (relative to data folder), and 'parent_path'
        """
        # Determine the starting folder
        if folder_path:
            try:
                # Resolve to get the folder ID
                resolved = self._resolve_path(folder_path + '/dummy.csv')
                folder_id = resolved.parent_id
                base_path = folder_path
            except (FileNotFoundError, ValueError):
                return []
        else:
            folder_id = self._get_data_folder_id()
            base_path = ''

        # Convert pattern to a simple filter (only handle *.ext or * patterns)
        mime_filter = None
        name_filter = None
        if pattern == '*.csv':
            mime_filter = CSV_MIME_TYPE
        elif pattern != '*':
            # For other patterns, we'll filter by name prefix after retrieval
            if pattern.startswith('*.'):
                # Extract extension for MIME type matching
                ext = pattern[2:]
                if ext == 'json':
                    mime_filter = 'application/json'
            else:
                name_filter = pattern

        # List files in this folder
        files = []

        if recursive:
            # Recursively list all files
            files.extend(self._list_files_recursive(folder_id, base_path, mime_filter, name_filter))
        else:
            # List only files in this folder
            search_path = ResolvedFilePath(name=None, parent_id=folder_id)
            found_files = self.search(search_path, mime_type=mime_filter)

            for file in found_files:
                # Apply name filter if specified
                if name_filter and not file['name'].startswith(name_filter):
                    continue

                # Construct relative path
                file_rel_path = f"{base_path}/{file['name']}" if base_path else file['name']

                files.append({
                    'id': file['id'],
                    'name': file['name'],
                    'path': file_rel_path,
                    'parent_path': base_path,
                })

        return files

    @retry_on_500
    def move_file(self, file_id: str, old_parent_id: str, new_parent_id: str) -> None:
        """
        Moves a file from one folder to another on Google Drive.

        Args:
            file_id (str): The ID of the file to move.
            old_parent_id (str): The ID of the current parent folder.
            new_parent_id (str): The ID of the new parent folder.
        """
        try:
            self.service.files().update(
                fileId=file_id,
                addParents=new_parent_id,
                removeParents=old_parent_id,
                fields='id, parents'
            ).execute()
        except HttpError as error:
            print(f"Error while moving file {file_id}: {error}")
            raise

    def read_file(self, file_path: str) -> io.TextIOWrapper:
        """
        Read a file from Google Drive and return it as a text stream.

        Args:
            file_path: The file path as a string (e.g., 'pvoutput/file.csv')

        Returns:
            io.TextIOWrapper: A text stream containing the file content with UTF-8 encoding

        Raises:
            FileNotFoundError: If the file doesn't exist
            HttpError: If the API request fails
        """
        resolved_file_path = self._resolve_path(file_path)
        file_id = self._get_file_id(resolved_file_path, mime_type=None)

        stream = io.BytesIO()
        self._download_to_stream(file_id, stream)
        stream.seek(0)

        # Wrap BytesIO with TextIOWrapper for text operations
        return io.TextIOWrapper(stream, encoding='utf-8')

    @retry_on_500
    def rename_file(self, file_id: str, new_name: str) -> None:
        """
        Renames a file on Google Drive.

        Args:
            file_id (str): The ID of the file to rename.
            new_name (str): The new name for the file.
        """
        try:
            self.service.files().update(fileId=file_id, body={'name': new_name}).execute()
        except HttpError as error:
            print(f"Error while renaming file {file_id}: {error}")
            raise

    @retry_on_500
    def search(
            self,
            file_path: ResolvedFilePath,
            *,
            mime_type: Optional[str] = None,
            include_trashed: bool = False
    ) -> list[dict]:
        """
        Search for files/folders based on name, parent, and optional mime type.

        Args:
            file_path (ResolvedFilePath): The file path with name and parent_id to search for.
            mime_type (Optional[str]): The MIME type to filter by (e.g., folder mime type)
            include_trashed (bool): Whether to include trashed files in the search results

        Returns:
            list[dict]: List of matching files/folders
        """
        query_str = _build_query_string(file_path.name, mime_type, file_path.parent_id, include_trashed)

        def search_files(page_token: Optional[str] = None) -> tuple[list[dict], Optional[str]]:
            results = self.service.files().list(
                q=query_str,
                spaces="drive",
                fields="nextPageToken, files(id, name, parents)",
                pageToken=page_token
            ).execute()
            return results.get("files", []), results.get("nextPageToken", None)

        try:
            files_, next_page_token = search_files()
            while next_page_token:
                more_files, next_page_token = search_files(next_page_token)
                files_.extend(more_files)
            return files_
        except HttpError as error:
            print(f"Error while searching files with query '{query_str}': {error}")
            raise

    @retry_on_500
    def trash_file(self, file_id: str) -> None:
        """
        Moves a file to the Google Drive trash bin.

        Args:
            file_id (str): The ID of the file to trash.
        """
        try:
            self.service.files().update(fileId=file_id, body={'trashed': True}).execute()
        except HttpError as error:
            print(f"Error while trashing file {file_id}: {error}")
            raise

    @retry_on_500
    def upload_file(self, media_body: MediaIoBaseUpload, file_path: ResolvedFilePath, mimetype: str) -> str:
        """
        Uploads a file to Google Drive in the specified parent folder.

        Args:
            media_body: A file-like object or MediaIoBaseUpload instance containing the file data.
            file_path (ResolvedFilePath): The file path with name and parent_id.
            mimetype (str): The MIME type of the file (e.g., 'text/csv').

        Returns:
            str: The file ID of the uploaded file.
        """
        file_metadata = {
            'name': file_path.name,
            'parents': [file_path.parent_id] if file_path.parent_id else [],
            'mimeType': mimetype
        }
        try:
            file = (
                self.service.files()
                .create(body=file_metadata, media_body=media_body, fields='id')
                .execute()
            )
            return file.get('id')
        except HttpError as error:
            print(f"An error occurred while uploading {file_path.name}: {error}")
            raise

    def write_csv(self, file_path: str, rows, overwrite: bool = False) -> None:
        """Upload CSV data to Google Drive.

        If overwrite is False and a matching CSV already exists, raise FileExistsError.
        If overwrite is True, existing matching files are trashed before upload.
        """
        resolved_file_path = self._resolve_path(file_path)

        # Check for existing files
        existing_files = self.search(resolved_file_path, mime_type=CSV_MIME_TYPE)
        if existing_files and not overwrite:
            raise FileExistsError(f"File already exists on Google Drive: {file_path}")

        # If overwrite requested, trash existing files
        if existing_files and overwrite:
            for f in existing_files:
                try:
                    self.trash_file(f['id'])
                except Exception as e:
                    print(f"Warning: failed to trash existing file {f.get('id')}: {e}")

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
            # resolved_file_path = self.resolve_path(file_path)  # already resolved above
            self.upload_file(media_body, resolved_file_path, CSV_MIME_TYPE)

    def write_metadata(self, csv_file_path: str, metadata: dict) -> None:
        """Upload JSON metadata to Google Drive."""
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
            resolved_file_path = self._resolve_path(metadata_path)
            self.upload_file(media_body, resolved_file_path, 'application/json')

    # Private methods (alphabetically)

    def _create_folder_internal(self, folder_path: ResolvedFilePath) -> str:
        """
        Internal method to create a single folder in Google Drive.

        Args:
            folder_path (ResolvedFilePath): The folder path with name and parent_id.

        Returns:
            str: The folder ID.

        Raises:
            HttpError: If the API request fails
        """
        folder_metadata = {
            "name": folder_path.name,
            "mimeType": FOLDER_MIME_TYPE,
            "parents": [folder_path.parent_id] if folder_path.parent_id else []
        }
        try:
            folder = self.service.files().create(
                body=folder_metadata,
                fields="id"
            ).execute()
            return folder.get("id")
        except HttpError as error:
            print(f"Error while creating folder {folder_path.name}: {error}")
            raise

    def _download_to_stream(self, file_id: str, stream: io.IOBase) -> None:
        """
        Downloads a file from Google Drive to a stream.

        Args:
            file_id (str): The ID of the file to download.
            stream (io.IOBase): The stream to write the file content to.

        Raises:
            HttpError: If the API request fails
        """
        request = self._get_file_media(file_id)
        downloader = MediaIoBaseDownload(stream, request)

        done = False
        while done is False:
            try:
                status, done = downloader.next_chunk()
                if status:
                    print(f"Download {int(status.progress() * 100)}%.")
            except HttpError as error:
                print(f"An error occurred while downloading file: {error}")
                raise

    @lru_cache(maxsize=1)
    def _get_data_folder_id(self) -> str:
        """
        Get the ID of the root data folder, caching it for subsequent calls.
        All GDriveClient operations are scoped under this folder.

        Returns:
            str: The folder ID of the DATA_FOLDER_NAME folder
        """
        # Search for the data folder at the root level (parent_id=None)
        data_folder_path = ResolvedFilePath(name=DATA_FOLDER_NAME, parent_id=None)
        existing_folders = self.search(data_folder_path, mime_type=FOLDER_MIME_TYPE)

        if len(existing_folders) > 1:
            raise RuntimeError(f"More than one folder named '{DATA_FOLDER_NAME}' exists at the root level.")
        if not existing_folders:
            # Create the data folder if it doesn't exist
            folder_metadata = {
                "name": DATA_FOLDER_NAME,
                "mimeType": FOLDER_MIME_TYPE,
            }
            try:
                folder = self.service.files().create(
                    body=folder_metadata,
                    fields="id"
                ).execute()
                folder_id = folder.get("id")
                print(f"    Created root data folder: {DATA_FOLDER_NAME}")
                return folder_id
            except HttpError as error:
                print(f"Error while creating root data folder {DATA_FOLDER_NAME}: {error}")
                raise
        else:
            return existing_folders[0]["id"]

    def _get_file_id(self, file_path: ResolvedFilePath, mime_type: Optional[str] = None) -> str:
        """
        Search for a file by name and return its ID.

        Args:
            file_path (ResolvedFilePath): The file path with name and parent_id to search for.
            mime_type (Optional[str]): The MIME type to filter by. If None, any file type is matched.

        Returns:
            str: The file ID.

        Raises:
            FileNotFoundError: If the file is not found.
        """
        files_ = self.search(file_path, mime_type=mime_type)
        if not files_:
            raise FileNotFoundError(f"File '{file_path.name}' not found in the specified Google Drive folder.")
        return files_[0]['id']

    @retry_on_500
    def _get_file_media(self, file_id: str) -> Any:
        """
        Gets the media request for downloading a file from Google Drive.

        Args:
            file_id (str): The ID of the file to download.

        Returns:
            The media request object for downloading.
        """
        return self.service.files().get_media(fileId=file_id)

    def _list_files_recursive(self, folder_id: str, base_path: str, mime_filter: str | None, name_filter: str | None) -> list[dict]:
        """
        Recursively list files in a folder and its subfolders.

        Args:
            folder_id: The folder ID to start from
            base_path: The relative path prefix for files in this folder
            mime_filter: Optional MIME type filter
            name_filter: Optional name prefix filter

        Returns:
            List of file dicts with 'id', 'name', 'path', and 'parent_path'
        """
        files = []

        # Get all items in this folder (files and subfolders)
        search_path = ResolvedFilePath(name=None, parent_id=folder_id)

        # Get files
        if mime_filter:
            found_files = self.search(search_path, mime_type=mime_filter)
        else:
            found_files = self.search(search_path, mime_type=None)

        for file in found_files:
            # Apply name filter if specified
            if name_filter and not file['name'].startswith(name_filter):
                continue

            # Construct relative path
            file_rel_path = f"{base_path}/{file['name']}" if base_path else file['name']

            files.append({
                'id': file['id'],
                'name': file['name'],
                'path': file_rel_path,
                'parent_path': base_path,
            })

        # Get subfolders and recurse
        subfolders = self.search(search_path, mime_type=FOLDER_MIME_TYPE)
        for folder in subfolders:
            subfolder_path = f"{base_path}/{folder['name']}" if base_path else folder['name']
            files.extend(self._list_files_recursive(folder['id'], subfolder_path, mime_filter, name_filter))

        return files

    def _resolve_path(self, path: str) -> ResolvedFilePath:
        """
        Resolves a path like 'pvoutput/file.csv' to a ResolvedFilePath with the file name and parent folder ID.
        All paths are automatically scoped under the DATA_FOLDER_NAME root folder.

        Args:
            path (str): The path to resolve (relative to the data folder).

        Returns:
            ResolvedFilePath: The resolved file path with name and parent_id.
        """
        parts = [p for p in path.split('/') if p]
        if not parts:
            raise ValueError("Path must not be empty")

        file_name = parts[-1]
        # Start from the data folder
        parent_id = self._get_data_folder_id()

        for part in parts[:-1]:
            folder_path = ResolvedFilePath(name=part, parent_id=parent_id)
            parent_id = self.get_folder(folder_path)

        return ResolvedFilePath(name=file_name, parent_id=parent_id)


def _format_date(date_: date) -> str:
    """Format date as YYYYMMDD string"""
    return "%04d%02d%02d" % (date_.year, date_.month, date_.day)


def _build_query_string(
        file_name: str | None,
        mime_type: str | None,
        parent_id: str | None,
        include_trashed: bool = False
) -> str:
    query_conditions = []
    if file_name:
        query_conditions.append(f"name = '{file_name}'")
    if mime_type:
        query_conditions.append(f"mimeType = '{mime_type}'")
    if parent_id:
        query_conditions.append(f"'{parent_id}' in parents")
    if not include_trashed:
        query_conditions.append("trashed = false")
    query_str = " and ".join(query_conditions)
    return query_str
