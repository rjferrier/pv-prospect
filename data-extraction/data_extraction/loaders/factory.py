from io import BytesIO
from typing import Protocol, Iterable

from loaders.gdrive import GDriveClient
from loaders.local import LocalStorageClient


class StorageClient(Protocol):
    def read_file(self, file_path: str) -> BytesIO:
        """Read a file and return its contents as a BytesIO stream."""
        ...

    def create_folder(self, folder_path: str) -> str | None:
        """Create a folder structure. Returns folder ID/path if created, None if already exists."""
        ...

    def file_exists(self, file_path: str) -> bool:
        """Check if a file exists."""
        ...

    def write_csv(self, file_path: str, rows: Iterable[Iterable[str]], overwrite: bool = False) -> None:
        """Write CSV data to storage."""
        ...

    def write_metadata(self, csv_file_path: str, metadata: dict) -> None:
        """Write JSON metadata alongside a CSV file."""
        ...

    def list_files(self, folder_path: str | None = None, pattern: str = "*", recursive: bool = False) -> list[dict]:
        """
        List files in a directory.

        Returns list of dicts with 'id', 'name', 'path', and parent information.
        Note: For GDriveClient, this method is not part of the standard interface but is used by utility scripts.
        """
        ...

    def rename_file(self, file_id_or_path: str, new_name: str) -> None:
        """
        Rename a file.

        Args:
            file_id_or_path: For GDrive: file ID, For Local: file path
            new_name: New name for the file
        """
        ...

    def move_file(self, file_id_or_path: str, old_parent_or_new_path: str, new_parent_id: str | None = None) -> None:
        """
        Move a file to a different location.

        Args:
            file_id_or_path: For GDrive: file ID, For Local: file path
            old_parent_or_new_path: For GDrive: old parent ID, For Local: new parent path
            new_parent_id: For GDrive: new parent ID, For Local: not used (None)
        """
        ...

    def trash_file(self, file_id_or_path: str) -> None:
        """
        Move a file to trash.

        Args:
            file_id_or_path: For GDrive: file ID, For Local: file path
        """


def get_storage_client(local_dir: str | None) -> StorageClient:
    """
    Get a storage client for file operations.

    Args:
        local_dir: If provided, returns a LocalStorageClient using this as the base directory.
                   All file paths will be relative to this directory.
                   If None, returns a GDriveClient that automatically scopes all operations
                   to a root 'data' folder in Google Drive.

    Returns:
        StorageClient: Either a LocalStorageClient or GDriveClient

    Examples:
        # Use Google Drive (all paths scoped under 'data' folder automatically)
        client = get_storage_client(None)
        client.write_csv('pvoutput/site123.csv', rows)  # Creates data/pvoutput/site123.csv

        # Use local storage
        client = get_storage_client('/tmp/my_data')
        client.write_csv('pvoutput/site123.csv', rows)  # Creates /tmp/my_data/pvoutput/site123.csv
    """
    if local_dir:
        print(f"Using local storage at {local_dir}")
        return LocalStorageClient(local_dir)
    else:
        print("Using Google Drive storage")
        return GDriveClient.build_service()
