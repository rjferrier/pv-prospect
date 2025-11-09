from io import BytesIO
from typing import Protocol, Iterable

from loaders.gdrive import GDriveClient
from loaders.local import LocalStorageClient


class StorageClient(Protocol):
    def read_file(self, file_path: str) -> BytesIO:
        ...
    def create_folder(self, folder_path: str) -> None:
        ...
    def file_exists(self, file_path: str) -> bool:
        ...
    def write_csv(self, file_path: str, rows: Iterable[Iterable[str]], overwrite: bool = False) -> None:
        ...
    def write_metadata(self, csv_file_path: str, metadata: dict) -> None:
        ...


def get_storage_client(local_dir: str | None) -> StorageClient:
    if local_dir:
        return LocalStorageClient(local_dir)
    else:
        return GDriveClient.build_service()
