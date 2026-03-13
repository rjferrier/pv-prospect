import csv
import io
from typing import Iterable

from pv_prospect.etl.storage.base import FileSystem


class Loader:
    """Writes files to storage via an injected FileSystem."""

    def __init__(self, fs: FileSystem) -> None:
        self._fs = fs

    def create_folder(self, folder_path: str) -> str:
        self._fs.mkdir(folder_path)
        return folder_path

    def write_csv(
        self, file_path: str, rows: Iterable[Iterable[str]], overwrite: bool = False
    ) -> None:
        if not overwrite and self._fs.exists(file_path):
            raise FileExistsError(f'File already exists: {file_path}')
        buf = io.StringIO()
        writer = csv.writer(buf)
        for row in rows:
            writer.writerow(row)
        self._fs.write_text(file_path, buf.getvalue())

    def write_text(self, file_path: str, text: str, overwrite: bool = False) -> None:
        if not overwrite and self._fs.exists(file_path):
            raise FileExistsError(f'File already exists: {file_path}')
        self._fs.write_text(file_path, text)

    def file_exists(self, file_path: str) -> bool:
        return self._fs.exists(file_path)
