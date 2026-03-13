import io
from typing import TextIO

from pv_prospect.etl.storage.base import FileEntry, FileSystem


class Extractor:
    """Reads files from storage via an injected FileSystem."""

    def __init__(self, fs: FileSystem) -> None:
        self._fs = fs

    def read_file(self, file_path: str) -> TextIO:
        text = self._fs.read_text(file_path)
        return io.StringIO(text)

    def file_exists(self, file_path: str) -> bool:
        return self._fs.exists(file_path)

    def list_files(
        self,
        folder_path: str | None = None,
        pattern: str = '*',
        recursive: bool = False,
    ) -> list[FileEntry]:
        return self._fs.list_files(folder_path or '', pattern, recursive)
