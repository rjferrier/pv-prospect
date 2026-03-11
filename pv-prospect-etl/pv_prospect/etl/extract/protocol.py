import io
from typing import Protocol


class Extractor(Protocol):
    def read_file(self, file_path: str) -> io.TextIOWrapper:
        """Read a file and return its contents as a text stream."""
        ...

    def file_exists(self, file_path: str) -> bool:
        """Check if a file exists."""
        ...

    def list_files(self, folder_path: str | None = None, pattern: str = "*", recursive: bool = False) -> list[dict]:
        """
        List files in a directory.
        Returns list of dicts with 'id', 'name', 'path', and parent information.
        """
        ...
