from typing import Iterable, Protocol


class Loader(Protocol):
    def create_folder(self, folder_path: str) -> str | None:
        """Create a folder structure. Returns folder ID/path if created, None if already exists."""
        ...

    def write_csv(
        self, file_path: str, rows: Iterable[Iterable[str]], overwrite: bool = False
    ) -> None:
        """Write CSV data to storage."""
        ...

    def write_text(self, file_path: str, text: str, overwrite: bool = False) -> None:
        """Write text data to storage."""
        ...

    def file_exists(self, file_path: str) -> bool:
        """Check if a file exists."""
        ...
