import csv
import os
from pathlib import Path
from typing import Iterable

from pv_prospect.etl.clients.local import LocalClient


class LocalLoader(LocalClient):
    """Client for storing files in a local directory."""

    def __init__(self, base_dir: str):
        super().__init__(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def create_folder(self, folder_path: str) -> str | None:
        """Create a folder structure in the local storage."""
        full_path = self.base_dir / folder_path

        if full_path.exists():
            return None

        full_path.mkdir(parents=True, exist_ok=True)
        print(f"    Created folder: {full_path}")
        return str(full_path)

    def write_csv(self, file_path: str, rows: Iterable[Iterable[str]], overwrite: bool = False) -> None:
        """Write CSV data to a local file."""
        full_path = self.base_dir / file_path

        full_path.parent.mkdir(parents=True, exist_ok=True)

        if full_path.exists() and not overwrite:
            raise FileExistsError(f"File already exists: {full_path}")

        with open(full_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for row in rows:
                writer.writerow(row)

        print(f"    Written to: {full_path}")

    def write_text(self, file_path: str, text: str, overwrite: bool = False) -> None:
        """Write text data to a local file."""
        full_path = self.base_dir / file_path

        full_path.parent.mkdir(parents=True, exist_ok=True)

        if full_path.exists() and not overwrite:
            raise FileExistsError(f"File already exists: {full_path}")

        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(text)

        print(f"    Written to: {full_path}")
