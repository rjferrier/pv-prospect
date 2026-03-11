import io
from pathlib import Path

from pv_prospect.etl.clients.local import LocalClient


class LocalExtractor(LocalClient):
    """Client for reading files from a local directory."""

    def read_file(self, file_path: str) -> io.TextIOWrapper:
        """Read a file from local storage and return it as a text stream."""
        full_path = self.base_dir / file_path

        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")

        return open(full_path, 'r', encoding='utf-8')

    def list_files(self, folder_path: str | None = None, pattern: str = "*", recursive: bool = False) -> list[dict]:
        """List files in a local directory."""
        if folder_path:
            search_dir = self.base_dir / folder_path
        else:
            search_dir = self.base_dir

        if not search_dir.exists():
            return []

        files = []
        glob_pattern = f"**/{pattern}" if recursive else pattern

        for file_path in search_dir.glob(glob_pattern):
            if file_path.is_file():
                relative_path = file_path.relative_to(self.base_dir)
                parent_path = str(relative_path.parent) if relative_path.parent != Path('.') else ''

                files.append({
                    'id': str(relative_path),  # Use relative path as ID
                    'name': file_path.name,
                    'path': str(relative_path),
                    'parent_path': parent_path,
                    'created_time': file_path.stat().st_ctime,
                })

        return files
