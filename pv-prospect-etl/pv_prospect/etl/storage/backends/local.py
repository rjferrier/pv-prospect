from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from pv_prospect.etl.storage.base import FileEntry, StorageConfig


@dataclass
class LocalStorageConfig(StorageConfig):
    """Local storage configuration."""

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], tracking: 'StorageConfig | None' = None
    ) -> 'LocalStorageConfig':
        return cls(prefix=data['prefix'], tracking=tracking)


class LocalFileSystem:
    """Thin I/O adapter for the local filesystem."""

    def __init__(self, base_dir: str) -> None:
        self._base_dir = Path(base_dir).resolve()
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def exists(self, path: str) -> bool:
        return (self._base_dir / path).exists()

    def read_text(self, path: str) -> str:
        full_path = self._base_dir / path
        if not full_path.exists():
            raise FileNotFoundError(f'File not found: {full_path}')
        return full_path.read_text(encoding='utf-8')

    def write_text(self, path: str, content: str) -> None:
        full_path = self._base_dir / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(content, encoding='utf-8')

    def mkdir(self, path: str) -> None:
        (self._base_dir / path).mkdir(parents=True, exist_ok=True)

    def list_files(
        self, prefix: str, pattern: str = '*', recursive: bool = False
    ) -> list[FileEntry]:
        search_dir = self._base_dir / prefix if prefix else self._base_dir
        if not search_dir.exists():
            return []

        glob_pattern = f'**/{pattern}' if recursive else pattern
        files = []
        for file_path in search_dir.glob(glob_pattern):
            if file_path.is_file():
                relative_path = file_path.relative_to(self._base_dir)
                parent_path = (
                    str(relative_path.parent)
                    if relative_path.parent != Path('.')
                    else ''
                )
                files.append(
                    FileEntry(
                        id=str(relative_path),
                        name=file_path.name,
                        path=str(relative_path),
                        parent_path=parent_path,
                        created_time=file_path.stat().st_ctime,
                    )
                )
        return files
