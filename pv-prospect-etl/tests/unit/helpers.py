"""Shared test helpers for etl tests."""

import fnmatch

from pv_prospect.etl.storage.base import FileEntry


class FakeFileSystem:
    """In-memory FileSystem for unit testing."""

    def __init__(
        self,
        files: dict[str, str] | None = None,
        binary_files: dict[str, bytes] | None = None,
    ) -> None:
        self._files = dict(files or {})
        self._binary_files = dict(binary_files or {})
        self.created_dirs: list[str] = []

    def exists(self, path: str) -> bool:
        return path in self._files or path in self._binary_files

    def read_text(self, path: str) -> str:
        if path not in self._files:
            raise FileNotFoundError(path)
        return self._files[path]

    def write_text(self, path: str, content: str) -> None:
        self._files[path] = content

    def read_bytes(self, path: str) -> bytes:
        if path not in self._binary_files:
            raise FileNotFoundError(path)
        return self._binary_files[path]

    def write_bytes(self, path: str, content: bytes) -> None:
        self._binary_files[path] = content

    def delete(self, path: str) -> None:
        self._files.pop(path, None)
        self._binary_files.pop(path, None)

    def mkdir(self, path: str) -> None:
        self.created_dirs.append(path)

    def rmdir(self, path: str) -> None:
        if path in self.created_dirs:
            self.created_dirs.remove(path)

    def list_files(
        self, prefix: str, pattern: str = '*', recursive: bool = False
    ) -> list[FileEntry]:
        results = []
        for path in self._files:
            if prefix and not path.startswith(prefix + '/') and path != prefix:
                continue
            name = path.split('/')[-1]
            if pattern != '*' and not fnmatch.fnmatch(name, pattern):
                continue
            parent = '/'.join(path.split('/')[:-1])
            results.append(
                FileEntry(
                    id=path,
                    name=name,
                    path=path,
                    parent_path=parent,
                )
            )
        return results
