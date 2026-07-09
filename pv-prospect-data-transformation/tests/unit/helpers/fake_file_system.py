"""In-memory FileSystem for unit testing."""

import fnmatch

from pv_prospect.etl.storage.base import FileEntry


class FakeFileSystem:
    """In-memory FileSystem for unit testing.

    A real backend round-trips a path written with ``write_text`` through
    ``read_bytes`` (and vice versa) — both hit the same bytes. This fake
    keeps the constructor's ``files`` / ``binary_files`` as separate dicts
    for test convenience, but every read falls back to the other dict and
    every write evicts any stale entry from it, so a written file is
    visible to ``read_text``, ``read_bytes``, ``exists``, and
    ``list_files`` alike.
    """

    def __init__(
        self,
        files: dict[str, str] | None = None,
        binary_files: dict[str, bytes] | None = None,
    ) -> None:
        self._files = dict(files or {})
        self._binary_files = dict(binary_files or {})

    def exists(self, path: str) -> bool:
        return path in self._files or path in self._binary_files

    def read_text(self, path: str) -> str:
        if path in self._files:
            return self._files[path]
        if path in self._binary_files:
            return self._binary_files[path].decode('utf-8')
        raise FileNotFoundError(path)

    def write_text(self, path: str, content: str) -> None:
        self._binary_files.pop(path, None)
        self._files[path] = content

    def read_bytes(self, path: str) -> bytes:
        if path in self._binary_files:
            return self._binary_files[path]
        if path in self._files:
            return self._files[path].encode('utf-8')
        raise FileNotFoundError(path)

    def write_bytes(self, path: str, content: bytes) -> None:
        self._files.pop(path, None)
        self._binary_files[path] = content

    def delete(self, path: str) -> None:
        self._files.pop(path, None)
        self._binary_files.pop(path, None)

    def mkdir(self, path: str) -> None:
        pass

    def list_files(
        self,
        prefix: str,
        pattern: str = '*',
        recursive: bool = False,
        start_offset: str = '',
    ) -> list[FileEntry]:
        results = []
        all_paths = list(self._files) + [
            path for path in self._binary_files if path not in self._files
        ]
        for path in all_paths:
            if prefix and not path.startswith(prefix + '/') and path != prefix:
                continue
            if start_offset and path < start_offset:
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
