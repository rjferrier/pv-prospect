"""Shared test helpers for etl tests."""

from pv_prospect.etl.storage.base import FileEntry


class FakeFileSystem:
    """In-memory FileSystem for unit testing."""

    def __init__(self, files: dict[str, str] | None = None) -> None:
        self._files = dict(files or {})
        self.created_dirs: list[str] = []

    def exists(self, path: str) -> bool:
        return path in self._files

    def read_text(self, path: str) -> str:
        if path not in self._files:
            raise FileNotFoundError(path)
        return self._files[path]

    def write_text(self, path: str, content: str) -> None:
        self._files[path] = content

    def mkdir(self, path: str) -> None:
        self.created_dirs.append(path)

    def list_files(
        self, prefix: str, pattern: str = '*', recursive: bool = False
    ) -> list[FileEntry]:
        results = []
        for path in self._files:
            if prefix and not path.startswith(prefix + '/') and path != prefix:
                continue
            name = path.split('/')[-1]
            if pattern != '*':
                if pattern.startswith('*.'):
                    if not name.endswith(f'.{pattern[2:]}'):
                        continue
                elif name != pattern:
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
