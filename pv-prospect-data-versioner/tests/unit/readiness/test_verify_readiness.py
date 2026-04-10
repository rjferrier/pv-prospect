import pytest
from pv_prospect.data_versioner.readiness import ReadinessError, verify_readiness
from pv_prospect.etl.storage import FileEntry


class StubFileSystem:
    """Minimal FileSystem stub for testing."""

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self._files: dict[str, bytes] = files or {}

    def __str__(self) -> str:
        return 'StubFileSystem'

    def exists(self, path: str) -> bool:
        return path in self._files

    def read_text(self, path: str) -> str:
        return self._files[path].decode()

    def write_text(self, path: str, content: str) -> None:
        self._files[path] = content.encode()

    def read_bytes(self, path: str) -> bytes:
        return self._files[path]

    def write_bytes(self, path: str, content: bytes) -> None:
        self._files[path] = content

    def mkdir(self, path: str) -> None:
        pass

    def delete(self, path: str) -> None:
        self._files.pop(path, None)

    def list_files(
        self, prefix: str, pattern: str = '*', recursive: bool = False
    ) -> list[FileEntry]:
        from fnmatch import fnmatch

        results = []
        for path in sorted(self._files):
            if not path.startswith(prefix + '/') and path != prefix:
                continue
            name = path.rsplit('/', 1)[-1]
            if fnmatch(name, pattern):
                parent = path.rsplit('/', 1)[0] if '/' in path else ''
                results.append(
                    FileEntry(id=path, name=name, path=path, parent_path=parent)
                )
        return results


def test_returns_file_paths_when_ready() -> None:
    prepared_fs = StubFileSystem(
        {
            'weather.csv': b'data',
            'pv/89665.csv': b'data',
            'pv/12345.csv': b'data',
        }
    )
    batches_fs = StubFileSystem()

    paths = verify_readiness(prepared_fs, batches_fs)

    assert 'weather.csv' in paths
    assert 'pv/89665.csv' in paths
    assert 'pv/12345.csv' in paths
    assert len(paths) == 3


def test_raises_when_weather_csv_missing() -> None:
    prepared_fs = StubFileSystem(
        {
            'pv/89665.csv': b'data',
        }
    )
    batches_fs = StubFileSystem()

    with pytest.raises(ReadinessError, match='weather.csv not found'):
        verify_readiness(prepared_fs, batches_fs)


def test_raises_when_no_pv_files() -> None:
    prepared_fs = StubFileSystem(
        {
            'weather.csv': b'data',
        }
    )
    batches_fs = StubFileSystem()

    with pytest.raises(ReadinessError, match='No CSV files found in pv/'):
        verify_readiness(prepared_fs, batches_fs)


def test_raises_when_unassembled_batches_remain() -> None:
    prepared_fs = StubFileSystem(
        {
            'weather.csv': b'data',
            'pv/89665.csv': b'data',
        }
    )
    batches_fs = StubFileSystem(
        {
            'weather/504900_minus35400_20250624.csv': b'data',
        }
    )

    with pytest.raises(ReadinessError, match='unassembled batch file'):
        verify_readiness(prepared_fs, batches_fs)


def test_raises_with_all_errors_combined() -> None:
    prepared_fs = StubFileSystem()
    batches_fs = StubFileSystem(
        {
            'pv/89665_20250624.csv': b'data',
        }
    )

    with pytest.raises(ReadinessError) as exc_info:
        verify_readiness(prepared_fs, batches_fs)

    message = str(exc_info.value)
    assert 'weather.csv not found' in message
    assert 'No CSV files found' in message
    assert 'unassembled batch' in message
