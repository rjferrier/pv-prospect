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


_WEATHER = 'weather/weather_2026-05-01_2026-05-15_0-07.csv'
_PV_89665 = 'pv/89665/pv_89665_2026-05-01_2026-05-08.csv'
_PV_12345 = 'pv/12345/pv_12345_2026-05-01_2026-05-08.csv'


def test_returns_sorted_partition_paths_when_ready() -> None:
    prepared_fs = StubFileSystem(
        {_WEATHER: b'data', _PV_89665: b'data', _PV_12345: b'data'}
    )

    paths = verify_readiness(prepared_fs, StubFileSystem())

    assert paths == sorted([_WEATHER, _PV_89665, _PV_12345])


def test_versions_a_single_populated_corpus() -> None:
    """The versioner snapshots whatever partition files are present; a
    cycle with only PV partitions (no new weather) is still ready."""
    prepared_fs = StubFileSystem({_PV_89665: b'data'})

    assert verify_readiness(prepared_fs, StubFileSystem()) == [_PV_89665]


def test_rejects_a_misshapen_weather_partition() -> None:
    prepared_fs = StubFileSystem(
        {_PV_89665: b'data', 'weather/weather_2026-05-01.csv': b'data'}
    )

    with pytest.raises(ReadinessError, match='Unexpected file in weather/'):
        verify_readiness(prepared_fs, StubFileSystem())


def test_rejects_an_old_format_pv_master() -> None:
    """A pre-partition ``pv/{site}.csv`` master sits directly under
    ``pv/`` rather than in a per-site subdirectory, so it fails the shape
    check rather than being snapshotted silently."""
    prepared_fs = StubFileSystem({_WEATHER: b'data', 'pv/89665.csv': b'data'})

    with pytest.raises(ReadinessError, match='Unexpected file in pv/'):
        verify_readiness(prepared_fs, StubFileSystem())


def test_rejects_pv_partition_whose_dir_and_filename_site_disagree() -> None:
    prepared_fs = StubFileSystem(
        {_WEATHER: b'data', 'pv/89665/pv_12345_2026-05-01_2026-05-08.csv': b'data'}
    )

    with pytest.raises(ReadinessError, match='Unexpected file in pv/'):
        verify_readiness(prepared_fs, StubFileSystem())


def test_raises_when_no_partition_files() -> None:
    with pytest.raises(ReadinessError, match='No prepared partition files'):
        verify_readiness(StubFileSystem(), StubFileSystem())


def test_raises_when_unassembled_batches_remain() -> None:
    prepared_fs = StubFileSystem({_WEATHER: b'data', _PV_89665: b'data'})
    batches_fs = StubFileSystem({'pv/89665_20260501.csv': b'data'})

    with pytest.raises(ReadinessError, match='unassembled batch file'):
        verify_readiness(prepared_fs, batches_fs)


def test_accumulates_all_errors() -> None:
    prepared_fs = StubFileSystem({'weather/garbage.csv': b'data'})
    batches_fs = StubFileSystem({'pv/89665_20260501.csv': b'data'})

    with pytest.raises(ReadinessError) as exc_info:
        verify_readiness(prepared_fs, batches_fs)

    message = str(exc_info.value)
    assert 'Unexpected file in weather/' in message
    assert 'unassembled batch' in message
