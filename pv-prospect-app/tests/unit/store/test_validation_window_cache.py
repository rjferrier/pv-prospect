import json
import textwrap

from pv_prospect.app.store import WINDOW_CSV, WINDOW_MANIFEST, ValidationWindowCache
from pv_prospect.etl.storage.backends.local import LocalFileSystem

_WINDOW_CSV = textwrap.dedent("""\
    system_id,time,temperature,plane_of_array_irradiance,power,power_max
    12345,2025-01-01,5.0,100.0,200.0,400.0
    12345,2025-01-02,6.0,110.0,210.0,420.0
""")


def _write_artifacts(fs: LocalFileSystem, updated_at: str) -> None:
    fs.write_text(WINDOW_MANIFEST, json.dumps({'updated_at': updated_at, 'rows': 2}))
    fs.write_text(WINDOW_CSV, _WINDOW_CSV)


def test_load_then_current_returns_store(tmp_path) -> None:  # type: ignore[no-untyped-def]
    fs = LocalFileSystem(str(tmp_path))
    _write_artifacts(fs, '2025-01-02T00:00:00')
    cache = ValidationWindowCache(fs)
    cache.load()
    store = cache.current()
    assert store is not None
    assert 12345 in store.system_ids


def test_current_reloads_when_updated_at_changes(tmp_path) -> None:  # type: ignore[no-untyped-def]
    fs = LocalFileSystem(str(tmp_path))
    _write_artifacts(fs, '2025-01-02T00:00:00')
    cache = ValidationWindowCache(fs)
    cache.load()
    first = cache.current()

    _write_artifacts(fs, '2025-01-03T00:00:00')
    second = cache.current()

    assert second is not first
    assert second is not None
    assert second.updated_at == '2025-01-03T00:00:00'


def test_current_returns_same_object_when_unchanged(tmp_path) -> None:  # type: ignore[no-untyped-def]
    fs = LocalFileSystem(str(tmp_path))
    _write_artifacts(fs, '2025-01-02T00:00:00')
    cache = ValidationWindowCache(fs)
    cache.load()
    assert cache.current() is cache.current()


def test_current_attempts_load_when_nothing_cached(tmp_path) -> None:  # type: ignore[no-untyped-def]
    fs = LocalFileSystem(str(tmp_path))
    _write_artifacts(fs, '2025-01-02T00:00:00')
    cache = ValidationWindowCache(fs)
    store = cache.current()
    assert store is not None
    assert 12345 in store.system_ids


def test_current_returns_none_when_artifacts_absent(tmp_path) -> None:  # type: ignore[no-untyped-def]
    fs = LocalFileSystem(str(tmp_path))
    cache = ValidationWindowCache(fs)
    assert cache.current() is None
