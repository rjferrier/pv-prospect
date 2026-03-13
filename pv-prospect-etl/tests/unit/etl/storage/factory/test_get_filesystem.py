"""Tests for get_filesystem."""

import pytest
from pv_prospect.etl.storage.backends.local import LocalFileSystem, LocalStorageConfig
from pv_prospect.etl.storage.factory import get_filesystem


def test_returns_local_file_system_for_local_config(tmp_path):
    config = LocalStorageConfig(prefix=str(tmp_path))

    fs = get_filesystem(config)

    assert isinstance(fs, LocalFileSystem)


def test_raises_for_unknown_config_type():
    class UnknownConfig:
        pass

    with pytest.raises(NotImplementedError):
        get_filesystem(UnknownConfig())  # type: ignore[arg-type]
