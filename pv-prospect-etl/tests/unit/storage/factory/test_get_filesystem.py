"""Tests for get_filesystem."""

import pytest
from pv_prospect.etl.storage import get_filesystem
from pv_prospect.etl.storage.backends import LocalFileSystem, LocalStorageConfig


def test_returns_local_file_system_for_local_config(tmp_path):
    config = LocalStorageConfig(prefix=str(tmp_path))

    fs = get_filesystem(config)

    assert isinstance(fs, LocalFileSystem)


def test_raises_for_unknown_config_type():
    class UnknownConfig:
        pass

    with pytest.raises(NotImplementedError):
        get_filesystem(UnknownConfig())  # type: ignore[arg-type]
