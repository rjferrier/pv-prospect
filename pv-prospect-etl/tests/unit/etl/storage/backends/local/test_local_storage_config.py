"""Tests for LocalStorageConfig."""

from pv_prospect.etl.storage.backends.local import LocalStorageConfig


def test_from_dict_creates_config_with_prefix() -> None:
    data = {'prefix': '/tmp/data'}

    config = LocalStorageConfig.from_dict(data)

    assert config.prefix == '/tmp/data'
    assert config.tracking is None


def test_from_dict_preserves_tracking() -> None:
    tracking = LocalStorageConfig(prefix='/tmp/tracking')

    config = LocalStorageConfig.from_dict({'prefix': '/tmp/data'}, tracking=tracking)

    assert config.tracking is tracking


def test_default_prefix_is_empty_string() -> None:
    config = LocalStorageConfig()

    assert config.prefix == ''
