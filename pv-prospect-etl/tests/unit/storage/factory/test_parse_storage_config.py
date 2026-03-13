"""Tests for parse_storage_config."""

import pytest
from pv_prospect.etl.storage import parse_storage_config
from pv_prospect.etl.storage.backends import GcsStorageConfig, LocalStorageConfig


def test_parses_local_config() -> None:
    data = {'backend': 'local', 'prefix': '/tmp/data'}

    config = parse_storage_config(data)

    assert isinstance(config, LocalStorageConfig)
    assert config.prefix == '/tmp/data'


def test_parses_gcs_config() -> None:
    data = {'backend': 'gcs', 'bucket_name': 'my-bucket', 'prefix': 'raw'}

    config = parse_storage_config(data)

    assert isinstance(config, GcsStorageConfig)
    assert config.bucket_name == 'my-bucket'
    assert config.prefix == 'raw'


def test_backend_is_case_insensitive() -> None:
    data = {'backend': 'LOCAL', 'prefix': '/tmp'}

    config = parse_storage_config(data)

    assert isinstance(config, LocalStorageConfig)


def test_raises_on_missing_backend() -> None:
    with pytest.raises(ValueError, match='Missing'):
        parse_storage_config({'prefix': '/tmp'})


def test_raises_on_invalid_backend() -> None:
    with pytest.raises(ValueError, match='Invalid storage backend'):
        parse_storage_config({'backend': 'azure'})


def test_parses_nested_tracking_config() -> None:
    data = {
        'backend': 'local',
        'prefix': '/tmp/data',
        'tracking': {
            'backend': 'gcs',
            'bucket_name': 'tracking-bucket',
            'prefix': 'dvc',
        },
    }

    config = parse_storage_config(data)

    assert isinstance(config.tracking, GcsStorageConfig)
    assert config.tracking.bucket_name == 'tracking-bucket'
