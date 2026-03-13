"""Tests for GcsStorageConfig."""

from pv_prospect.etl.storage.backends import GcsStorageConfig, LocalStorageConfig


def test_from_dict_creates_config_with_bucket_and_prefix() -> None:
    data = {'bucket_name': 'my-bucket', 'prefix': 'data/raw'}

    config = GcsStorageConfig.from_dict(data)

    assert config.bucket_name == 'my-bucket'
    assert config.prefix == 'data/raw'
    assert config.tracking is None


def test_from_dict_defaults_prefix_to_empty_string() -> None:
    data = {'bucket_name': 'my-bucket'}

    config = GcsStorageConfig.from_dict(data)

    assert config.prefix == ''


def test_from_dict_preserves_tracking() -> None:
    tracking = LocalStorageConfig(prefix='/tmp/tracking')

    config = GcsStorageConfig.from_dict(
        {'bucket_name': 'my-bucket', 'prefix': 'pfx'}, tracking=tracking
    )

    assert config.tracking is tracking


def test_default_bucket_name_is_empty_string() -> None:
    config = GcsStorageConfig()

    assert config.bucket_name == ''
