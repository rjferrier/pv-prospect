from pv_prospect.app.store import storage_config_for
from pv_prospect.etl.storage.backends import GcsStorageConfig, LocalStorageConfig


def test_gs_uri_with_nested_prefix() -> None:
    config = storage_config_for('gs://my-bucket/path/to/prefix')
    assert isinstance(config, GcsStorageConfig)
    assert config.bucket_name == 'my-bucket'
    assert config.prefix == 'path/to/prefix'


def test_gs_uri_no_prefix() -> None:
    config = storage_config_for('gs://my-bucket')
    assert isinstance(config, GcsStorageConfig)
    assert config.bucket_name == 'my-bucket'
    assert config.prefix == ''


def test_local_path() -> None:
    config = storage_config_for('/some/local/path')
    assert isinstance(config, LocalStorageConfig)
    assert config.prefix == '/some/local/path'
