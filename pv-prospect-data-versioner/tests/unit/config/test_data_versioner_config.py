import pytest
from pv_prospect.data_versioner.config import DataVersionerConfig
from pv_prospect.etl.storage.backends import GcsStorageConfig, LocalStorageConfig

_VERSIONER_KEYS = {
    'instance_repo_url': 'git@github.com:rjferrier/pv-prospect-instance.git',
    'instance_repo_branch': 'main',
    'dvc_remote_name': 'feature',
    'prepared_data_dir': 'data/prepared',
}


def test_from_dict_parses_gcs_config() -> None:
    data = {
        'staged_prepared_data_storage': {
            'backend': 'gcs',
            'bucket_name': 'my-bucket',
            'prefix': 'prepared',
        },
        'staged_cleaned_data_storage': {
            'backend': 'gcs',
            'bucket_name': 'my-bucket',
            'prefix': 'cleaned',
        },
        'staged_prepared_batches_data_storage': {
            'backend': 'gcs',
            'bucket_name': 'my-bucket',
            'prefix': 'prepared-batches',
        },
        **_VERSIONER_KEYS,
    }

    config = DataVersionerConfig.from_dict(data)

    assert isinstance(config.staged_prepared_data_storage, GcsStorageConfig)
    assert config.staged_prepared_data_storage.bucket_name == 'my-bucket'
    assert config.staged_prepared_data_storage.prefix == 'prepared'
    assert isinstance(config.staged_cleaned_data_storage, GcsStorageConfig)
    assert config.staged_cleaned_data_storage.prefix == 'cleaned'
    assert isinstance(config.staged_prepared_batches_data_storage, GcsStorageConfig)
    assert config.staged_prepared_batches_data_storage.prefix == 'prepared-batches'


def test_from_dict_parses_local_config() -> None:
    data = {
        'staged_prepared_data_storage': {
            'backend': 'local',
            'prefix': '/tmp/prepared',
        },
        'staged_cleaned_data_storage': {
            'backend': 'local',
            'prefix': '/tmp/cleaned',
        },
        'staged_prepared_batches_data_storage': {
            'backend': 'local',
            'prefix': '/tmp/prepared-batches',
        },
        **_VERSIONER_KEYS,
    }

    config = DataVersionerConfig.from_dict(data)

    assert isinstance(config.staged_prepared_data_storage, LocalStorageConfig)
    assert config.staged_prepared_data_storage.prefix == '/tmp/prepared'
    assert isinstance(config.staged_cleaned_data_storage, LocalStorageConfig)
    assert config.staged_cleaned_data_storage.prefix == '/tmp/cleaned'


def test_from_dict_parses_versioner_keys() -> None:
    data = {
        'staged_prepared_data_storage': {'backend': 'local', 'prefix': '/tmp'},
        'staged_cleaned_data_storage': {'backend': 'local', 'prefix': '/tmp'},
        'staged_prepared_batches_data_storage': {'backend': 'local', 'prefix': '/tmp'},
        **_VERSIONER_KEYS,
    }

    config = DataVersionerConfig.from_dict(data)

    assert (
        config.instance_repo_url == 'git@github.com:rjferrier/pv-prospect-instance.git'
    )
    assert config.instance_repo_branch == 'main'
    assert config.dvc_remote_name == 'feature'
    assert config.prepared_data_dir == 'data/prepared'


def test_from_dict_uses_defaults_for_optional_keys() -> None:
    data = {
        'staged_prepared_data_storage': {'backend': 'local', 'prefix': '/tmp'},
        'staged_cleaned_data_storage': {'backend': 'local', 'prefix': '/tmp'},
        'staged_prepared_batches_data_storage': {'backend': 'local', 'prefix': '/tmp'},
        'instance_repo_url': 'git@github.com:rjferrier/pv-prospect-instance.git',
    }

    config = DataVersionerConfig.from_dict(data)

    assert config.instance_repo_branch == 'main'
    assert config.dvc_remote_name == 'feature'
    assert config.prepared_data_dir == 'data/prepared'


def test_from_dict_raises_on_missing_key() -> None:
    data = {
        'staged_prepared_data_storage': {'backend': 'local', 'prefix': '/tmp'},
    }

    with pytest.raises(KeyError):
        DataVersionerConfig.from_dict(data)
