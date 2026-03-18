import pytest
from pv_prospect.data_transformation.config import DataTransformationConfig
from pv_prospect.etl.storage.backends import GcsStorageConfig, LocalStorageConfig


def test_from_dict_parses_gcs_config():
    data = {
        'staged_raw_data_storage': {
            'backend': 'gcs',
            'bucket_name': 'my-raw-bucket',
        },
        'staged_model_data_storage': {
            'backend': 'gcs',
            'bucket_name': 'my-model-bucket',
        },
    }

    config = DataTransformationConfig.from_dict(data)

    assert isinstance(config.staged_raw_data_storage, GcsStorageConfig)
    assert config.staged_raw_data_storage.bucket_name == 'my-raw-bucket'
    assert isinstance(config.staged_model_data_storage, GcsStorageConfig)
    assert config.staged_model_data_storage.bucket_name == 'my-model-bucket'


def test_from_dict_parses_local_config():
    data = {
        'staged_raw_data_storage': {
            'backend': 'local',
            'prefix': '/tmp/raw',
        },
        'staged_model_data_storage': {
            'backend': 'local',
            'prefix': '/tmp/model',
        },
    }

    config = DataTransformationConfig.from_dict(data)

    assert isinstance(config.staged_raw_data_storage, LocalStorageConfig)
    assert config.staged_raw_data_storage.prefix == '/tmp/raw'
    assert isinstance(config.staged_model_data_storage, LocalStorageConfig)
    assert config.staged_model_data_storage.prefix == '/tmp/model'


def test_from_dict_raises_on_missing_key():
    data = {
        'staged_raw_data_storage': {
            'backend': 'gcs',
            'bucket_name': 'my-raw-bucket',
        },
    }

    with pytest.raises(KeyError):
        DataTransformationConfig.from_dict(data)
