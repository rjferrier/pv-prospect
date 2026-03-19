import pytest
from pv_prospect.data_sources import DataSource, SourceDescriptor
from pv_prospect.data_transformation.config import DataTransformationConfig
from pv_prospect.etl.storage.backends import GcsStorageConfig, LocalStorageConfig

_DATA_SOURCES = {
    'data_sources': {
        'pv': 'pvoutput',
        'weather': 'openmeteo/historical',
    },
}


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
        'intermediate_data_storage': {
            'backend': 'local',
            'prefix': '/tmp',
        },
        **_DATA_SOURCES,
    }

    config = DataTransformationConfig.from_dict(data)

    assert isinstance(config.staged_raw_data_storage, GcsStorageConfig)
    assert config.staged_raw_data_storage.bucket_name == 'my-raw-bucket'
    assert isinstance(config.staged_model_data_storage, GcsStorageConfig)
    assert config.staged_model_data_storage.bucket_name == 'my-model-bucket'
    assert isinstance(config.intermediate_data_storage, LocalStorageConfig)
    assert config.intermediate_data_storage.prefix == '/tmp'


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
        'intermediate_data_storage': {
            'backend': 'local',
            'prefix': '/tmp/intermediate',
        },
        **_DATA_SOURCES,
    }

    config = DataTransformationConfig.from_dict(data)

    assert isinstance(config.staged_raw_data_storage, LocalStorageConfig)
    assert config.staged_raw_data_storage.prefix == '/tmp/raw'
    assert isinstance(config.staged_model_data_storage, LocalStorageConfig)
    assert config.staged_model_data_storage.prefix == '/tmp/model'
    assert isinstance(config.intermediate_data_storage, LocalStorageConfig)
    assert config.intermediate_data_storage.prefix == '/tmp/intermediate'


def test_from_dict_raises_on_missing_key():
    data = {
        'staged_raw_data_storage': {
            'backend': 'gcs',
            'bucket_name': 'my-raw-bucket',
        },
    }

    with pytest.raises(KeyError):
        DataTransformationConfig.from_dict(data)


def test_from_dict_parses_data_sources():
    data = {
        'staged_raw_data_storage': {'backend': 'local', 'prefix': '/tmp'},
        'staged_model_data_storage': {'backend': 'local', 'prefix': '/tmp'},
        'intermediate_data_storage': {'backend': 'local', 'prefix': '/tmp'},
        **_DATA_SOURCES,
    }

    config = DataTransformationConfig.from_dict(data)

    assert (
        config.data_sources.get_descriptor(DataSource.PV) is SourceDescriptor.PVOUTPUT
    )
    assert (
        config.data_sources.get_descriptor(DataSource.WEATHER)
        is SourceDescriptor.OPENMETEO_HISTORICAL
    )
