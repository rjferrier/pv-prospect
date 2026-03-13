from enum import Enum
from typing import Any, Dict, Union

from pv_prospect.etl.storage.backends import (
    GcsFileSystem,
    GcsStorageConfig,
    LocalFileSystem,
    LocalStorageConfig,
)
from pv_prospect.etl.storage.base import FileSystem


class Backend(Enum):
    LOCAL = 'local'
    GCS = 'gcs'


AnyStorageConfig = Union[LocalStorageConfig, GcsStorageConfig]


def parse_storage_config(data: Dict[str, Any]) -> AnyStorageConfig:
    backend_str = data.get('backend')
    if not backend_str:
        raise ValueError("Missing 'backend' key in storage configuration")

    try:
        backend = Backend(backend_str.lower())
    except ValueError:
        raise ValueError(f'Invalid storage backend: {backend_str}') from None

    storage_config_cls = _get_storage_config_class(backend)
    tracking = parse_tracking_config(data.get('tracking'))
    return storage_config_cls.from_dict(data, tracking)


def get_filesystem(config: AnyStorageConfig) -> FileSystem:
    if isinstance(config, LocalStorageConfig):
        return LocalFileSystem(config.prefix)

    if isinstance(config, GcsStorageConfig):
        return GcsFileSystem(config.bucket_name, config.prefix)

    raise NotImplementedError(f'type of {config} ({type(config)}) is not recognised')


def parse_tracking_config(tracking_data: Any | None) -> AnyStorageConfig | None:
    if not tracking_data:
        return None
    return parse_storage_config(tracking_data)


def _get_storage_config_class(backend: Backend) -> type[AnyStorageConfig]:
    if backend == Backend.LOCAL:
        return LocalStorageConfig
    if backend == Backend.GCS:
        return GcsStorageConfig

    raise ValueError(f'Unsupported storage backend: {backend}')
