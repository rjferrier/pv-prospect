from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Union


class Backend(Enum):
    LOCAL = 'local'
    GCS = 'gcs'


@dataclass
class StorageConfig(metaclass=ABCMeta):
    prefix: str = field(default='')
    tracking: Optional['AnyStorageConfig'] = field(default=None)

    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StorageConfig': ...


@dataclass
class LocalStorageConfig(StorageConfig):
    """Local storage configuration."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LocalStorageConfig':
        return cls(
            prefix=data['prefix'], tracking=parse_tracking_config(data.get('tracking'))
        )


@dataclass
class GcsStorageConfig(StorageConfig):
    """GCS storage configuration."""

    bucket_name: str = ''

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GcsStorageConfig':
        return cls(
            bucket_name=data['bucket_name'],
            prefix=data.get('prefix', ''),
            tracking=parse_tracking_config(data.get('tracking')),
        )


AnyStorageConfig = Union[LocalStorageConfig, GcsStorageConfig]


def parse_storage_config(data: Dict[str, Any]) -> AnyStorageConfig:
    backend_str = data.get('backend')
    if not backend_str:
        raise ValueError("Missing 'backend' key in storage configuration")

    try:
        backend = Backend(backend_str.lower())
    except ValueError:
        raise ValueError(f'Invalid storage backend: {backend_str}') from None

    if backend == Backend.LOCAL:
        return LocalStorageConfig.from_dict(data)
    elif backend == Backend.GCS:
        return GcsStorageConfig.from_dict(data)

    raise ValueError(f'Unsupported storage backend: {backend}')


def parse_tracking_config(tracking_data: Any | None) -> AnyStorageConfig | None:
    if not tracking_data:
        return None
    return parse_storage_config(tracking_data)
