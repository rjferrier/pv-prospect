from dataclasses import dataclass
from typing import Any, Dict, Union, Optional
from enum import Enum


class Backend(Enum):
    LOCAL = 'local'
    GCS = 'gcs'


@dataclass
class StorageConfig:
    tracking: Optional['AnyStorageConfig']


@dataclass
class LocalStorageConfig(StorageConfig):
    """Local storage configuration."""
    base_dir: str = ''

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LocalStorageConfig':
        tracking = parse_storage_config(data.get('tracking')) if data.get('tracking') else None
        return cls(
            base_dir=data['base_dir'],
            tracking=tracking
        )


@dataclass
class GcsStorageConfig(StorageConfig):
    """GCS storage configuration."""
    bucket_name: str = ''
    prefix: str = ''

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GcsStorageConfig':
        tracking = parse_storage_config(data.get('tracking')) if data.get('tracking') else None
        return cls(
            bucket_name=data['bucket_name'],
            prefix=data.get('prefix', ''),
            tracking=tracking
        )


AnyStorageConfig = Union[LocalStorageConfig, GcsStorageConfig]

def parse_storage_config(data: Dict[str, Any]) -> AnyStorageConfig:
    backend_str = data.get('backend')
    if not backend_str:
        raise ValueError("Missing 'backend' key in storage configuration")
    
    try:
        backend = Backend(backend_str.lower())
    except ValueError:
        raise ValueError(f"Invalid storage backend: {backend_str}")

    if backend == Backend.LOCAL:
        return LocalStorageConfig.from_dict(data)
    elif backend == Backend.GCS:
        return GcsStorageConfig.from_dict(data)

    raise ValueError(f"Unsupported storage backend: {backend}")
