"""Configuration for the data transformation pipeline."""

from dataclasses import dataclass
from typing import Any, Dict

from pv_prospect.etl.storage.factory import AnyStorageConfig, parse_storage_config


@dataclass
class DataTransformationConfig:
    """Configuration for data transformation processing."""

    staged_raw_data_storage: AnyStorageConfig
    staged_model_data_storage: AnyStorageConfig
    intermediate_data_storage: AnyStorageConfig

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataTransformationConfig':
        return cls(
            staged_raw_data_storage=parse_storage_config(
                data['staged_raw_data_storage']
            ),
            staged_model_data_storage=parse_storage_config(
                data['staged_model_data_storage']
            ),
            intermediate_data_storage=parse_storage_config(
                data['intermediate_data_storage']
            ),
        )
