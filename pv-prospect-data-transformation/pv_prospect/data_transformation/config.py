"""Configuration for the data transformation pipeline."""

from dataclasses import dataclass
from typing import Any, Dict

from pv_prospect.data_sources import DataSourcesConfig
from pv_prospect.etl.storage.factory import AnyStorageConfig, parse_storage_config


@dataclass
class DataTransformationConfig:
    """Configuration for data transformation processing."""

    resources_storage: AnyStorageConfig
    staged_raw_data_storage: AnyStorageConfig
    staged_cleaned_data_storage: AnyStorageConfig
    staged_prepared_batches_data_storage: AnyStorageConfig
    staged_prepared_data_storage: AnyStorageConfig
    data_sources: DataSourcesConfig
    log_storage: AnyStorageConfig | None = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataTransformationConfig':
        log_data = data.get('log_storage')
        return cls(
            resources_storage=parse_storage_config(data['resources_storage']),
            staged_raw_data_storage=parse_storage_config(
                data['staged_raw_data_storage']
            ),
            staged_cleaned_data_storage=parse_storage_config(
                data['staged_cleaned_data_storage']
            ),
            staged_prepared_batches_data_storage=parse_storage_config(
                data['staged_prepared_batches_data_storage']
            ),
            staged_prepared_data_storage=parse_storage_config(
                data['staged_prepared_data_storage']
            ),
            data_sources=DataSourcesConfig.from_dict(data),
            log_storage=parse_storage_config(log_data) if log_data else None,
        )
