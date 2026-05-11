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
    manifests_storage: AnyStorageConfig | None = None
    cursors_storage: AnyStorageConfig | None = None
    ledger_storage: AnyStorageConfig | None = None
    log_storage: AnyStorageConfig | None = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataTransformationConfig':
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
            manifests_storage=_optional_storage(data, 'manifests_storage'),
            cursors_storage=_optional_storage(data, 'cursors_storage'),
            ledger_storage=_optional_storage(data, 'ledger_storage'),
            log_storage=_optional_storage(data, 'log_storage'),
        )


def _optional_storage(data: Dict[str, Any], key: str) -> AnyStorageConfig | None:
    value = data.get(key)
    return parse_storage_config(value) if value else None
