"""Configuration management for the ETL pipeline."""

from dataclasses import dataclass
from typing import Any, Dict

from pv_prospect.data_sources import DataSourcesConfig
from pv_prospect.etl.storage.factory import AnyStorageConfig, parse_storage_config


@dataclass
class TaskQueueConfig:
    """Configuration for tasks queue."""

    task_spacing: float
    task_jitter: float
    join_timeout: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskQueueConfig':
        return cls(
            task_spacing=data['task_spacing'],
            task_jitter=data['task_jitter'],
            join_timeout=data['join_timeout'],
        )


@dataclass
class OpenMeteoConfig:
    """Configuration for the OpenMeteo extractor."""

    max_model_variables: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OpenMeteoConfig':
        return cls(max_model_variables=data['max_model_variables'])


@dataclass
class DataExtractionConfig:
    """Configuration for ETL processing behavior."""

    resources_storage: AnyStorageConfig
    staged_raw_data_storage: AnyStorageConfig
    task_queue: TaskQueueConfig
    data_sources: DataSourcesConfig
    openmeteo: OpenMeteoConfig
    manifests_storage: AnyStorageConfig | None = None
    cursors_storage: AnyStorageConfig | None = None
    ledger_storage: AnyStorageConfig | None = None
    log_storage: AnyStorageConfig | None = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataExtractionConfig':
        task_queue_config = TaskQueueConfig.from_dict(data.get('task_queue', {}))
        return cls(
            task_queue=task_queue_config,
            resources_storage=parse_storage_config(data['resources_storage']),
            staged_raw_data_storage=parse_storage_config(
                data['staged_raw_data_storage']
            ),
            data_sources=DataSourcesConfig.from_dict(data),
            openmeteo=OpenMeteoConfig.from_dict(data['openmeteo']),
            manifests_storage=_optional_storage(data, 'manifests_storage'),
            cursors_storage=_optional_storage(data, 'cursors_storage'),
            ledger_storage=_optional_storage(data, 'ledger_storage'),
            log_storage=_optional_storage(data, 'log_storage'),
        )


def _optional_storage(data: Dict[str, Any], key: str) -> AnyStorageConfig | None:
    value = data.get(key)
    return parse_storage_config(value) if value else None
