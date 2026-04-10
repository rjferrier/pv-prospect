"""Configuration for the data versioning pipeline."""

from dataclasses import dataclass
from typing import Any, Dict

from pv_prospect.etl.storage.factory import AnyStorageConfig, parse_storage_config


@dataclass
class DataVersionerConfig:
    """Configuration for data versioning."""

    staged_prepared_data_storage: AnyStorageConfig
    staged_cleaned_data_storage: AnyStorageConfig
    staged_prepared_batches_data_storage: AnyStorageConfig
    instance_repo_url: str
    instance_repo_branch: str
    dvc_remote_name: str
    prepared_data_dir: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataVersionerConfig':
        return cls(
            staged_prepared_data_storage=parse_storage_config(
                data['staged_prepared_data_storage']
            ),
            staged_cleaned_data_storage=parse_storage_config(
                data['staged_cleaned_data_storage']
            ),
            staged_prepared_batches_data_storage=parse_storage_config(
                data['staged_prepared_batches_data_storage']
            ),
            instance_repo_url=data['instance_repo_url'],
            instance_repo_branch=data.get('instance_repo_branch', 'main'),
            dvc_remote_name=data.get('dvc_remote_name', 'feature'),
            prepared_data_dir=data.get('prepared_data_dir', 'data/prepared'),
        )
