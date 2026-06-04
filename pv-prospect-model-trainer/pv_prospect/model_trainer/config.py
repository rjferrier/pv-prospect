"""Configuration for the model trainer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class ModelTrainerConfig:
    """Configuration for the model trainer bootstrap and scheduled job."""

    instance_repo_url: str
    instance_repo_branch: str
    feature_remote_name: str
    prepared_data_dir: str
    pv_sites_csv_path: str
    model_remote_name: str
    model_dir: str
    model_bucket_name: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelTrainerConfig':
        return cls(
            instance_repo_url=data['instance_repo_url'],
            instance_repo_branch=data.get('instance_repo_branch', 'main'),
            feature_remote_name=data.get('feature_remote_name', 'feature'),
            prepared_data_dir=data.get('prepared_data_dir', 'data/prepared'),
            pv_sites_csv_path=data.get('pv_sites_csv_path', 'data/static/pv_sites.csv'),
            model_remote_name=data.get('model_remote_name', 'model'),
            model_dir=data.get('model_dir', 'models'),
            model_bucket_name=data.get('model_bucket_name', ''),
        )
