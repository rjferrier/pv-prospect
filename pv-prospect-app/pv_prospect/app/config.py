"""Configuration for the Prediction API."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class AppConfig:
    store_dir: str
    validation_window_dir: str
    resources_dir: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AppConfig':
        # Env vars override YAML config — used by Cloud Run to point at GCS paths
        # without rebundling config.
        store_dir = os.environ.get('STORE_DIR') or data['store_dir']
        validation_window_dir = (
            os.environ.get('VALIDATION_WINDOW_DIR') or data['validation_window_dir']
        )
        resources_dir = os.environ.get('RESOURCES_DIR') or data['resources_dir']
        return cls(
            store_dir=store_dir,
            validation_window_dir=validation_window_dir,
            resources_dir=resources_dir,
        )
