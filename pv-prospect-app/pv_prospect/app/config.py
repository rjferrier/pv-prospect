"""Configuration for the Prediction API."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class AppConfig:
    store_dir: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AppConfig':
        # STORE_DIR env var overrides the YAML config — used by the Cloud Run
        # Service to point at the GCS model bucket without rebundling config.
        store_dir = os.environ.get('STORE_DIR') or data['store_dir']
        return cls(store_dir=store_dir)
