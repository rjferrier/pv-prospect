"""Configuration for the Prediction API."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict

# Rate-limit defaults. These mirror the code defaults in rate_limit.py (used on
# the no-lifespan/test path) and are the canonical values in config-default.yaml.
_DEFAULT_RATE_LIMIT_ENABLED = True
_DEFAULT_RATE_LIMIT_PREDICT = '20/minute'
_DEFAULT_RATE_LIMIT_VALIDATE = '30/minute'
_DEFAULT_RATE_LIMIT_TRUSTED_HOPS = 1


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ('1', 'true', 'yes', 'on')


@dataclass
class AppConfig:
    store_dir: str
    validation_window_dir: str
    resources_dir: str
    rate_limit_enabled: bool = _DEFAULT_RATE_LIMIT_ENABLED
    rate_limit_predict: str = _DEFAULT_RATE_LIMIT_PREDICT
    rate_limit_validate: str = _DEFAULT_RATE_LIMIT_VALIDATE
    rate_limit_trusted_hops: int = _DEFAULT_RATE_LIMIT_TRUSTED_HOPS

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AppConfig':
        # Env vars override YAML config — used by Cloud Run to point at GCS paths
        # (and tune rate limits) without rebundling config.
        store_dir = os.environ.get('STORE_DIR') or data['store_dir']
        validation_window_dir = (
            os.environ.get('VALIDATION_WINDOW_DIR') or data['validation_window_dir']
        )
        resources_dir = os.environ.get('RESOURCES_DIR') or data['resources_dir']
        rate_limit_enabled = _env_bool(
            'RATE_LIMIT_ENABLED',
            data.get('rate_limit_enabled', _DEFAULT_RATE_LIMIT_ENABLED),
        )
        rate_limit_predict = os.environ.get('RATE_LIMIT_PREDICT') or data.get(
            'rate_limit_predict', _DEFAULT_RATE_LIMIT_PREDICT
        )
        rate_limit_validate = os.environ.get('RATE_LIMIT_VALIDATE') or data.get(
            'rate_limit_validate', _DEFAULT_RATE_LIMIT_VALIDATE
        )
        rate_limit_trusted_hops = int(
            os.environ.get('RATE_LIMIT_TRUSTED_HOPS')
            or data.get('rate_limit_trusted_hops', _DEFAULT_RATE_LIMIT_TRUSTED_HOPS)
        )
        return cls(
            store_dir=store_dir,
            validation_window_dir=validation_window_dir,
            resources_dir=resources_dir,
            rate_limit_enabled=rate_limit_enabled,
            rate_limit_predict=rate_limit_predict,
            rate_limit_validate=rate_limit_validate,
            rate_limit_trusted_hops=rate_limit_trusted_hops,
        )
