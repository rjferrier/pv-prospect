"""Configuration management for the ETL pipeline."""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class EtlConfig:
    """Configuration for ETL processing behavior."""
    task_spacing: float
    task_jitter: float
    join_timeout: int

    @classmethod
    def from_yaml(cls, config_path: Optional[Path] = None) -> 'EtlConfig':
        """
        Load configuration from a YAML file.

        Args:
            config_path: Path to the YAML config file. If None, uses default location.

        Returns:
            EtlConfig instance with values from the YAML file.

        Raises:
            FileNotFoundError: If the config file doesn't exist.
            ValueError: If required configuration values are missing.
        """
        if config_path is None:
            # Default to resources/etl_config.yaml
            config_path = Path(__file__).parent / 'resources' / 'etl_config.yaml'

        if not config_path.exists():
            raise FileNotFoundError(
                f"ETL configuration file not found at {config_path}. "
                f"Please create the file with required settings: "
                f"task_spacing, task_jitter, join_timeout"
            )

        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)

        if not data:
            raise ValueError(f"ETL configuration file at {config_path} is empty")

        # Environment variables override YAML values
        task_spacing = cls._get_float_env('ETL_TASK_SPACING', data.get('task_spacing'))
        task_jitter = cls._get_float_env('ETL_TASK_JITTER', data.get('task_jitter'))
        join_timeout = cls._get_int_env('ETL_JOIN_TIMEOUT', data.get('join_timeout'))

        # Validate that all required fields are present
        missing = []
        if task_spacing is None:
            missing.append('task_spacing')
        if task_jitter is None:
            missing.append('task_jitter')
        if join_timeout is None:
            missing.append('join_timeout')

        if missing:
            raise ValueError(
                f"Missing required configuration values in {config_path}: {', '.join(missing)}"
            )

        return cls(
            task_spacing=task_spacing,
            task_jitter=task_jitter,
            join_timeout=join_timeout,
        )

    @staticmethod
    def _get_bool_env(key: str, default: Optional[bool]) -> Optional[bool]:
        """Get boolean from environment variable, falling back to default."""
        value = os.getenv(key)
        if value is None:
            return default
        return value.lower() in ('true', '1', 'yes', 'on')

    @staticmethod
    def _get_float_env(key: str, default: Optional[float]) -> Optional[float]:
        """Get float from environment variable, falling back to default."""
        value = os.getenv(key)
        if value is None:
            return default
        try:
            return float(value)
        except ValueError:
            return default

    @staticmethod
    def _get_int_env(key: str, default: Optional[int]) -> Optional[int]:
        """Get int from environment variable, falling back to default."""
        value = os.getenv(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default
