"""Exposes the path to bundled model-trainer configuration files."""

from importlib.resources import files
from pathlib import Path


def get_config_dir() -> Path:
    """Return the path to the model-trainer package's bundled resources directory."""
    return Path(str(files('pv_prospect.model_trainer').joinpath('resources')))
