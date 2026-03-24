"""Exposes the path to bundled data-extraction configuration files."""

from importlib.resources import files
from pathlib import Path


def get_config_dir() -> Path:
    """Return the path to the data-extraction package's bundled resources directory."""
    return Path(str(files('pv_prospect.data_extraction').joinpath('resources')))
