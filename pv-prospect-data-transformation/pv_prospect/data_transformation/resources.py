"""Exposes the path to bundled data-transformation configuration files."""

from importlib.resources import files
from pathlib import Path


def get_config_dir() -> Path:
    """Return the path to the data-transformation package's bundled resources directory."""
    return Path(str(files('pv_prospect.data_transformation').joinpath('resources')))
