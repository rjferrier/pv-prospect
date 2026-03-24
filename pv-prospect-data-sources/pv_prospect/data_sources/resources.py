"""Exposes the path to bundled data-sources configuration files."""

from importlib.resources import files
from pathlib import Path


def get_config_dir() -> Path:
    """Return the path to the data-sources package's bundled resources directory."""
    return Path(str(files('pv_prospect.data_sources').joinpath('resources')))
