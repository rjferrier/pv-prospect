"""Exposes the path to bundled ETL configuration files."""

from importlib.resources import files
from pathlib import Path


def get_config_dir() -> Path:
    """Return the path to the etl package's bundled resources directory."""
    return Path(str(files('pv_prospect.etl').joinpath('resources')))
