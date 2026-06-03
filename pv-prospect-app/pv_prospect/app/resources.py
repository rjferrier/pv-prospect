"""Exposes the path to bundled app configuration files."""

from importlib.resources import files
from pathlib import Path


def get_config_dir() -> Path:
    return Path(str(files('pv_prospect.app').joinpath('resources')))
