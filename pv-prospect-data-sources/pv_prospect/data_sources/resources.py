"""Exposes the path to the data-sources configuration files."""

from pathlib import Path

_PACKAGE_ROOT = Path(__file__).resolve().parent.parent.parent


def get_config_dir() -> Path:
    """Return the path to the data-sources package's resources directory."""
    return _PACKAGE_ROOT / 'resources'
