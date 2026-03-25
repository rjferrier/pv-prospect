import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Dict, Protocol, Type, TypeVar

import yaml  # type: ignore[import-untyped]

DEFAULT_RUNTIME_ENV = 'default'
LOCAL_RUNTIME_ENV = 'local'
RUNTIME_ENV_VAR = 'RUNTIME_ENV'
CONFIG_DIR_VAR = 'CONFIG_DIR'


class Config(Protocol):
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Any: ...


T = TypeVar('T', bound=Config)


def _merge_dicts(d1: Dict[str, Any], d2: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge d2 into d1."""
    for k, v in d2.items():
        if isinstance(v, dict) and k in d1 and isinstance(d1[k], dict):
            _merge_dicts(d1[k], v)
        else:
            d1[k] = v
    return d1


def _load_from_dir(config_dir: Path, runtime_env: str) -> Dict[str, Any]:
    """Load and merge default + env-specific config from a single directory."""
    data: Dict[str, Any] = {}

    default_path = config_dir / 'config-default.yaml'
    if default_path.exists():
        with open(default_path, 'r') as f:
            _merge_dicts(data, yaml.safe_load(f) or {})

    if runtime_env != DEFAULT_RUNTIME_ENV:
        env_path = config_dir / f'config-{runtime_env}.yaml'
        if env_path.exists():
            with open(env_path, 'r') as f:
                _merge_dicts(data, yaml.safe_load(f) or {})

    return data


def load_config_tree(
    config_dirs: Sequence[Path] | Path,
    runtime_env: str = DEFAULT_RUNTIME_ENV,
) -> Dict[str, Any]:
    """
    Load configuration from one or more directories, merging in order.

    Each directory may contain ``config-default.yaml`` and optionally
    ``config-{runtime_env}.yaml``.  Later directories override earlier ones.

    Returns an arbitrary tree of objects (dict).
    """
    if isinstance(config_dirs, Path):
        config_dirs = [config_dirs]

    data: Dict[str, Any] = {}
    for config_dir in config_dirs:
        if not config_dir.is_dir():
            raise FileNotFoundError(f'Config directory not found: {config_dir}')
        dir_data = _load_from_dir(config_dir, runtime_env)
        if dir_data:
            _merge_dicts(data, dir_data)

    return data


def map_from_yaml(
    cls: Type[T],
    runtime_env: str,
    config_dirs: Sequence[Path] | str,
) -> T:
    """
    Load configuration from YAML files using the provided factory.
    Loads default config and merges local config if RUNTIME_ENV is 'local'.
    """
    if isinstance(config_dirs, str):
        dirs: Sequence[Path] = [Path(config_dirs)]
    else:
        dirs = config_dirs

    data = load_config_tree(dirs, runtime_env)

    if not data:
        raise ValueError('Configuration is empty after loading YAML files')

    try:
        return cls.from_dict(data)
    except KeyError as e:
        raise ValueError(f'Missing required configuration value: {e}') from e


def resolve_env_config(
    runtime_env: str | None,
    config_dir: str | None,
) -> tuple[str, str | None]:
    """
    Resolve the runtime environment and optional config directory.

    Args:
        runtime_env: Value of RUNTIME_ENV env var (or None).
        config_dir: Value of CONFIG_DIR env var (or None).

    Returns:
        Tuple of (runtime_env, config_dir). config_dir may be None
        when no additional config directory is needed.
    """
    resolved_env = runtime_env if runtime_env else LOCAL_RUNTIME_ENV
    resolved_dir = config_dir if config_dir else None
    return resolved_env, resolved_dir


def get_config(
    cls: Type[T],
    base_config_dirs: Sequence[Path] | None = None,
) -> T:
    """
    Load configuration from YAML files using the provided factory.
    Reads RUNTIME_ENV and CONFIG_DIR from the environment.

    If *base_config_dirs* is provided, those directories are loaded first
    (in order).  If CONFIG_DIR is set and points to an existing directory,
    it is appended as a final overlay.
    """
    runtime_env, config_dir = resolve_env_config(
        os.getenv(RUNTIME_ENV_VAR),
        os.getenv(CONFIG_DIR_VAR),
    )

    config_dirs = list(base_config_dirs or [])
    if config_dir:
        config_dir_path = Path(config_dir)
        if config_dir_path.is_dir():
            config_dirs.append(config_dir_path)
    return map_from_yaml(cls, runtime_env, config_dirs)
