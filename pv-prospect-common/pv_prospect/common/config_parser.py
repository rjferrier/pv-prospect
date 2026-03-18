import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Dict, Protocol, Type, TypeVar

import yaml  # type: ignore[import-untyped]


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

    if runtime_env != 'default':
        env_path = config_dir / f'config-{runtime_env}.yaml'
        if env_path.exists():
            with open(env_path, 'r') as f:
                _merge_dicts(data, yaml.safe_load(f) or {})

    return data


def load_config_tree(
    config_dirs: Sequence[Path] | Path,
    runtime_env: str = 'default',
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
    container_path_exists: bool,
) -> tuple[str, str]:
    """
    Resolve the runtime environment and config directory from the given inputs.

    If neither is provided, falls back to container defaults when
    container_path_exists is True, or local defaults otherwise.

    Returns:
        Tuple of (runtime_env, config_dir).
    """
    if not runtime_env and not config_dir:
        if container_path_exists:
            runtime_env = 'default'
            config_dir = '/app/resources'
        else:
            runtime_env = 'local'
            config_dir = 'resources'

    runtime_env = runtime_env or 'default'
    config_dir = config_dir or '/app/resources'

    return runtime_env, config_dir


def get_config(
    cls: Type[T],
    base_config_dirs: Sequence[Path] | None = None,
) -> T:
    """
    Load configuration from YAML files using the provided factory.
    Reads 'RUNTIME_ENV' and 'CONFIG_DIR' from the environment.

    If *base_config_dirs* is provided, those directories are loaded first
    (in order) and the package's own config directory is merged on top.
    """
    runtime_env, config_dir = resolve_env_config(
        os.getenv('RUNTIME_ENV'),
        os.getenv('CONFIG_DIR'),
        os.path.exists('/app/resources'),
    )

    config_dirs = list(base_config_dirs or []) + [Path(config_dir)]
    return map_from_yaml(cls, runtime_env, config_dirs)
