import os
from pathlib import Path
from typing import Any, Dict, Type, TypeVar, Protocol
import yaml

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


def load_config_tree(config_dir: Path, runtime_env: str = 'default') -> Dict[str, Any]:
    """
    Load default configuration from config_dir and merge environment-specific 
    configuration if runtime_env is not 'default'.
    
    Returns an arbitrary tree of objects (dict).
    """
    default_config_path = config_dir / 'config-default.yaml'
    if not default_config_path.exists():
        raise FileNotFoundError(
            f"Default ETL configuration file not found at {default_config_path}."
        )

    with open(default_config_path, 'r') as f:
        data = yaml.safe_load(f) or {}

    if runtime_env != 'default':
        local_config_path = config_dir / f'config-{runtime_env}.yaml'
        if local_config_path.exists():
            with open(local_config_path, 'r') as f:
                local_data = yaml.safe_load(f) or {}
            _merge_dicts(data, local_data)
    return data


def map_from_yaml(cls: Type[T], runtime_env: str, config_dir: str) -> T:
    """
    Load configuration from YAML files using the provided factory.
    Loads default config and merges local config if RUNTIME_ENV is 'local'.
    """
    data = load_config_tree(Path(config_dir), runtime_env)

    if not data:
        raise ValueError("Configuration is empty after loading YAML files")

    try:
        return cls.from_dict(data)
    except KeyError as e:
        raise ValueError(f"Missing required configuration value: {e}")


def get_config(cls: Type[T]) -> T:
    """
    Load configuration from YAML files using the provided factory.
    Reads 'RUNTIME_ENV' and 'CONFIG_DIR' from the environment.
    If not explicitly provided, defaults to Production ('default', '/app/resources') 
    if running in a container, or Local ('local', 'resources') if running natively.
    """
    runtime_env = os.getenv('RUNTIME_ENV')
    config_dir = os.getenv('CONFIG_DIR')
    
    if not runtime_env and not config_dir:
        if os.path.exists('/app/resources'):
            runtime_env = 'default'
            config_dir = '/app/resources'
        else:
            runtime_env = 'local'
            config_dir = 'resources'
            
    runtime_env = runtime_env or 'default'
    config_dir = config_dir or '/app/resources'
    
    return map_from_yaml(cls, runtime_env, config_dir)
