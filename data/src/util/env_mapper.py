import os
from dataclasses import dataclass
from typing import Mapping, TypeVar, Generic

T = TypeVar('T')


@dataclass(frozen=True)
class Var(Generic[T]):
    env_var_name: str
    type: type
    value: T | None

    def absent(self) -> bool:
        return self.value is None


@dataclass(frozen=True)
class VarMapping(Generic[T]):
    env_var_name: str
    target_type: type[T]


class ExtractedValue[T]:
    name: str
    value: T
    mapping: VarMapping[T]

    def absent(self) -> bool:
        return self.value is None


def map_from_env(cls: type[T], env_mappings: dict[str, VarMapping[T]], **kwargs) -> T:
    env_values = {
        attr_name: _extract_value(os.environ, mapping)
        for attr_name, mapping in env_mappings.items()
    }

    env_var_names = _get_names_of_env_vars_with_missing_values(env_mappings, env_values)
    if env_var_names:
        raise ValueError(f"Missing environment variables: {', '.join(env_var_names)}")

    return cls(**env_values, **kwargs)


def _extract_value(env: Mapping[str, str], mapping: VarMapping[T]) -> T:
    raw_value = env.get(mapping.env_var_name)
    return mapping.target_type(raw_value) if raw_value is not None else None


def _get_names_of_env_vars_with_missing_values(mappings: dict[str, VarMapping[T]], values: dict[str, T]) -> list[str]:
    return [mappings[k].env_var_name for k, v in values.items() if v is None]
