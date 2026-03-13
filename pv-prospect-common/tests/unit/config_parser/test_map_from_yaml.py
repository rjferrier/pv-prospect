"""Tests for map_from_yaml."""

from pathlib import Path
from typing import Any, Dict

import pytest
from pv_prospect.common.config_parser import map_from_yaml


def test_calls_from_dict_on_class(tmp_path: Path) -> None:
    (tmp_path / 'config-default.yaml').write_text('db:\n  host: localhost\n')

    class FakeConfig:
        def __init__(self, data: Dict[str, Any]) -> None:
            self.data = data

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'FakeConfig':
            return cls(data)

    result = map_from_yaml(FakeConfig, 'default', str(tmp_path))
    assert result.data == {'db': {'host': 'localhost'}}


def test_empty_config_raises_value_error(tmp_path: Path) -> None:
    (tmp_path / 'config-default.yaml').write_text('')

    class FakeConfig:
        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'FakeConfig':
            return cls()

    with pytest.raises(ValueError, match='(?i)empty'):
        map_from_yaml(FakeConfig, 'default', str(tmp_path))


def test_missing_key_raises_value_error(tmp_path: Path) -> None:
    (tmp_path / 'config-default.yaml').write_text('a: 1\n')

    class FakeConfig:
        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> 'FakeConfig':
            raise KeyError('required_key')

    with pytest.raises(ValueError, match='required_key'):
        map_from_yaml(FakeConfig, 'default', str(tmp_path))
