"""Tests for load_config_tree."""

import pytest
from pv_prospect.common.config_parser import load_config_tree


def test_loads_default_config(tmp_path):
    (tmp_path / 'config-default.yaml').write_text('storage:\n  type: local\n')
    result = load_config_tree(tmp_path)
    assert result == {'storage': {'type': 'local'}}


def test_merges_env_config_over_default(tmp_path):
    (tmp_path / 'config-default.yaml').write_text(
        'storage:\n  type: local\n  path: /tmp\n'
    )
    (tmp_path / 'config-local.yaml').write_text('storage:\n  path: /data\n')
    result = load_config_tree(tmp_path, runtime_env='local')
    assert result == {'storage': {'type': 'local', 'path': '/data'}}


def test_default_env_skips_merge(tmp_path):
    (tmp_path / 'config-default.yaml').write_text('key: value\n')
    result = load_config_tree(tmp_path, runtime_env='default')
    assert result == {'key': 'value'}


def test_nonexistent_dir_raises_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_config_tree(tmp_path / 'nonexistent')


def test_one_nonexistent_dir_in_list_raises_file_not_found(tmp_path):
    (tmp_path / 'config-default.yaml').write_text('key: value\n')
    with pytest.raises(FileNotFoundError):
        load_config_tree([tmp_path, tmp_path / 'nonexistent'])


def test_missing_env_config_uses_default_only(tmp_path):
    (tmp_path / 'config-default.yaml').write_text('key: value\n')
    result = load_config_tree(tmp_path, runtime_env='staging')
    assert result == {'key': 'value'}


def test_merges_env_config_preserves_unoverridden_nested_keys(tmp_path):
    (tmp_path / 'config-default.yaml').write_text(
        'storage:\n  backend:\n    type: local\n    path: /tmp\n'
    )
    (tmp_path / 'config-local.yaml').write_text(
        'storage:\n  backend:\n    path: /data\n'
    )
    result = load_config_tree(tmp_path, runtime_env='local')
    assert result == {'storage': {'backend': {'type': 'local', 'path': '/data'}}}
