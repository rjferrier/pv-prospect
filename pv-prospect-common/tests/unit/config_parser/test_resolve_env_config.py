"""Tests for resolve_env_config."""

from pv_prospect.common.config_parser import resolve_env_config


def test_both_provided() -> None:
    env, dir_ = resolve_env_config('staging', '/opt/config')
    assert env == 'staging'
    assert dir_ == '/opt/config'


def test_neither_provided() -> None:
    env, dir_ = resolve_env_config(None, None)
    assert env == 'local'
    assert dir_ is None


def test_only_runtime_env_provided() -> None:
    env, dir_ = resolve_env_config('staging', None)
    assert env == 'staging'
    assert dir_ is None


def test_only_config_dir_provided() -> None:
    env, dir_ = resolve_env_config(None, '/custom')
    assert env == 'local'
    assert dir_ == '/custom'


def test_empty_strings_treated_as_not_provided() -> None:
    env, dir_ = resolve_env_config('', '')
    assert env == 'local'
    assert dir_ is None
