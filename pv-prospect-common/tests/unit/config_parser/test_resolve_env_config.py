"""Tests for resolve_env_config."""

from pv_prospect.common.config_parser import resolve_env_config


def test_both_provided() -> None:
    env, dir_ = resolve_env_config('staging', '/opt/config', False)
    assert env == 'staging'
    assert dir_ == '/opt/config'


def test_neither_provided_container_exists() -> None:
    env, dir_ = resolve_env_config(None, None, True)
    assert env == 'default'
    assert dir_ == '/app/resources'


def test_neither_provided_no_container() -> None:
    env, dir_ = resolve_env_config(None, None, False)
    assert env == 'local'
    assert dir_ == 'resources'


def test_only_runtime_env_provided() -> None:
    env, dir_ = resolve_env_config('staging', None, False)
    assert env == 'staging'
    assert dir_ == '/app/resources'


def test_only_config_dir_provided() -> None:
    env, dir_ = resolve_env_config(None, '/custom', False)
    assert env == 'default'
    assert dir_ == '/custom'


def test_empty_strings_treated_as_not_provided() -> None:
    env, dir_ = resolve_env_config('', '', False)
    assert env == 'local'
    assert dir_ == 'resources'
