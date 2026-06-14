"""Tests for ModelTrainerConfig.from_dict."""

from __future__ import annotations

import pytest
from pv_prospect.model_trainer.config import ModelTrainerConfig

_REQUIRED_KEYS = {
    'instance_repo_url': 'git@github.com:rjferrier/pv-prospect-instance.git'
}


def test_from_dict_parses_required_key() -> None:
    config = ModelTrainerConfig.from_dict(_REQUIRED_KEYS)
    assert (
        config.instance_repo_url == 'git@github.com:rjferrier/pv-prospect-instance.git'
    )


def test_from_dict_uses_defaults_for_optional_keys() -> None:
    config = ModelTrainerConfig.from_dict(_REQUIRED_KEYS)
    assert config.instance_repo_branch == 'main'
    assert config.feature_remote_name == 'feature'
    assert config.prepared_data_dir == 'data/prepared'
    assert config.pv_sites_csv_path == 'data/static/pv_sites.csv'
    assert config.model_remote_name == 'model'
    assert config.model_dir == 'models'
    assert config.model_bucket_name == ''
    assert config.promotion_tolerance == 0.02
    assert config.compute_loso is True


def test_from_dict_accepts_compute_loso_override() -> None:
    config = ModelTrainerConfig.from_dict({**_REQUIRED_KEYS, 'compute_loso': False})
    assert config.compute_loso is False


def test_from_dict_accepts_optional_overrides() -> None:
    data = {
        **_REQUIRED_KEYS,
        'instance_repo_branch': 'develop',
        'feature_remote_name': 'my-feature',
        'prepared_data_dir': 'data/custom',
        'pv_sites_csv_path': 'data/static/custom_sites.csv',
        'model_remote_name': 'my-model',
        'model_dir': 'artifacts',
        'model_bucket_name': 'my-versioned-model',
        'promotion_tolerance': 0.05,
    }
    config = ModelTrainerConfig.from_dict(data)
    assert config.instance_repo_branch == 'develop'
    assert config.feature_remote_name == 'my-feature'
    assert config.prepared_data_dir == 'data/custom'
    assert config.pv_sites_csv_path == 'data/static/custom_sites.csv'
    assert config.model_remote_name == 'my-model'
    assert config.model_dir == 'artifacts'
    assert config.model_bucket_name == 'my-versioned-model'
    assert config.promotion_tolerance == 0.05


def test_from_dict_raises_on_missing_required_key() -> None:
    with pytest.raises(KeyError):
        ModelTrainerConfig.from_dict({})
