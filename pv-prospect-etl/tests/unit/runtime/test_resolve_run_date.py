"""Tests for resolve_run_date."""

import os
from datetime import date
from unittest.mock import patch

from pv_prospect.etl import resolve_run_date


def test_returns_run_date_env_var_when_set() -> None:
    with patch.dict(os.environ, {'RUN_DATE': '2025-06-24'}, clear=False):
        assert resolve_run_date() == '2025-06-24'


def test_falls_back_to_today_when_unset() -> None:
    env = {k: v for k, v in os.environ.items() if k != 'RUN_DATE'}
    with patch.dict(os.environ, env, clear=True):
        assert resolve_run_date() == date.today().isoformat()
