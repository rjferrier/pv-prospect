"""Tests for resolve_log_level."""

import logging

from pv_prospect.common import resolve_log_level


def test_named_level_resolves_to_constant():
    assert resolve_log_level('DEBUG') == logging.DEBUG
    assert resolve_log_level('WARNING') == logging.WARNING


def test_name_is_case_insensitive():
    assert resolve_log_level('info') == logging.INFO


def test_surrounding_whitespace_is_ignored():
    assert resolve_log_level('  ERROR  ') == logging.ERROR


def test_none_defaults_to_info():
    assert resolve_log_level(None) == logging.INFO


def test_empty_string_defaults_to_info():
    assert resolve_log_level('') == logging.INFO


def test_notset_defaults_to_info():
    # NOTSET (0) would make the root logger emit everything, including
    # DEBUG; it must not be honoured as a LOG_LEVEL value.
    assert resolve_log_level('NOTSET') == logging.INFO


def test_unrecognised_value_defaults_to_info():
    assert resolve_log_level('verbose') == logging.INFO
