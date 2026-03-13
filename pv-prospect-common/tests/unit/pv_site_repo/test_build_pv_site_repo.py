"""Tests for build_pv_site_repo and singleton accessors."""

import io

import pytest
from pv_prospect.common.pv_site_repo import (
    build_pv_site_repo,
    get_all_pv_system_ids,
    get_pv_site_by_system_id,
    pv_sites_by_system_id,
)

from .helpers import CSV_HEADER, SAMPLE_ROW_STR, TWO_PANEL_ROW_STR


def _clear_repo() -> None:
    pv_sites_by_system_id.clear()


def test_populates_singleton() -> None:
    _clear_repo()
    csv_content = f'{CSV_HEADER}\n{SAMPLE_ROW_STR}\n'
    build_pv_site_repo(io.StringIO(csv_content))
    assert 89665 in pv_sites_by_system_id


def test_only_builds_once() -> None:
    _clear_repo()
    build_pv_site_repo(io.StringIO(f'{CSV_HEADER}\n{SAMPLE_ROW_STR}\n'))
    assert len(pv_sites_by_system_id) == 1

    build_pv_site_repo(io.StringIO(f'{CSV_HEADER}\n{TWO_PANEL_ROW_STR}\n'))
    assert len(pv_sites_by_system_id) == 1
    assert 89665 in pv_sites_by_system_id


def test_get_all_pv_system_ids_returns_sorted() -> None:
    _clear_repo()
    build_pv_site_repo(
        io.StringIO(f'{CSV_HEADER}\n{TWO_PANEL_ROW_STR}\n{SAMPLE_ROW_STR}\n')
    )
    assert get_all_pv_system_ids() == [12345, 89665]


def test_get_pv_site_by_system_id_returns_site() -> None:
    _clear_repo()
    build_pv_site_repo(io.StringIO(f'{CSV_HEADER}\n{SAMPLE_ROW_STR}\n'))
    assert get_pv_site_by_system_id(89665).name == 'Test Site'


def test_get_pv_site_by_unknown_id_raises() -> None:
    _clear_repo()
    build_pv_site_repo(io.StringIO(f'{CSV_HEADER}\n{SAMPLE_ROW_STR}\n'))
    with pytest.raises(KeyError):
        get_pv_site_by_system_id(99999)
