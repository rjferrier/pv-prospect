"""Tests for create_pv_sites_by_system_id."""

import io

from pv_prospect.common.pv_site_repo import create_pv_sites_by_system_id

from .helpers import CSV_HEADER, SAMPLE_ROW_STR, TWO_PANEL_ROW_STR


def test_returns_dict_keyed_by_system_id() -> None:
    csv_content = f'{CSV_HEADER}\n{SAMPLE_ROW_STR}\n'
    result = create_pv_sites_by_system_id(io.StringIO(csv_content))
    assert 89665 in result
    assert result[89665].name == 'Test Site'


def test_multiple_rows() -> None:
    csv_content = f'{CSV_HEADER}\n{SAMPLE_ROW_STR}\n{TWO_PANEL_ROW_STR}\n'
    result = create_pv_sites_by_system_id(io.StringIO(csv_content))
    assert len(result) == 2
    assert 89665 in result
    assert 12345 in result
