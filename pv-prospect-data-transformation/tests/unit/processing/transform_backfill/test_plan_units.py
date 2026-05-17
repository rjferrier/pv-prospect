"""Tests for plan_units."""

import json

from pv_prospect.data_transformation.processing import (
    ConsumedMarker,
    TransformUnit,
    plan_units,
    save_marker,
    workflow_name_for,
)
from pv_prospect.etl import BackfillScope

from tests.unit.helpers.fake_file_system import FakeFileSystem

_PV_EXTRACT_WF = 'pv-prospect-extract-pv-sites-backfill'
_WEATHER_EXTRACT_WF = 'pv-prospect-extract-weather-grid-backfill'


def _ledger_line(status: str, descriptor: dict[str, str]) -> str:
    return (
        json.dumps(
            {
                'recorded_at': '2026-05-14T07:00:00+00:00',
                'run_date': '2026-05-14',
                'workflow': 'extract',
                'task_hash': 'h',
                'descriptor': descriptor,
                'status': status,
            }
        )
        + '\n'
    )


def _ledger_path(run_date: str, hhmmss: str, extract_workflow: str) -> str:
    return f'{run_date}/{run_date}-{hhmmss}-{extract_workflow}.jsonl'


def _plan(
    scope: BackfillScope,
    ledger_files: dict[str, str],
    *,
    marker: str = '',
    max_extract_runs: int = 4,
) -> tuple[list[TransformUnit], str]:
    """Run plan_units against in-memory filesystems and return its result."""
    ledger_fs = FakeFileSystem(ledger_files)
    cursors_fs = FakeFileSystem()
    if marker:
        save_marker(cursors_fs, workflow_name_for(scope), ConsumedMarker(marker))
    return plan_units(scope, ledger_fs, cursors_fs, max_extract_runs)


def test_consumes_only_ledgers_above_the_marker() -> None:
    ledgers = {
        _ledger_path('2026-05-12', '020000', _PV_EXTRACT_WF): _ledger_line(
            'completed',
            {
                'data_source': 'pv',
                'start_date': '2026-01-01',
                'end_date': '2026-01-29',
                'pv_system_id': '111',
            },
        ),
        _ledger_path('2026-05-13', '020000', _PV_EXTRACT_WF): _ledger_line(
            'completed',
            {
                'data_source': 'pv',
                'start_date': '2026-02-01',
                'end_date': '2026-03-01',
                'pv_system_id': '222',
            },
        ),
    }
    marker = f'2026-05-12-020000-{_PV_EXTRACT_WF}.jsonl'

    units, _ = _plan(BackfillScope.PV_SITES, ledgers, marker=marker)

    assert [u.pv_system_id for u in units] == [222]


def test_consumes_oldest_first_capped_at_max_extract_runs() -> None:
    ledgers = {
        _ledger_path(f'2026-05-{day:02d}', '020000', _PV_EXTRACT_WF): _ledger_line(
            'completed',
            {
                'data_source': 'pv',
                'start_date': '2026-01-01',
                'end_date': '2026-01-29',
                'pv_system_id': str(day),
            },
        )
        for day in (10, 11, 12, 13, 14)
    }

    units, next_marker = _plan(BackfillScope.PV_SITES, ledgers, max_extract_runs=2)

    assert [u.pv_system_id for u in units] == [10, 11]
    assert next_marker == f'2026-05-11-020000-{_PV_EXTRACT_WF}.jsonl'


def test_skips_failed_extraction_entries() -> None:
    ledgers = {
        _ledger_path('2026-05-14', '020000', _PV_EXTRACT_WF): (
            _ledger_line(
                'failed',
                {
                    'data_source': 'pv',
                    'start_date': '2026-01-01',
                    'end_date': '2026-01-29',
                    'pv_system_id': '111',
                },
            )
            + _ledger_line(
                'completed',
                {
                    'data_source': 'pv',
                    'start_date': '2026-01-01',
                    'end_date': '2026-01-29',
                    'pv_system_id': '222',
                },
            )
        )
    }

    units, _ = _plan(BackfillScope.PV_SITES, ledgers)

    assert [u.pv_system_id for u in units] == [222]


def test_pv_sites_pv_and_weather_descriptors_both_become_units() -> None:
    ledgers = {
        _ledger_path('2026-05-14', '024844', _PV_EXTRACT_WF): (
            _ledger_line(
                'completed',
                {
                    'data_source': 'pv',
                    'start_date': '2026-03-18',
                    'end_date': '2026-04-15',
                    'pv_system_id': '4708',
                },
            )
            + _ledger_line(
                'completed',
                {
                    'data_source': 'weather',
                    'start_date': '2026-03-18',
                    'end_date': '2026-04-15',
                    'pv_system_id': '4708',
                },
            )
        )
    }

    units, _ = _plan(BackfillScope.PV_SITES, ledgers)

    assert {u.kind for u in units} == {'pv', 'weather'}


def test_weather_grid_ledger_spanning_windows_yields_per_task_windows() -> None:
    ledgers = {
        _ledger_path('2026-05-14', '063130', _WEATHER_EXTRACT_WF): (
            _ledger_line(
                'completed',
                {
                    'data_source': 'weather',
                    'start_date': '2026-01-01',
                    'end_date': '2026-01-15',
                    'location': '50.06,-5.16',
                },
            )
            + _ledger_line(
                'completed',
                {
                    'data_source': 'weather',
                    'start_date': '2026-02-01',
                    'end_date': '2026-02-15',
                    'location': '51.00,-4.00',
                },
            )
        )
    }

    units, _ = _plan(BackfillScope.WEATHER_GRID, ledgers)

    windows = {u.location: (u.start_date, u.end_date) for u in units}
    assert windows == {
        '50.06,-5.16': ('2026-01-01', '2026-01-15'),
        '51.00,-4.00': ('2026-02-01', '2026-02-15'),
    }


def test_no_unconsumed_ledgers_returns_empty_units_and_keeps_marker() -> None:
    ledger_name = _ledger_path('2026-05-14', '020000', _PV_EXTRACT_WF)
    ledgers = {
        ledger_name: _ledger_line(
            'completed',
            {
                'data_source': 'pv',
                'start_date': '2026-01-01',
                'end_date': '2026-01-29',
                'pv_system_id': '111',
            },
        )
    }
    marker = f'2026-05-14-020000-{_PV_EXTRACT_WF}.jsonl'

    units, next_marker = _plan(BackfillScope.PV_SITES, ledgers, marker=marker)

    assert units == []
    assert next_marker == marker


def test_next_marker_is_highest_consumed_ledger_name() -> None:
    ledgers = {
        _ledger_path('2026-05-13', '020000', _PV_EXTRACT_WF): _ledger_line(
            'completed',
            {
                'data_source': 'pv',
                'start_date': '2026-01-01',
                'end_date': '2026-01-29',
                'pv_system_id': '111',
            },
        ),
        _ledger_path('2026-05-14', '020000', _PV_EXTRACT_WF): _ledger_line(
            'completed',
            {
                'data_source': 'pv',
                'start_date': '2026-02-01',
                'end_date': '2026-03-01',
                'pv_system_id': '222',
            },
        ),
    }

    _, next_marker = _plan(BackfillScope.PV_SITES, ledgers)

    assert next_marker == f'2026-05-14-020000-{_PV_EXTRACT_WF}.jsonl'


def test_first_run_consumes_from_the_oldest_ledger() -> None:
    """With no marker file, the empty initial marker means the planner
    starts at the oldest extraction ledger."""
    ledgers = {
        _ledger_path('2026-05-13', '020000', _PV_EXTRACT_WF): _ledger_line(
            'completed',
            {
                'data_source': 'pv',
                'start_date': '2026-01-01',
                'end_date': '2026-01-29',
                'pv_system_id': '111',
            },
        ),
    }

    units, _ = _plan(BackfillScope.PV_SITES, ledgers)

    assert [u.pv_system_id for u in units] == [111]
