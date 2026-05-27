"""Tests for plan_slices."""

import json
from datetime import date

from pv_prospect.data_transformation.processing import (
    ConsumedMarker,
    plan_slices,
    save_marker,
    workflow_name_for,
)
from pv_prospect.etl import BackfillScope, PVSlice, WeatherSlice

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
) -> tuple[list[PVSlice] | list[WeatherSlice], str]:
    ledger_fs = FakeFileSystem(ledger_files)
    cursors_fs = FakeFileSystem()
    if marker:
        save_marker(cursors_fs, workflow_name_for(scope), ConsumedMarker(marker))
    return plan_slices(scope, ledger_fs, cursors_fs, max_extract_runs)


def test_pv_pv_and_weather_descriptors_dedup_into_one_slice() -> None:
    """The PV-sites extraction emits two descriptors per (system, window)
    — one for PV data, one for the same-window weather — and the slice
    planner must collapse them into a single PVSlice."""
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

    slices, _ = _plan(BackfillScope.PV_SITES, ledgers)

    assert slices == [
        PVSlice(
            pv_system_id=4708,
            start_date=date(2026, 3, 18),
            end_date=date(2026, 4, 15),
        )
    ]


def test_weather_grid_descriptors_dedup_per_sample_window() -> None:
    """Each grid point in a sample emits its own descriptor; the slice
    planner collapses them all into one WeatherSlice per
    (sample_index, window)."""
    ledgers = {
        _ledger_path('2026-05-14', '063130', _WEATHER_EXTRACT_WF): (
            _ledger_line(
                'completed',
                {
                    'data_source': 'weather',
                    'start_date': '2026-01-01',
                    'end_date': '2026-01-15',
                    'location': '50.06,-5.16',
                    'grid_point_sample_index': '7',
                },
            )
            + _ledger_line(
                'completed',
                {
                    'data_source': 'weather',
                    'start_date': '2026-01-01',
                    'end_date': '2026-01-15',
                    'location': '50.10,-5.20',
                    'grid_point_sample_index': '7',
                },
            )
        )
    }

    slices, _ = _plan(BackfillScope.WEATHER_GRID, ledgers)

    assert slices == [
        WeatherSlice(
            grid_point_sample_index=7,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 15),
        )
    ]


def test_distinct_windows_yield_distinct_slices() -> None:
    ledgers = {
        _ledger_path('2026-05-14', '063130', _WEATHER_EXTRACT_WF): (
            _ledger_line(
                'completed',
                {
                    'start_date': '2026-01-01',
                    'end_date': '2026-01-15',
                    'location': '50.06,-5.16',
                    'grid_point_sample_index': '7',
                },
            )
            + _ledger_line(
                'completed',
                {
                    'start_date': '2026-02-01',
                    'end_date': '2026-02-15',
                    'location': '50.06,-5.16',
                    'grid_point_sample_index': '8',
                },
            )
        )
    }

    slices, _ = _plan(BackfillScope.WEATHER_GRID, ledgers)

    assert {(s.grid_point_sample_index, s.start_date) for s in slices} == {
        (7, date(2026, 1, 1)),
        (8, date(2026, 2, 1)),
    }


def test_failed_extraction_descriptors_are_skipped() -> None:
    """A failed extraction's descriptor doesn't appear in
    read_completed_descriptors, so the slice planner doesn't see it
    at all."""
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

    slices, _ = _plan(BackfillScope.PV_SITES, ledgers)

    assert [s.pv_system_id for s in slices] == [222]


def test_pv_descriptor_without_end_date_is_skipped() -> None:
    """A PV-sites slice needs an end_date; descriptors without one
    (a daily-extract artefact) are not slice candidates."""
    ledgers = {
        _ledger_path('2026-05-14', '020000', _PV_EXTRACT_WF): _ledger_line(
            'completed',
            {
                'data_source': 'pv',
                'start_date': '2026-01-01',
                'pv_system_id': '111',
            },
        )
    }

    slices, _ = _plan(BackfillScope.PV_SITES, ledgers)

    assert slices == []


def test_marker_advances_to_last_consumed_ledger() -> None:
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


def test_no_unconsumed_ledgers_returns_empty_and_keeps_marker() -> None:
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

    slices, next_marker = _plan(BackfillScope.PV_SITES, ledgers, marker=marker)

    assert slices == []
    assert next_marker == marker
