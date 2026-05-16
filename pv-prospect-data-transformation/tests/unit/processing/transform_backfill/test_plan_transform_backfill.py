"""Tests for plan_transform_backfill."""

import json

from pv_prospect.data_transformation.processing import (
    ConsumedMarker,
    plan_transform_backfill,
    save_marker,
    workflow_name_for,
)
from pv_prospect.etl import BackfillScope

from tests.unit.helpers.fake_file_system import FakeFileSystem

_RUN_DATE = '2026-05-15'
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
) -> tuple[FakeFileSystem, FakeFileSystem, str]:
    """Run plan_transform_backfill against in-memory filesystems.

    Returns ``(manifests_fs, cursors_fs, next_marker)``.
    """
    ledger_fs = FakeFileSystem(ledger_files)
    manifests_fs = FakeFileSystem()
    cursors_fs = FakeFileSystem()
    if marker:
        save_marker(cursors_fs, workflow_name_for(scope), ConsumedMarker(marker))
    next_marker = plan_transform_backfill(
        scope, _RUN_DATE, ledger_fs, manifests_fs, cursors_fs, max_extract_runs
    )
    return manifests_fs, cursors_fs, next_marker


def _phases(
    manifests_fs: FakeFileSystem, scope: BackfillScope
) -> list[list[list[dict[str, str]]]]:
    """Read the v2 phased manifest back as the original phases-of-env-lists.

    Concatenates each phase's chunked part files in order, then
    reconstructs each task's env from the phase descriptor's
    ``common_env`` plus the per-row ``zip(task_keys, row)``, skipping
    ``None`` cells (which represent keys absent from that task).
    """
    workflow_name = workflow_name_for(scope)
    index = json.loads(manifests_fs.read_text(f'{_RUN_DATE}/{workflow_name}.json'))
    phases: list[list[list[dict[str, str]]]] = []
    for descriptor in index['phases']:
        common_env = descriptor['common_env']
        task_keys = descriptor['task_keys']
        rows: list[list[str | None]] = []
        for part_file in descriptor['files']:
            part = json.loads(manifests_fs.read_text(f'{_RUN_DATE}/{part_file}'))
            rows.extend(part['rows'])
        phase = [
            common_env
            + [
                {'name': k, 'value': v}
                for k, v in zip(task_keys, row, strict=False)
                if v is not None
            ]
            for row in rows
        ]
        phases.append(phase)
    return phases


def _env(task: list[dict[str, str]]) -> dict[str, str]:
    return {e['name']: e['value'] for e in task}


def _sidecar_next_marker(manifests_fs: FakeFileSystem, scope: BackfillScope) -> str:
    sidecar = json.loads(
        manifests_fs.read_text(f'{_RUN_DATE}/{workflow_name_for(scope)}.marker.json')
    )
    return sidecar['next_marker']


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

    manifests_fs, _, _ = _plan(BackfillScope.PV_SITES, ledgers, marker=marker)

    clean = _phases(manifests_fs, BackfillScope.PV_SITES)[0]
    assert [_env(t)['PV_SYSTEM_ID'] for t in clean] == ['222']


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

    manifests_fs, _, next_marker = _plan(
        BackfillScope.PV_SITES, ledgers, max_extract_runs=2
    )

    clean = _phases(manifests_fs, BackfillScope.PV_SITES)[0]
    assert [_env(t)['PV_SYSTEM_ID'] for t in clean] == ['10', '11']
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

    manifests_fs, _, _ = _plan(BackfillScope.PV_SITES, ledgers)

    clean = _phases(manifests_fs, BackfillScope.PV_SITES)[0]
    assert [_env(t)['PV_SYSTEM_ID'] for t in clean] == ['222']


def test_pv_sites_pv_and_weather_entries_both_become_units() -> None:
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

    manifests_fs, _, _ = _plan(BackfillScope.PV_SITES, ledgers)

    clean, prepare, assemble = _phases(manifests_fs, BackfillScope.PV_SITES)
    assert {_env(t)['TRANSFORM_STEP'] for t in clean} == {
        'clean_pv',
        'clean_weather',
    }
    assert {_env(t)['TRANSFORM_STEP'] for t in prepare} == {
        'prepare_pv',
        'prepare_weather',
    }
    assert [_env(t)['TRANSFORM_STEP'] for t in assemble] == [
        'assemble_pv',
        'assemble_weather',
    ]


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

    manifests_fs, _, _ = _plan(BackfillScope.WEATHER_GRID, ledgers)

    clean = _phases(manifests_fs, BackfillScope.WEATHER_GRID)[0]
    windows = {
        _env(t)['LOCATION']: (_env(t)['START_DATE'], _env(t)['END_DATE']) for t in clean
    }
    assert windows == {
        '50.06,-5.16': ('2026-01-01', '2026-01-15'),
        '51.00,-4.00': ('2026-02-01', '2026-02-15'),
    }


def test_no_unconsumed_ledgers_writes_empty_manifest_and_keeps_marker() -> None:
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

    manifests_fs, _, next_marker = _plan(BackfillScope.PV_SITES, ledgers, marker=marker)

    assert _phases(manifests_fs, BackfillScope.PV_SITES) == [[], [], []]
    assert next_marker == marker


def test_next_marker_sidecar_holds_highest_consumed_ledger_name() -> None:
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

    manifests_fs, _, next_marker = _plan(BackfillScope.PV_SITES, ledgers)

    highest = f'2026-05-14-020000-{_PV_EXTRACT_WF}.jsonl'
    assert next_marker == highest
    assert _sidecar_next_marker(manifests_fs, BackfillScope.PV_SITES) == highest


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

    manifests_fs, _, _ = _plan(BackfillScope.PV_SITES, ledgers)

    clean = _phases(manifests_fs, BackfillScope.PV_SITES)[0]
    assert [_env(t)['PV_SYSTEM_ID'] for t in clean] == ['111']
