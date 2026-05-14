"""Tests for build_transform_phases."""

from pv_prospect.data_transformation.processing.entrypoint import (
    build_transform_phases,
)


def _env(task: list[dict[str, str]]) -> dict[str, str]:
    return {e['name']: e['value'] for e in task}


def _all_tasks(
    phases: list[list[list[dict[str, str]]]],
) -> list[list[dict[str, str]]]:
    return [task for phase in phases for task in phase]


def test_end_date_propagated_to_every_task_when_given() -> None:
    """The backfill window's END_DATE must reach every task so the
    container transforms the whole range, not just the start date."""
    phases = build_transform_phases(
        'pv-prospect-transform-pv-sites-backfill',
        '2026-03-18',
        '2026-04-15',
        [4708, 24667],
        [],
        'day',
        '2026-05-14',
    )
    for task in _all_tasks(phases):
        assert _env(task)['END_DATE'] == '2026-04-15'


def test_end_date_omitted_from_every_task_when_none() -> None:
    """A daily-transform plan passes no END_DATE; it must stay unset so the
    container falls back to a single-day window."""
    phases = build_transform_phases(
        'pv-prospect-transform',
        '2026-05-13',
        None,
        [4708],
        [],
        'day',
        '2026-05-14',
    )
    for task in _all_tasks(phases):
        assert 'END_DATE' not in _env(task)


def test_window_start_date_set_on_every_task() -> None:
    phases = build_transform_phases(
        'pv-prospect-transform-pv-sites-backfill',
        '2026-03-18',
        '2026-04-15',
        [4708],
        [],
        'day',
        '2026-05-14',
    )
    for task in _all_tasks(phases):
        env = _env(task)
        assert env['START_DATE'] == '2026-03-18'
        assert env['DATE'] == '2026-03-18'


def test_phases_are_clean_then_prepare_then_assemble() -> None:
    phases = build_transform_phases(
        'pv-prospect-transform-pv-sites-backfill',
        '2026-03-18',
        '2026-04-15',
        [4708, 24667],
        [],
        'day',
        '2026-05-14',
    )
    clean, prepare, assemble = phases
    assert {_env(t)['TRANSFORM_STEP'] for t in clean} == {
        'clean_pv',
        'clean_weather',
    }
    assert {_env(t)['TRANSFORM_STEP'] for t in prepare} == {
        'prepare_pv',
        'prepare_weather',
    }
    assert {_env(t)['TRANSFORM_STEP'] for t in assemble} == {
        'assemble_pv',
        'assemble_weather',
    }


def test_locations_produce_weather_only_tasks() -> None:
    phases = build_transform_phases(
        'pv-prospect-transform-weather-grid-backfill',
        '2026-04-15',
        '2026-04-29',
        [],
        ['50.49,-3.54', '51.00,-4.00'],
        'day',
        '2026-05-14',
    )
    clean, prepare, assemble = phases
    assert [_env(t)['TRANSFORM_STEP'] for t in clean] == [
        'clean_weather',
        'clean_weather',
    ]
    assert [_env(t)['LOCATION'] for t in clean] == ['50.49,-3.54', '51.00,-4.00']
    assert [_env(t)['TRANSFORM_STEP'] for t in prepare] == [
        'prepare_weather',
        'prepare_weather',
    ]
    assert [_env(t)['TRANSFORM_STEP'] for t in assemble] == ['assemble_weather']
