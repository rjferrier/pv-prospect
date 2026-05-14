"""Tests for build_transform_phases."""

from pv_prospect.data_transformation.processing import (
    TransformUnit,
    build_transform_phases,
)

_WORKFLOW = 'pv-prospect-transform-pv-sites-backfill'
_RUN_DATE = '2026-05-14'


def _env(task: list[dict[str, str]]) -> dict[str, str]:
    return {e['name']: e['value'] for e in task}


def _all_tasks(
    phases: list[list[list[dict[str, str]]]],
) -> list[list[dict[str, str]]]:
    return [task for phase in phases for task in phase]


def test_pv_unit_emits_clean_prepare_assemble_pv() -> None:
    unit = TransformUnit('pv', '2026-03-18', '2026-04-15', pv_system_id=4708)

    clean, prepare, assemble = build_transform_phases([unit], _WORKFLOW, _RUN_DATE)

    assert [_env(t)['TRANSFORM_STEP'] for t in clean] == ['clean_pv']
    assert [_env(t)['TRANSFORM_STEP'] for t in prepare] == ['prepare_pv']
    assert [_env(t)['TRANSFORM_STEP'] for t in assemble] == ['assemble_pv']
    assert all(
        _env(t)['PV_SYSTEM_ID'] == '4708'
        for t in _all_tasks([clean, prepare, assemble])
    )


def test_weather_unit_located_by_pv_system_id() -> None:
    unit = TransformUnit('weather', '2026-03-18', '2026-04-15', pv_system_id=4708)

    clean, prepare, assemble = build_transform_phases([unit], _WORKFLOW, _RUN_DATE)

    assert [_env(t)['TRANSFORM_STEP'] for t in clean] == ['clean_weather']
    assert [_env(t)['TRANSFORM_STEP'] for t in prepare] == ['prepare_weather']
    assert _env(clean[0])['PV_SYSTEM_ID'] == '4708'
    assert [_env(t)['TRANSFORM_STEP'] for t in assemble] == ['assemble_weather']


def test_weather_unit_located_by_location() -> None:
    unit = TransformUnit('weather', '2026-04-15', '2026-04-29', location='50.49,-3.54')

    clean, prepare, _ = build_transform_phases([unit], _WORKFLOW, _RUN_DATE)

    assert _env(clean[0])['LOCATION'] == '50.49,-3.54'
    assert 'PV_SYSTEM_ID' not in _env(clean[0])
    assert _env(prepare[0])['LOCATION'] == '50.49,-3.54'


def test_each_unit_carries_its_own_window() -> None:
    """A single run may mix windows (weather-grid's diagonal march); each
    unit's clean/prepare tasks must carry that unit's own window."""
    units = [
        TransformUnit('weather', '2026-01-01', '2026-01-15', location='a'),
        TransformUnit('weather', '2026-02-01', '2026-02-15', location='b'),
    ]

    clean, prepare, _ = build_transform_phases(units, _WORKFLOW, _RUN_DATE)

    assert (_env(clean[0])['START_DATE'], _env(clean[0])['END_DATE']) == (
        '2026-01-01',
        '2026-01-15',
    )
    assert (_env(clean[1])['START_DATE'], _env(clean[1])['END_DATE']) == (
        '2026-02-01',
        '2026-02-15',
    )
    assert _env(prepare[0])['START_DATE'] == '2026-01-01'
    assert _env(prepare[1])['START_DATE'] == '2026-02-01'


def test_start_date_and_date_both_set_to_unit_start() -> None:
    unit = TransformUnit('pv', '2026-03-18', '2026-04-15', pv_system_id=4708)

    clean, _, _ = build_transform_phases([unit], _WORKFLOW, _RUN_DATE)

    env = _env(clean[0])
    assert env['START_DATE'] == '2026-03-18'
    assert env['DATE'] == '2026-03-18'


def test_end_date_omitted_when_unit_has_none() -> None:
    """A daily-transform unit carries no end_date; END_DATE must stay
    unset so the container falls back to a single-day window."""
    unit = TransformUnit('pv', '2026-05-13', None, pv_system_id=4708)

    phases = build_transform_phases([unit], 'pv-prospect-transform', _RUN_DATE)

    for task in _all_tasks(phases):
        assert 'END_DATE' not in _env(task)


def test_assemble_pv_deduped_per_system() -> None:
    """Two windows for the same PV system yield one assemble_pv."""
    units = [
        TransformUnit('pv', '2026-01-01', '2026-01-29', pv_system_id=4708),
        TransformUnit('pv', '2026-02-01', '2026-02-29', pv_system_id=4708),
        TransformUnit('pv', '2026-01-01', '2026-01-29', pv_system_id=24667),
    ]

    _, _, assemble = build_transform_phases(units, _WORKFLOW, _RUN_DATE)

    assert [_env(t)['TRANSFORM_STEP'] for t in assemble] == [
        'assemble_pv',
        'assemble_pv',
    ]
    assert {_env(t)['PV_SYSTEM_ID'] for t in assemble} == {'4708', '24667'}


def test_assemble_weather_emitted_once_with_no_identifier() -> None:
    units = [
        TransformUnit('weather', '2026-01-01', '2026-01-15', location='a'),
        TransformUnit('weather', '2026-02-01', '2026-02-15', location='b'),
    ]

    _, _, assemble = build_transform_phases(units, _WORKFLOW, _RUN_DATE)

    assert [_env(t)['TRANSFORM_STEP'] for t in assemble] == ['assemble_weather']
    assert 'LOCATION' not in _env(assemble[0])
    assert 'PV_SYSTEM_ID' not in _env(assemble[0])


def test_phases_are_clean_then_prepare_then_assemble() -> None:
    units = [
        TransformUnit('pv', '2026-03-18', '2026-04-15', pv_system_id=4708),
        TransformUnit('weather', '2026-03-18', '2026-04-15', pv_system_id=4708),
    ]

    clean, prepare, assemble = build_transform_phases(units, _WORKFLOW, _RUN_DATE)

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


def test_no_units_yields_three_empty_phases() -> None:
    assert build_transform_phases([], _WORKFLOW, _RUN_DATE) == [[], [], []]
