"""Transform-backfill planning for the data-transformation pipeline.

This module owns the transform side's planning logic: the
:class:`TransformUnit` value type, :func:`build_transform_phases` (shared
with the daily transform), and — still cursor-based for now — the
per-:class:`BackfillScope` plan/commit wrappers.

Two scopes are tracked independently — one for the PV-site transform
backfill, one for the weather-grid transform backfill — so each can
advance at the cadence of its corresponding extraction backfill without
being held back by the other. Window lengths come from
:func:`default_window_days` and so always match the extraction side.
"""

from dataclasses import dataclass
from datetime import date
from typing import Literal

from pv_prospect.etl import (
    BackfillCursor,
    BackfillPlan,
    BackfillScope,
    build_env_list,
    commit_backfill,
    default_window_days,
    inject_task_hash,
    plan_backfill,
)
from pv_prospect.etl.storage import FileSystem

_WORKFLOW_NAME_BY_SCOPE: dict[BackfillScope, str] = {
    BackfillScope.PV_SITES: 'pv-prospect-transform-pv-sites-backfill',
    BackfillScope.WEATHER_GRID: 'pv-prospect-transform-weather-grid-backfill',
}


def workflow_name_for(scope: BackfillScope) -> str:
    """Return the workflow name for the transform backfill of *scope*."""
    return _WORKFLOW_NAME_BY_SCOPE[scope]


# ---------------------------------------------------------------------------
# Phase building (shared by the daily transform and the backfill planner)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TransformUnit:
    """One ``(entity, window)`` pair to transform.

    ``kind`` selects the step family: a ``'pv'`` unit becomes
    ``clean_pv`` / ``prepare_pv`` / ``assemble_pv`` tasks, a ``'weather'``
    unit ``clean_weather`` / ``prepare_weather`` / ``assemble_weather``.
    Exactly one of ``pv_system_id`` / ``location`` identifies the entity;
    a weather unit may be located by either (PV-site weather is keyed by
    system id, grid weather by a ``lat,lon`` string). ``start_date`` is
    inclusive and ``end_date`` exclusive; ``end_date`` is ``None`` for the
    daily single-day transform.
    """

    kind: Literal['pv', 'weather']
    start_date: str
    end_date: str | None = None
    pv_system_id: int | None = None
    location: str | None = None


def _transform_task_env(
    transform_step: str,
    start_date_str: str,
    end_date_str: str | None,
    workflow_name: str,
    run_date: str,
    pv_system_id: int | None = None,
    location: str | None = None,
) -> list[dict[str, str]]:
    """Build the env list for one transform task, with TASK_HASH injected.

    ``END_DATE`` is included only when *end_date_str* is set, so the
    container processes the whole ``[START_DATE, END_DATE)`` window. When
    omitted the container falls back to a single day — the daily-transform
    behaviour.
    """
    env: dict[str, str] = {
        'TRANSFORM_STEP': transform_step,
        'DATE': start_date_str,
        'START_DATE': start_date_str,
        'WORKFLOW_NAME': workflow_name,
        'RUN_DATE': run_date,
    }
    if end_date_str:
        env['END_DATE'] = end_date_str
    if pv_system_id is not None:
        env['PV_SYSTEM_ID'] = str(pv_system_id)
    if location is not None:
        env['LOCATION'] = location
    return inject_task_hash(build_env_list(**env))


def build_transform_phases(
    units: list[TransformUnit],
    workflow_name: str,
    run_date: str,
) -> list[list[list[dict[str, str]]]]:
    """Enumerate the clean / prepare / assemble phases for *units*.

    Each unit becomes a clean task and a prepare task carrying that unit's
    own ``[START_DATE, END_DATE)`` window, so one run can mix windows
    (e.g. weather-grid extraction's diagonal march through
    ``(sample_file, date)`` space). Phase 2 emits one ``assemble_pv`` per
    distinct ``pv_system_id`` and a single ``assemble_weather`` when any
    weather unit is present. An assemble task ignores its date window but
    carries the first-seen relevant unit's window so its task hash is
    deterministic. The returned phases are unfiltered — the caller applies
    :meth:`WorkflowOrchestrator.filter_remaining_tasks` if it wants
    self-filtering (the daily transform does; the backfill does not).
    """

    def task(unit: TransformUnit, transform_step: str) -> list[dict[str, str]]:
        return _transform_task_env(
            transform_step,
            unit.start_date,
            unit.end_date,
            workflow_name,
            run_date,
            pv_system_id=unit.pv_system_id,
            location=unit.location,
        )

    clean: list[list[dict[str, str]]] = []
    prepare: list[list[dict[str, str]]] = []
    assemble_pv_units: dict[int, TransformUnit] = {}
    first_weather_unit: TransformUnit | None = None
    for unit in units:
        if unit.kind == 'pv':
            clean.append(task(unit, 'clean_pv'))
            prepare.append(task(unit, 'prepare_pv'))
            if unit.pv_system_id is not None:
                assemble_pv_units.setdefault(unit.pv_system_id, unit)
        else:
            clean.append(task(unit, 'clean_weather'))
            prepare.append(task(unit, 'prepare_weather'))
            if first_weather_unit is None:
                first_weather_unit = unit

    assemble: list[list[dict[str, str]]] = [
        task(unit, 'assemble_pv') for unit in assemble_pv_units.values()
    ]
    if first_weather_unit is not None:
        assemble.append(
            _transform_task_env(
                'assemble_weather',
                first_weather_unit.start_date,
                first_weather_unit.end_date,
                workflow_name,
                run_date,
            )
        )

    return [clean, prepare, assemble]


def plan_transform_backfill(
    scope: BackfillScope,
    today: date,
    run_date: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
    window_days: int | None = None,
) -> BackfillPlan:
    """Plan today's transform-backfill window for *scope* and persist the manifest.

    The live cursor is not advanced — that happens later via
    :func:`commit_transform_backfill`, after the transform jobs succeed.
    When ``window_days`` is ``None`` the scope's default window is used.
    """
    if window_days is None:
        window_days = default_window_days(scope)
    return plan_backfill(
        cursors_fs,
        manifests_fs,
        workflow_name_for(scope),
        run_date,
        today,
        window_days,
    )


def commit_transform_backfill(
    scope: BackfillScope,
    run_date: str,
    cursors_fs: FileSystem,
    manifests_fs: FileSystem,
) -> BackfillCursor:
    """Promote *scope*'s manifest next-cursor to the live cursor."""
    return commit_backfill(cursors_fs, manifests_fs, workflow_name_for(scope), run_date)
