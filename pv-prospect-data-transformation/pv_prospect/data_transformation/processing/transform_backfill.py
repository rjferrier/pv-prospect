"""Transform-backfill planning for the data-transformation pipeline.

The transform backfill plans from the *extraction* backfill's durable
record rather than from a cursor of its own. The extraction backfill's
consolidated ledger already enumerates exactly which ``(site/location,
data_source, window)`` tuples have raw data; this module reads those
``completed`` entries and turns them into transform tasks.

What this module owns:

  * the :class:`TransformUnit` value type and
    :func:`build_transform_phases` (shared with the daily transform);
  * :class:`ConsumedMarker` — a small high-water mark over extraction
    consolidated-ledger filenames that bounds each run's work and
    advances unconditionally;
  * :func:`plan_units`, which returns the units to transform plus the
    marker value to advance to once the work succeeds.

The plan/commit split is collapsed into a single in-container backfill
handler (see :mod:`pv_prospect.data_transformation.processing.entrypoint`
``_run_transform_backfill``). Resetting the marker re-derives every
transform task from the durable extraction record — the supported way
to re-transform after a feature-spec change.
"""

import json
from dataclasses import dataclass
from typing import Literal

from pv_prospect.data_sources import DataSourceType
from pv_prospect.etl import (
    BackfillScope,
    build_env_list,
    inject_task_hash,
)
from pv_prospect.etl.storage import (
    FileSystem,
    list_consolidated_ledgers,
    read_completed_descriptors,
)

_WORKFLOW_NAME_BY_SCOPE: dict[BackfillScope, str] = {
    BackfillScope.PV_SITES: 'pv-prospect-transform-pv-sites-backfill',
    BackfillScope.WEATHER_GRID: 'pv-prospect-transform-weather-grid-backfill',
}

# The extraction backfill workflow each transform scope plans from. Its
# consolidated ledger is the transform planner's sole input.
_EXTRACT_WORKFLOW_NAME_BY_SCOPE: dict[BackfillScope, str] = {
    BackfillScope.PV_SITES: 'pv-prospect-extract-pv-sites-backfill',
    BackfillScope.WEATHER_GRID: 'pv-prospect-extract-weather-grid-backfill',
}


def workflow_name_for(scope: BackfillScope) -> str:
    """Return the workflow name for the transform backfill of *scope*."""
    return _WORKFLOW_NAME_BY_SCOPE[scope]


def extract_workflow_name_for(scope: BackfillScope) -> str:
    """Return the extraction backfill workflow name for *scope*.

    This is the workflow whose consolidated ledger the transform backfill
    of *scope* plans from.
    """
    return _EXTRACT_WORKFLOW_NAME_BY_SCOPE[scope]


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


# ---------------------------------------------------------------------------
# Consumed-through marker
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConsumedMarker:
    """High-water mark over extraction consolidated-ledger filenames.

    ``consumed_through`` is the name of the most recent extraction
    consolidated ledger this transform backfill has already planned from.
    Ledger names (``<run_date>-<HHMMSS>-<workflow>.jsonl``) sort lexically
    and so chronologically, so a plain string comparison decides what is
    new. The initial value — marker file absent — is the empty string,
    which sorts before every real ledger name, so the first run consumes
    from the oldest extraction ledger forward.
    """

    consumed_through: str = ''


def serialize_marker(marker: ConsumedMarker) -> str:
    return json.dumps({'consumed_through': marker.consumed_through})


def deserialize_marker(text: str) -> ConsumedMarker:
    data = json.loads(text)
    return ConsumedMarker(consumed_through=data.get('consumed_through', ''))


def _marker_filename(workflow_name: str) -> str:
    """Live-marker path, relative to the cursors filesystem.

    Reuses the path the old transform cursor occupied — same path, new
    schema — so migration is a one-line overwrite.
    """
    return f'{workflow_name}.json'


def load_marker(cursors_fs: FileSystem, workflow_name: str) -> ConsumedMarker:
    """Load the live marker for *workflow_name*, or an initial empty one."""
    path = _marker_filename(workflow_name)
    if cursors_fs.exists(path):
        return deserialize_marker(cursors_fs.read_text(path))
    return ConsumedMarker()


def save_marker(
    cursors_fs: FileSystem, workflow_name: str, marker: ConsumedMarker
) -> None:
    """Persist *marker* as the live marker for *workflow_name*."""
    cursors_fs.write_text(_marker_filename(workflow_name), serialize_marker(marker))


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def _descriptor_to_unit(descriptor: dict[str, str]) -> TransformUnit | None:
    """Map one extraction ledger descriptor to a :class:`TransformUnit`.

    The extraction backfill records, per completed task, a descriptor of
    ``{data_source, start_date, end_date?, pv_system_id | location}``,
    where ``data_source`` carries the :class:`DataSourceType` value
    (``'pv'`` / ``'weather'``) propagated from the extract job's
    ``DATA_SOURCE`` env var — *not* the concrete :class:`DataSource`
    (e.g. ``'pvoutput'``). Returns ``None`` for a descriptor missing its
    dates or an identifier so a malformed entry becomes a skipped hole
    rather than a crash.
    """
    start_date = descriptor.get('start_date')
    if not start_date:
        return None
    end_date = descriptor.get('end_date')
    kind: Literal['pv', 'weather'] = (
        'pv' if descriptor.get('data_source') == DataSourceType.PV.value else 'weather'
    )
    pv_system_id = descriptor.get('pv_system_id')
    if pv_system_id is not None:
        return TransformUnit(kind, start_date, end_date, pv_system_id=int(pv_system_id))
    location = descriptor.get('location')
    if location is not None:
        return TransformUnit(kind, start_date, end_date, location=location)
    return None


def plan_units(
    scope: BackfillScope,
    ledger_fs: FileSystem,
    cursors_fs: FileSystem,
    max_extract_runs: int,
) -> tuple[list[TransformUnit], str]:
    """Plan *scope*'s transform units from extraction's committed ledger.

    Lists the extraction backfill's consolidated ledgers, keeps those
    above the live marker, takes the oldest *max_extract_runs* of them,
    and turns every ``completed`` entry into a :class:`TransformUnit`.
    ``failed`` extraction entries are skipped — no raw data means no
    transform task, correctly leaving a hole.

    Returns ``(units, next_marker)``. The caller is expected to run the
    units and only then advance the live marker (via :func:`save_marker`)
    to ``next_marker``. When no unconsumed ledgers exist, returns an
    empty unit list and the unchanged marker, so callers can no-op
    cleanly without touching state.

    Pure planning — no manifests are written and no marker is advanced.
    """
    workflow_name = workflow_name_for(scope)
    marker = load_marker(cursors_fs, workflow_name)

    ledgers = list_consolidated_ledgers(ledger_fs, extract_workflow_name_for(scope))
    unconsumed = [entry for entry in ledgers if entry.name > marker.consumed_through]
    chosen = unconsumed[:max_extract_runs]

    units: list[TransformUnit] = []
    for entry in chosen:
        for descriptor in read_completed_descriptors(ledger_fs, entry.path):
            unit = _descriptor_to_unit(descriptor)
            if unit is not None:
                units.append(unit)

    next_marker = chosen[-1].name if chosen else marker.consumed_through
    return units, next_marker
