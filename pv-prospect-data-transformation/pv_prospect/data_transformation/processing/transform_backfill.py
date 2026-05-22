"""Transform-backfill planning for the data-transformation pipeline.

The transform backfill plans from the *extraction* backfill's durable
record rather than from a cursor of its own. The extraction backfill's
consolidated ledger already enumerates exactly which ``(site/location,
data_source, window)`` tuples have raw data; this module reads those
``completed`` entries and turns them into transform tasks.

What this module owns:

  * the :class:`TransformInput` value type and
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
class TransformInput:
    """A specification of one extracted dataset for the transform to process.

    It identifies a body of raw extracted data and nothing more — a
    ``data_source_type``, an entity, and a date-range window. It is the
    *input* to :func:`build_transform_phases`, not a node in the transform
    DAG: which clean / prepare / assemble steps run, and how they feed one
    another, is the planner's knowledge, not this type's.

    ``data_source_type`` is the API the raw data came from. Exactly one of
    ``pv_system_id`` / ``location`` identifies the entity — a ``lat,lon``
    string for grid-point weather, a system id otherwise.
    ``grid_point_sample_index`` is set only for weather from the
    weather-grid backfill: the grid-point sample file the data belongs to,
    and the key its weather partition file groups by. ``start_date`` is
    inclusive and ``end_date`` exclusive; ``end_date`` is ``None`` for the
    daily single-day transform.
    """

    data_source_type: DataSourceType
    start_date: str
    end_date: str | None = None
    pv_system_id: int | None = None
    location: str | None = None
    grid_point_sample_index: int | None = None


def _transform_task_env(
    transform_step: str,
    start_date_str: str,
    end_date_str: str | None,
    workflow_name: str,
    run_date: str,
    pv_system_id: int | None = None,
    location: str | None = None,
    grid_point_sample_index: int | None = None,
) -> list[dict[str, str]]:
    """Build the env list for one transform task, with TASK_HASH injected.

    ``END_DATE`` is included only when *end_date_str* is set, so the
    container processes the whole ``[START_DATE, END_DATE)`` window. When
    omitted the container falls back to a single day — the daily-transform
    behaviour. ``GRID_POINT_SAMPLE_INDEX`` is included only for grid-weather
    units; ``prepare_weather`` keys its prepared rows by it.
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
    if grid_point_sample_index is not None:
        env['GRID_POINT_SAMPLE_INDEX'] = str(grid_point_sample_index)
    return inject_task_hash(build_env_list(**env))


def build_transform_phases(
    transform_inputs: list[TransformInput],
    workflow_name: str,
    run_date: str,
) -> list[list[list[dict[str, str]]]]:
    """Plan the clean / prepare / assemble phases for *transform_inputs*.

    This is where the transform DAG lives: :class:`TransformInput` only
    describes raw inputs, so the planner derives every step from them.
    Each input yields a ``clean_{source}`` task carrying its own
    ``[START_DATE, END_DATE)`` window, so one run can mix windows
    (weather-grid extraction's diagonal march through ``(sample_file,
    date)`` space). A ``'pv'`` input and a grid-weather input also yield a
    prepare task; a PV-site-weather input (no ``grid_point_sample_index``)
    yields ``clean_weather`` only. Phase 2 emits one ``assemble_pv`` per
    distinct ``pv_system_id`` and a single ``assemble_weather`` when any
    grid-weather input is present.

    The phases run in order, every task in one finishing before the next.
    That ordering is load-bearing, and it hides a dependency: ``prepare_pv``
    joins cleaned PV power with cleaned on-site weather, but that cleaned
    weather is produced by a *separate* PV-site-weather input's
    ``clean_weather`` task. That cross-input edge is modelled nowhere — it
    holds only because every clean runs before any prepare, and because
    ``run_prepare_pv`` reads the cleaned weather by path convention (see
    the package README's pipeline section).

    An assemble task ignores its date window but carries the first-seen
    relevant input's window so its task hash is deterministic. The
    returned phases are unfiltered — the caller applies
    :meth:`WorkflowOrchestrator.filter_remaining_tasks` if it wants
    self-filtering (the daily transform does; the backfill does not).
    """

    def task(
        transform_input: TransformInput, transform_step: str
    ) -> list[dict[str, str]]:
        return _transform_task_env(
            transform_step,
            transform_input.start_date,
            transform_input.end_date,
            workflow_name,
            run_date,
            pv_system_id=transform_input.pv_system_id,
            location=transform_input.location,
            grid_point_sample_index=transform_input.grid_point_sample_index,
        )

    clean: list[list[dict[str, str]]] = []
    prepare: list[list[dict[str, str]]] = []
    assemble_pv_inputs: dict[int, TransformInput] = {}
    first_weather_input: TransformInput | None = None
    for transform_input in transform_inputs:
        if transform_input.data_source_type == DataSourceType.PV:
            clean.append(task(transform_input, 'clean_pv'))
            prepare.append(task(transform_input, 'prepare_pv'))
            if transform_input.pv_system_id is not None:
                assemble_pv_inputs.setdefault(
                    transform_input.pv_system_id, transform_input
                )
        elif transform_input.grid_point_sample_index is not None:
            # Grid-point weather — a sample index is present only on weather
            # from the weather-grid backfill, which is destined for the
            # weather model: cleaned, prepared and assembled.
            clean.append(task(transform_input, 'clean_weather'))
            prepare.append(task(transform_input, 'prepare_weather'))
            if first_weather_input is None:
                first_weather_input = transform_input
        else:
            # PV-site weather: cleaned only. Its cleaned output is the join
            # input prepare_pv reads — an implicit edge (see the docstring).
            clean.append(task(transform_input, 'clean_weather'))

    assemble: list[list[dict[str, str]]] = [
        task(transform_input, 'assemble_pv')
        for transform_input in assemble_pv_inputs.values()
    ]
    if first_weather_input is not None:
        assemble.append(
            _transform_task_env(
                'assemble_weather',
                first_weather_input.start_date,
                first_weather_input.end_date,
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


def _descriptor_to_unit(descriptor: dict[str, str]) -> TransformInput | None:
    """Map one extraction ledger descriptor to a :class:`TransformInput`.

    The extraction backfill records, per completed task, a descriptor of
    ``{data_source, start_date, end_date?, pv_system_id | location,
    grid_point_sample_index?}``, where ``data_source`` carries the
    :class:`DataSourceType` value (``'pv'`` / ``'weather'``) propagated
    from the extract job's ``DATA_SOURCE`` env var — *not* the concrete
    :class:`DataSource` (e.g. ``'pvoutput'``). ``grid_point_sample_index``
    is set only by the weather-grid backfill. Returns ``None`` for a
    descriptor missing its dates, its data source, or an identifier, so a
    malformed entry becomes a skipped hole rather than a crash.
    """
    start_date = descriptor.get('start_date')
    if not start_date:
        return None
    end_date = descriptor.get('end_date')
    try:
        data_source_type = DataSourceType(descriptor.get('data_source'))
    except ValueError:
        return None
    grid_point_sample_index_str = descriptor.get('grid_point_sample_index')
    grid_point_sample_index = (
        int(grid_point_sample_index_str)
        if grid_point_sample_index_str is not None
        else None
    )
    pv_system_id = descriptor.get('pv_system_id')
    if pv_system_id is not None:
        return TransformInput(
            data_source_type, start_date, end_date, pv_system_id=int(pv_system_id)
        )
    location = descriptor.get('location')
    if location is not None:
        return TransformInput(
            data_source_type,
            start_date,
            end_date,
            location=location,
            grid_point_sample_index=grid_point_sample_index,
        )
    return None


def plan_units(
    scope: BackfillScope,
    ledger_fs: FileSystem,
    cursors_fs: FileSystem,
    max_extract_runs: int,
) -> tuple[list[TransformInput], str]:
    """Plan *scope*'s transform inputs from extraction's committed ledger.

    Lists the extraction backfill's consolidated ledgers, keeps those
    above the live marker, takes the oldest *max_extract_runs* of them,
    and turns every ``completed`` entry into a :class:`TransformInput`.
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

    transform_inputs: list[TransformInput] = []
    for entry in chosen:
        for descriptor in read_completed_descriptors(ledger_fs, entry.path):
            transform_input = _descriptor_to_unit(descriptor)
            if transform_input is not None:
                transform_inputs.append(transform_input)

    next_marker = chosen[-1].name if chosen else marker.consumed_through
    return transform_inputs, next_marker
