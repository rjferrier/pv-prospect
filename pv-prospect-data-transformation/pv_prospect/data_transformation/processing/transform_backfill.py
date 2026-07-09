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
from datetime import date

from pv_prospect.data_sources import DataSourceType
from pv_prospect.etl import (
    BackfillScope,
    PVSlice,
    WeatherSlice,
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
    """A specification of one extracted PV-related dataset for the daily
    transform to process.

    It identifies a body of raw extracted data and nothing more — a
    ``data_source_type``, a PV system id, and a date-range window. It is
    the *input* to :func:`build_transform_phases`, not a node in the
    transform DAG: which clean / prepare / assemble steps run, and how
    they feed one another, is the planner's knowledge, not this type's.

    ``data_source_type`` is the API the raw data came from
    (``PV`` for PVOutput, ``WEATHER`` for the PV-site OpenMeteo data
    that joins onto PV via ``prepare_pv``). ``location`` is set only
    for the daily transform's ad-hoc per-location weather cleans (not
    used in production scheduling). ``start_date`` is inclusive and
    ``end_date`` exclusive; ``end_date`` is ``None`` for the daily
    single-day transform.

    The grid-weather backfill no longer produces TransformInputs — it
    plans slices directly via :func:`plan_slices`.
    """

    data_source_type: DataSourceType
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
    transform_inputs: list[TransformInput],
    workflow_name: str,
    run_date: str,
) -> list[list[list[dict[str, str]]]]:
    """Plan the clean / prepare / assemble phases for *transform_inputs*.

    Used by the daily transform's :func:`_run_plan_transform` to write
    the phased manifest. Each PV input yields ``clean_pv`` and
    ``prepare_pv`` tasks; each weather input yields ``clean_weather``
    only — its cleaned output is read back by ``prepare_pv`` by path
    convention. Phase 2 emits one ``assemble_pv`` per distinct
    ``(pv_system_id, start_date, end_date)`` — per-slice so distinct
    slices' task hashes don't collide.

    The cross-input dependency from ``prepare_pv`` to a separate
    PV-site weather ``clean_weather`` is modelled nowhere; it holds
    only because every clean runs before any prepare, and because
    ``run_prepare_pv`` reads the cleaned weather by path convention.
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
        )

    clean: list[list[dict[str, str]]] = []
    prepare: list[list[dict[str, str]]] = []
    # Assembly dedup keyed by ``(pv_system_id, start_date, end_date)`` so
    # a previously-completed assemble for one slice doesn't filter out an
    # unrelated slice on a later run via the orchestrator's cross-run
    # task-hash scan.
    assemble_pv_inputs: dict[tuple[int, str, str | None], TransformInput] = {}
    for transform_input in transform_inputs:
        if transform_input.data_source_type == DataSourceType.PV:
            clean.append(task(transform_input, 'clean_pv'))
            prepare.append(task(transform_input, 'prepare_pv'))
            if transform_input.pv_system_id is not None:
                assemble_pv_inputs.setdefault(
                    (
                        transform_input.pv_system_id,
                        transform_input.start_date,
                        transform_input.end_date,
                    ),
                    transform_input,
                )
        else:
            # PV-site weather: cleaned only. Its cleaned output is the join
            # input prepare_pv reads — an implicit edge (see the docstring).
            clean.append(task(transform_input, 'clean_weather'))

    assemble: list[list[dict[str, str]]] = [
        task(transform_input, 'assemble_pv')
        for transform_input in assemble_pv_inputs.values()
    ]

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


def _consume_extract_descriptors(
    scope: BackfillScope,
    ledger_fs: FileSystem,
    cursors_fs: FileSystem,
    max_extract_runs: int,
) -> tuple[list[dict[str, str]], str]:
    """Return the next ``max_extract_runs`` worth of completed descriptors.

    Lists the extraction backfill's consolidated ledgers, keeps those
    whose filename sorts above the live marker, takes the oldest
    *max_extract_runs* of them, and concatenates their ``completed``
    descriptors. Returns ``(descriptors, next_marker)``; the caller
    decides what to do with each descriptor.

    ``next_marker`` is the filename of the last consolidated ledger
    consumed — what callers should later promote via
    :func:`save_marker` once their run succeeds. When no unconsumed
    ledgers exist, returns an empty descriptor list and the unchanged
    marker, so callers can no-op cleanly.

    The marker doubles as the listing's lower bound: everything at or
    below it is discarded anyway, so there is nothing to gain from
    scanning the ledger history that precedes it.
    """
    workflow_name = workflow_name_for(scope)
    marker = load_marker(cursors_fs, workflow_name)

    ledgers = list_consolidated_ledgers(
        ledger_fs, extract_workflow_name_for(scope), since=marker.consumed_through
    )
    unconsumed = [entry for entry in ledgers if entry.name > marker.consumed_through]
    chosen = unconsumed[:max_extract_runs]

    descriptors: list[dict[str, str]] = []
    for entry in chosen:
        descriptors.extend(read_completed_descriptors(ledger_fs, entry.path))

    next_marker = chosen[-1].name if chosen else marker.consumed_through
    return descriptors, next_marker


def _descriptor_to_pv_slice_key(
    descriptor: dict[str, str],
) -> tuple[int, str, str] | None:
    """Reduce one PV-sites extraction descriptor to a PV-slice key.

    A PV slice is keyed by ``(pv_system_id, start_date, end_date)``.
    The PV-sites extraction backfill emits two descriptors per slice
    (one for the PV-data task, one for the weather-data task); both
    map to the same key so the caller can dedup. Returns ``None`` if
    the descriptor is missing any keying field.
    """
    pv_system_id = descriptor.get('pv_system_id')
    start_date = descriptor.get('start_date')
    end_date = descriptor.get('end_date')
    if pv_system_id is None or not start_date or not end_date:
        return None
    return int(pv_system_id), start_date, end_date


def _descriptor_to_weather_slice_key(
    descriptor: dict[str, str],
) -> tuple[int, str, str] | None:
    """Reduce one weather-grid extraction descriptor to a weather-slice key.

    A weather slice is keyed by ``(grid_point_sample_index, start_date,
    end_date)``. Each grid point in the sample produces its own
    descriptor; all share the slice key so the caller can dedup.
    Returns ``None`` if the descriptor is missing any keying field.
    """
    sample_index = descriptor.get('grid_point_sample_index')
    start_date = descriptor.get('start_date')
    end_date = descriptor.get('end_date')
    if sample_index is None or not start_date or not end_date:
        return None
    return int(sample_index), start_date, end_date


def plan_slices(
    scope: BackfillScope,
    ledger_fs: FileSystem,
    cursors_fs: FileSystem,
    max_extract_runs: int,
) -> tuple[list[PVSlice] | list[WeatherSlice], str]:
    """Plan *scope*'s slices from extraction's committed ledger.

    Reads the next ``max_extract_runs`` worth of completed extraction
    descriptors and groups them into :class:`PVSlice` or
    :class:`WeatherSlice` values — one per distinct
    ``(identifier, start, end)`` key. The slice list is deduplicated
    and ordered as the keys are first encountered in the descriptor
    stream. The slice task body checks raw-file existence per input
    and routes to a ``partial`` ledger outcome if anything's missing,
    so a slice is planned as long as *any* of its descriptors was
    recorded completed.

    Returns ``(slices, next_marker)``. The caller runs the slices and
    only then advances the live marker via :func:`save_marker`. When
    no unconsumed ledgers exist, returns an empty list and the
    unchanged marker.

    Pure planning — no manifests are written and no marker is advanced.
    """
    descriptors, next_marker = _consume_extract_descriptors(
        scope, ledger_fs, cursors_fs, max_extract_runs
    )
    if scope is BackfillScope.PV_SITES:
        pv_keys: dict[tuple[int, str, str], None] = {}
        for descriptor in descriptors:
            key = _descriptor_to_pv_slice_key(descriptor)
            if key is not None:
                pv_keys.setdefault(key, None)
        pv_slices = [
            PVSlice(
                pv_system_id=sid,
                start_date=date.fromisoformat(start),
                end_date=date.fromisoformat(end),
            )
            for sid, start, end in pv_keys
        ]
        return pv_slices, next_marker
    if scope is BackfillScope.WEATHER_GRID:
        weather_keys: dict[tuple[int, str, str], None] = {}
        for descriptor in descriptors:
            key = _descriptor_to_weather_slice_key(descriptor)
            if key is not None:
                weather_keys.setdefault(key, None)
        weather_slices = [
            WeatherSlice(
                grid_point_sample_index=sample_idx,
                start_date=date.fromisoformat(start),
                end_date=date.fromisoformat(end),
            )
            for sample_idx, start, end in weather_keys
        ]
        return weather_slices, next_marker
    raise ValueError(f'Unknown backfill scope: {scope}')
