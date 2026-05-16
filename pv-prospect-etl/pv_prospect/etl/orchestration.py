import hashlib
import json
from datetime import datetime, timezone
from typing import Callable, Literal, Mapping

from pv_prospect.etl.storage import FileSystem
from pv_prospect.etl.storage.ledger import (
    ledger_entry_path,
    ledger_prefix,
    list_consolidated_ledgers,
)

NowFn = Callable[[], datetime]
TaskStatus = Literal['completed', 'failed']


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ledger_line(line: str) -> dict[str, object] | None:
    try:
        rec = json.loads(line)
    except json.JSONDecodeError:
        return None
    return rec if isinstance(rec, dict) else None


def _has_completed_entry(content: str) -> bool:
    for line in content.splitlines():
        rec = _parse_ledger_line(line)
        if rec is not None and rec.get('status') == 'completed':
            return True
    return False


def compute_task_hash(task_env: list[dict[str, str]]) -> str:
    """Generate a unique hash for a task based on its environment variables.

    The hash deliberately excludes ``TASK_HASH`` (so reinjecting one doesn't
    change the hash) and ``RUN_DATE`` (so two runs of the same data-window task
    on different days share a hash, enabling cross-day resume).
    """
    ignored = {'TASK_HASH', 'RUN_DATE'}
    sorted_items = sorted(
        (e['name'], e['value']) for e in task_env if e['name'] not in ignored
    )
    task_str = json.dumps(sorted_items)
    return hashlib.sha256(task_str.encode()).hexdigest()


def inject_task_hash(task_env: list[dict[str, str]]) -> list[dict[str, str]]:
    """Inject a TASK_HASH environment variable into the task."""
    task_hash = compute_task_hash(task_env)
    # Return a new list to avoid mutating the original
    return [e for e in task_env if e['name'] != 'TASK_HASH'] + [
        {'name': 'TASK_HASH', 'value': task_hash}
    ]


def build_env_list(**kwargs: str) -> list[dict[str, str]]:
    """Helper to build a list of environment variable dicts."""
    return [{'name': k, 'value': v} for k, v in kwargs.items()]


class WorkflowOrchestrator:
    """Coordinates a single Cloud Workflow execution.

    A workflow execution is identified by its *run date* — the UTC date on
    which the workflow was triggered, pinned once at the workflow's ``init``
    step and propagated to every task as ``RUN_DATE``. Both the manifest
    and the JSONL ledger are keyed by run date, so all artefacts produced
    by one execution share a single date prefix.

    The run date is distinct from ``START_DATE``/``END_DATE``, which are
    arguments describing the *data window* being processed. Those live in
    each ledger entry's ``descriptor`` field, not in the path.

    Each task records its outcome (``completed`` or ``failed``) through
    :meth:`record_outcome`. ``filter_remaining_tasks`` consults the ledger
    (across all past run dates) to skip already-completed tasks on re-run,
    so the ledger doubles as both audit trail and resumption mechanism.

    When no ``ledger_fs`` is configured, :meth:`record_outcome` is a no-op
    and :meth:`filter_remaining_tasks` returns every task — workflows still
    run but resumption is disabled.
    """

    def __init__(
        self,
        workflow_name: str,
        run_date: str,
        manifests_fs: FileSystem | None = None,
        ledger_fs: FileSystem | None = None,
        run_label: str = '',
    ):
        self.workflow_name = workflow_name
        self.run_date = run_date
        self.manifests_fs = manifests_fs
        self.ledger_fs = ledger_fs
        self.run_label = run_label
        self.manifest_path = f'{run_date}/{workflow_name}.json'

    def record_outcome(
        self,
        task_hash: str,
        descriptor: Mapping[str, str],
        status: TaskStatus,
        error: str | None = None,
        now: NowFn = _utc_now,
    ) -> None:
        """Append a JSONL ledger entry recording the outcome of a task.

        Each entry has the schema::

            {
                "recorded_at": "<ISO 8601 UTC>",
                "run_date": "<YYYY-MM-DD>",
                "workflow": "<workflow_name>",
                "task_hash": "<sha256>",
                "descriptor": { ... opaque per-workflow keys ... },
                "status": "completed" | "failed",
                "error": "<repr of exception, only for failed>"
            }

        No-op if no ``ledger_fs`` was configured or if *task_hash* is empty.
        Idempotent on completed entries: a second 'completed' record for
        the same task hash within the same run is not appended. Failures
        are always appended (a task may be retried in-run and end up with
        multiple 'failed' entries plus a final 'completed').
        """
        if self.ledger_fs is None or not task_hash:
            return
        path = ledger_entry_path(
            self.run_date, self.workflow_name, task_hash, self.run_label
        )
        if status == 'completed' and self.ledger_fs.exists(path):
            if _has_completed_entry(self.ledger_fs.read_text(path)):
                return
        entry: dict[str, object] = {
            'recorded_at': now().isoformat(),
            'run_date': self.run_date,
            'workflow': self.workflow_name,
            'task_hash': task_hash,
            'descriptor': dict(descriptor),
            'status': status,
        }
        if error is not None:
            entry['error'] = error
        self.ledger_fs.append_text(path, json.dumps(entry) + '\n')

    def filter_remaining_tasks(
        self, all_tasks: list[list[dict[str, str]]]
    ) -> list[list[dict[str, str]]]:
        """Filter out tasks whose ledger shows a 'completed' entry.

        Identifies each task by :func:`compute_task_hash` over its env
        (excluding any pre-injected ``TASK_HASH`` and the run date). See
        :meth:`completed_task_hashes` for the ledger scan semantics.
        """
        completed = self.completed_task_hashes()
        return [task for task in all_tasks if compute_task_hash(task) not in completed]

    def completed_task_hashes(self) -> set[str]:
        """Return the set of task hashes recorded as ``completed``.

        Scans both per-task ledger files in the current run
        (``<run_date>/<workflow>/<hash>.jsonl``) and every consolidated
        ledger file ever produced for this workflow
        (``<date>/<date>-*-<workflow>.jsonl``). Pooling completed task
        hashes across all past runs preserves cross-day resume: a task
        that finished yesterday under a different ``run_date`` is still
        reflected here today.

        Returns an empty set when ``ledger_fs`` is None or no ledger
        files exist.
        """
        ledger_fs = self.ledger_fs
        if ledger_fs is None:
            return set()
        completed: set[str] = set()
        per_task_dir = ledger_prefix(self.run_date, self.workflow_name, self.run_label)
        for entry in ledger_fs.list_files(per_task_dir, '*.jsonl'):
            if _has_completed_entry(ledger_fs.read_text(entry.path)):
                completed.add(entry.name.removesuffix('.jsonl'))
        for entry in list_consolidated_ledgers(ledger_fs, self.workflow_name):
            for line in ledger_fs.read_text(entry.path).splitlines():
                rec = _parse_ledger_line(line)
                if rec is None:
                    continue
                if rec.get('status') == 'completed':
                    task_hash = rec.get('task_hash')
                    if isinstance(task_hash, str) and task_hash:
                        completed.add(task_hash)
        return completed

    def write_manifest(self, phases: list[list[list[dict[str, str]]]]) -> None:
        """Write the phases and tasks to the manifest file."""
        if self.manifests_fs is None:
            raise RuntimeError(
                'WorkflowOrchestrator.write_manifest called without manifests_fs'
            )
        self.manifests_fs.write_text(self.manifest_path, json.dumps({'phases': phases}))

    def phase_part_file_name(self, phase_index: int, part_index: int) -> str:
        """Filename (relative to the manifests filesystem run-date directory)
        of one chunked phase-part file written by
        :meth:`write_phased_manifest`. Phases that exceed
        :data:`TASKS_PER_PHASE_FILE` rows are sharded into multiple parts
        so each per-step HTTP fetch stays well under the Workflows 2 MiB
        response limit."""
        return f'{self.workflow_name}.phase-{phase_index}.part-{part_index}.json'

    def write_phased_manifest(self, phases: list[list[list[dict[str, str]]]]) -> None:
        """Write a v2 phased manifest, split across an index + chunked phase files.

        Hoists each phase's constant env-vars into a phase-level
        ``common_env`` and stores only the varying fields as positional
        ``rows`` aligned to a sorted ``task_keys`` schema. Each phase's
        rows are then sharded into chunks of at most
        :data:`TASKS_PER_PHASE_FILE` so any single per-step HTTP fetch
        stays well under the Cloud Workflows 2 MiB response limit — the
        weather-grid transform backfill reaches ~20 K tasks per phase
        once a few extract ledgers pile up, which a single un-sharded
        phase file blows past on its own.

        File layout under ``manifests_fs``:

        * ``<run_date>/<workflow>.json`` -- the index document. Each
          phase entry carries ``common_env``, ``task_keys`` and a
          ``files`` list naming the chunked phase-part files in order.
        * ``<run_date>/<workflow>.phase-<N>.part-<M>.json`` -- one per
          chunk; each holds ``{"rows": [...]}``.

        The Cloud Workflow fetches the index, then for each phase
        iterates its ``files`` list in order: fetches each part,
        reconstructs every task's env as
        ``common_env + zip(task_keys, row)``, and parallel-dispatches.
        """
        if self.manifests_fs is None:
            raise RuntimeError(
                'WorkflowOrchestrator.write_phased_manifest called without manifests_fs'
            )
        phase_entries, phase_chunks = pack_phases(phases)
        files_per_phase: list[list[str]] = [
            [
                self.phase_part_file_name(phase_index, part_index)
                for part_index in range(len(chunks))
            ]
            for phase_index, chunks in enumerate(phase_chunks)
        ]
        for entry, files in zip(phase_entries, files_per_phase, strict=True):
            entry['files'] = files
        index_doc: dict[str, object] = {
            'version': PHASED_MANIFEST_VERSION,
            'phases': phase_entries,
        }
        self.manifests_fs.write_text(self.manifest_path, json.dumps(index_doc))
        for files, chunks in zip(files_per_phase, phase_chunks, strict=True):
            for file_name, chunk in zip(files, chunks, strict=True):
                self.manifests_fs.write_text(
                    f'{self.run_date}/{file_name}', json.dumps(chunk)
                )


PHASED_MANIFEST_VERSION = 2

# Maximum rows per chunked phase-part file. Two distinct Cloud Workflows
# runtime ceilings drive this — the binding one in practice is memory,
# not the HTTP-response limit:
#
#   * 2 MiB per-step HTTP-response limit. Row encoding sits at ~130 B
#     each (5 keys × ~25 B), so even 5000 rows ≈ 650 KiB stays well
#     under this limit on its own.
#
#   * 256 MiB per-execution memory limit. Each iteration of the inner
#     `parallel: for: in: rows` block fans out one workflow branch per
#     row, and every branch holds its own `task_op` / `task_op_done` /
#     `exec` (Cloud Run API responses ~5–15 KiB each, plus framework
#     overhead). The retained per-branch state of 5000 branches sums to
#     well over the 256 MiB budget — which is what the weather-grid
#     transform backfill blew past at ~20 K tasks/phase. Keeping each
#     chunk's parallel block to a few hundred branches at most bounds
#     this. 500 × ~30 KiB per branch ≈ 15 MiB per chunk; the parallel
#     block's memory is reclaimed when the block completes, so the
#     ceiling is per-chunk, not cumulative.
TASKS_PER_PHASE_FILE = 500


def pack_phases(
    phases: list[list[list[dict[str, str]]]],
) -> tuple[list[dict[str, object]], list[list[dict[str, object]]]]:
    """Pack *phases* into the v2 manifest's index + chunked per-phase shapes.

    For each phase, hoists env entries that are identical across every
    task into a phase-level ``common_env`` and re-encodes the remaining
    varying fields as positional ``rows`` aligned to a sorted ``task_keys``
    schema. The rows are then sliced into chunks of at most
    :data:`TASKS_PER_PHASE_FILE`, each of which lands in its own phase-
    part file. Hoisting compacts the row encoding; chunking bounds the
    file size regardless of how many tasks pile up — together they keep
    any single per-step HTTP fetch under the Cloud Workflows 2 MiB
    response limit.

    Tolerates heterogeneous task keys within a phase by using the union
    of keys for ``task_keys`` and padding missing values with ``None``
    (serialised as JSON ``null``). The Workflows expander skips ``null``
    cells so the corresponding env-var is omitted from the Cloud Run
    container — preserving the legacy "absent key" semantics rather than
    surfacing an empty string. A key is hoisted into ``common_env`` only
    when every task carries it with the same value.

    Returns ``(phase_entries, phase_chunks)`` as two parallel lists
    indexed by phase. ``phase_chunks[i]`` is a list of one or more
    ``{"rows": [...]}`` dicts (an empty phase contributes a single
    empty chunk so the index still names one file per phase). The
    writer attaches per-chunk filenames to each entry's ``files`` list
    and wraps the entries under a ``version``/``phases`` index document.
    """
    phase_entries: list[dict[str, object]] = []
    phase_chunks: list[list[dict[str, object]]] = []
    for phase in phases:
        common_env, task_keys, rows = _pack_phase(phase)
        chunks = _chunk_rows(rows, TASKS_PER_PHASE_FILE)
        phase_entries.append({'common_env': common_env, 'task_keys': task_keys})
        phase_chunks.append([{'rows': chunk} for chunk in chunks])
    return phase_entries, phase_chunks


def _chunk_rows(
    rows: list[list[str | None]], chunk_size: int
) -> list[list[list[str | None]]]:
    """Slice *rows* into chunks of at most *chunk_size*; always at least one
    chunk so an empty phase still produces a single empty file."""
    if not rows:
        return [[]]
    return [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]


def _pack_phase(
    phase: list[list[dict[str, str]]],
) -> tuple[list[dict[str, str]], list[str], list[list[str | None]]]:
    if not phase:
        return [], [], []
    task_dicts = [{e['name']: e['value'] for e in task} for task in phase]
    all_keys: set[str] = set().union(*(set(td) for td in task_dicts))
    common: dict[str, str] = {}
    for key in all_keys:
        first = task_dicts[0].get(key)
        if first is None:
            continue
        if all(td.get(key) == first for td in task_dicts[1:]):
            common[key] = first
    varying = sorted(all_keys - set(common))
    common_env = [{'name': k, 'value': common[k]} for k in sorted(common)]
    rows: list[list[str | None]] = [[td.get(k) for k in varying] for td in task_dicts]
    return common_env, varying, rows
