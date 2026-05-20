import hashlib
import json
from datetime import datetime, timezone
from typing import Callable, Literal, Mapping

from pv_prospect.etl.storage import FileSystem
from pv_prospect.etl.storage.ledger import (
    LedgerCollector,
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

    A *distributed* workflow leaves ``ledger_collector`` unset:
    :meth:`record_outcome` writes one per-task file, later merged by
    :func:`consolidate_ledger`. A *single-process* workflow passes a
    :class:`LedgerCollector`; :meth:`record_outcome` then buffers
    outcomes in memory for a single end-of-run flush, with no per-task
    file fan-out. ``ledger_fs`` is still used in that mode for the
    cross-day consolidated-ledger scan in :meth:`completed_task_hashes`.

    When neither ``ledger_fs`` nor ``ledger_collector`` is configured,
    :meth:`record_outcome` is a no-op and :meth:`filter_remaining_tasks`
    returns every task — workflows still run but resumption is disabled.
    """

    def __init__(
        self,
        workflow_name: str,
        run_date: str,
        manifests_fs: FileSystem | None = None,
        ledger_fs: FileSystem | None = None,
        run_label: str = '',
        ledger_collector: LedgerCollector | None = None,
    ):
        self.workflow_name = workflow_name
        self.run_date = run_date
        self.manifests_fs = manifests_fs
        self.ledger_fs = ledger_fs
        self.run_label = run_label
        self.ledger_collector = ledger_collector
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

        No-op if *task_hash* is empty or neither ``ledger_fs`` nor
        ``ledger_collector`` was configured. With a ``ledger_collector``
        the entry is buffered in memory; otherwise it is appended to the
        task's per-task ledger file.

        Idempotent on completed entries: a second 'completed' record for
        the same task hash within the same run is not appended. Failures
        are always appended (a task may be retried in-run and end up with
        multiple 'failed' entries plus a final 'completed').
        """
        ledger_fs = self.ledger_fs
        collector = self.ledger_collector
        if not task_hash or (ledger_fs is None and collector is None):
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
        if collector is not None:
            collector.record(entry)
            return
        if ledger_fs is None:
            return
        path = ledger_entry_path(
            self.run_date, self.workflow_name, task_hash, self.run_label
        )
        if status == 'completed' and ledger_fs.exists(path):
            if _has_completed_entry(ledger_fs.read_text(path)):
                return
        ledger_fs.append_text(path, json.dumps(entry) + '\n')

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

        Scans every consolidated ledger file ever produced for this
        workflow (``<date>/<date>-*-<workflow>.jsonl``), and — unless a
        ``ledger_collector`` is in use — the per-task ledger files of the
        current run (``<run_date>/<workflow>/<hash>.jsonl``). Pooling
        completed task hashes across all past runs preserves cross-day
        resume: a task that finished yesterday under a different
        ``run_date`` is still reflected here today.

        The per-task scan is skipped with a ``ledger_collector``: that
        mode writes no per-task files, and a fresh single-process run
        starts with an empty in-memory buffer, so the consolidated
        ledgers are the only durable resume source.

        Returns an empty set when ``ledger_fs`` is None or no ledger
        files exist.
        """
        ledger_fs = self.ledger_fs
        if ledger_fs is None:
            return set()
        completed: set[str] = set()
        if self.ledger_collector is None:
            per_task_dir = ledger_prefix(
                self.run_date, self.workflow_name, self.run_label
            )
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
