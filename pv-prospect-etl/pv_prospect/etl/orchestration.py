import hashlib
import json
from datetime import datetime, timezone
from typing import Callable, Literal, Mapping

from pv_prospect.etl.storage import FileSystem
from pv_prospect.etl.storage.ledger import ledger_entry_path, ledger_prefix

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
    """Generate a unique hash for a task based on its environment variables."""
    # Hash everything except TASK_HASH if it's already there
    sorted_items = sorted(
        (e['name'], e['value']) for e in task_env if e['name'] != 'TASK_HASH'
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
    """Orchestrates job execution by writing manifests and tracking task outcomes
    via the JSONL ledger on *log_fs*.

    Each task records its outcome (``completed`` or ``failed``) through
    :meth:`record_outcome`. ``filter_remaining_tasks`` consults the same ledger
    to skip already-completed tasks on re-run, so the ledger doubles as both
    audit trail and resumption mechanism.

    When no *log_fs* is configured, ``record_outcome`` is a no-op and
    ``filter_remaining_tasks`` returns every task — i.e. resumption is disabled
    but workflows still run.
    """

    def __init__(
        self,
        resources_fs: FileSystem,
        workflow_name: str,
        run_date: str,
        log_fs: FileSystem | None = None,
    ):
        self.fs = resources_fs
        self.log_fs = log_fs
        self.workflow_name = workflow_name
        self.run_date = run_date
        self.manifest_path = f'manifests/{workflow_name}_{run_date}.json'

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

        No-op if no ``log_fs`` was configured or if *task_hash* is empty.
        Idempotent on completed entries: a second 'completed' record for
        the same task hash within the same run is not appended. Failures
        are always appended (a task may be retried in-run and end up with
        multiple 'failed' entries plus a final 'completed').
        """
        if self.log_fs is None or not task_hash:
            return
        path = ledger_entry_path(self.run_date, self.workflow_name, task_hash)
        if status == 'completed' and self.log_fs.exists(path):
            if _has_completed_entry(self.log_fs.read_text(path)):
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
        self.log_fs.append_text(path, json.dumps(entry) + '\n')

    def filter_remaining_tasks(
        self, all_tasks: list[list[dict[str, str]]]
    ) -> list[list[dict[str, str]]]:
        """Filter out tasks whose ledger shows a 'completed' entry.

        Reads both per-task ledger files (``<run_date>/<workflow>/<hash>.jsonl``,
        present mid-run) and any consolidated ledger files (``<run_date>/
        <run_date>-<HHMMSS>-<workflow>.jsonl``, present once consolidation has
        run). When *log_fs* is None, no filtering happens — every task is
        returned, so the workflow proceeds without resumption.
        """
        completed = self._completed_task_hashes()
        remaining = []
        for task in all_tasks:
            thash = next((e['value'] for e in task if e['name'] == 'TASK_HASH'), None)
            if not thash or thash not in completed:
                remaining.append(task)
        return remaining

    def _completed_task_hashes(self) -> set[str]:
        log_fs = self.log_fs
        if log_fs is None:
            return set()
        completed: set[str] = set()
        per_task_dir = ledger_prefix(self.run_date, self.workflow_name)
        for entry in log_fs.list_files(per_task_dir, '*.jsonl'):
            if _has_completed_entry(log_fs.read_text(entry.path)):
                completed.add(entry.name.removesuffix('.jsonl'))
        consolidated_pattern = f'{self.run_date}-*-{self.workflow_name}.jsonl'
        for entry in log_fs.list_files(self.run_date, consolidated_pattern):
            for line in log_fs.read_text(entry.path).splitlines():
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
        self.fs.write_text(self.manifest_path, json.dumps({'phases': phases}))
