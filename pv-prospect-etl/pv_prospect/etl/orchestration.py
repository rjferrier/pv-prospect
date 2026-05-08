import hashlib
import json
from datetime import datetime, timezone
from typing import Callable, Literal, Mapping

from pv_prospect.etl.storage import FileSystem
from pv_prospect.etl.storage.ledger import ledger_entry_path

NowFn = Callable[[], datetime]
TaskStatus = Literal['completed', 'failed']


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
    """Orchestrates job execution by writing manifests and tracking completion via checkpoints.

    When a *log_fs* is provided, also appends per-task outcome entries to a
    JSONL ledger via :meth:`record_outcome`. The ledger captures both
    successes and failures and is intended to grow into the durable record
    of task outcomes; checkpoints remain the resumption mechanism for now.
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
        self.checkpoint_prefix = f'checkpoints/{workflow_name}/{run_date}'
        self.manifest_path = f'manifests/{workflow_name}_{run_date}.json'

    def mark_task_completed(self, task_hash: str) -> None:
        """Write a checkpoint file to mark a task as completed."""
        if not task_hash:
            return

        path = f'{self.checkpoint_prefix}/{task_hash}.json'
        if not self.fs.exists(path):
            self.fs.write_text(path, json.dumps({'status': 'completed'}))

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
            existing = self.log_fs.read_text(path)
            for line in existing.splitlines():
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get('status') == 'completed':
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
        """Filter out tasks that have already been marked as completed."""
        entries = self.fs.list_files(self.checkpoint_prefix, '*.json')
        completed_hashes = {
            entry.path.split('/')[-1].replace('.json', '') for entry in entries
        }

        remaining = []
        for task in all_tasks:
            # Extract task hash
            thash = next((e['value'] for e in task if e['name'] == 'TASK_HASH'), None)
            if thash and thash not in completed_hashes:
                remaining.append(task)
            elif not thash:
                # If a task somehow has no hash, run it to be safe
                remaining.append(task)

        return remaining

    def write_manifest(self, phases: list[list[list[dict[str, str]]]]) -> None:
        """Write the phases and tasks to the manifest file."""
        self.fs.write_text(self.manifest_path, json.dumps({'phases': phases}))
