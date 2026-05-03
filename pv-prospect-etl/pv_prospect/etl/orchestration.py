import hashlib
import json

from pv_prospect.etl.storage import FileSystem


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
    """Orchestrates job execution by writing manifests and tracking completion via checkpoints."""

    def __init__(self, resources_fs: FileSystem, workflow_name: str, run_date: str):
        self.fs = resources_fs
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
