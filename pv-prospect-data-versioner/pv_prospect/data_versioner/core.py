"""Core orchestration for the data versioning pipeline."""

import logging
import os
import tempfile
from datetime import date

from pv_prospect.data_versioner.config import DataVersionerConfig
from pv_prospect.data_versioner.dvc_ops import dvc_add_files, dvc_push
from pv_prospect.data_versioner.git_ops import (
    clone_instance_repo,
    git_commit_and_tag,
    git_push,
    set_commit_identity,
    setup_ssh,
)
from pv_prospect.data_versioner.readiness import verify_readiness
from pv_prospect.etl.storage import FileSystem

logger = logging.getLogger(__name__)


def version_data(
    prepared_fs: FileSystem,
    cleaned_fs: FileSystem,
    batches_fs: FileSystem,
    config: DataVersionerConfig,
    deploy_key: str,
    version_date: date,
) -> None:
    """Orchestrate the full data versioning flow.

    1. Verify prepared data readiness
    2. Set up SSH and clone the instance repo
    3. Download prepared CSVs into the clone
    4. DVC add and push
    5. Git commit, tag, and push
    6. Clean staging (cleaned + prepared)
    """
    file_paths = verify_readiness(prepared_fs, batches_fs)

    tag = f'data-v{version_date.isoformat()}'
    message = f'Version prepared data {version_date.isoformat()}'

    with tempfile.TemporaryDirectory() as work_dir:
        env = setup_ssh(deploy_key, work_dir)
        clone_dir = os.path.join(work_dir, 'instance')

        repo = clone_instance_repo(
            config.instance_repo_url,
            config.instance_repo_branch,
            clone_dir,
            env,
        )
        set_commit_identity(repo, config.commit_author_name, config.commit_author_email)

        _download_prepared_files(
            prepared_fs, clone_dir, config.prepared_data_dir, file_paths
        )

        dvc_file_paths = dvc_add_files(clone_dir, config.prepared_data_dir, file_paths)
        dvc_push(clone_dir, config.dvc_remote_name, dvc_file_paths)

        git_commit_and_tag(repo, dvc_file_paths, tag, message)
        git_push(repo, env)

    _clean_staging(prepared_fs, cleaned_fs)
    logger.info('Versioning complete: %s', tag)


def _download_prepared_files(
    prepared_fs: FileSystem,
    clone_dir: str,
    prepared_data_dir: str,
    file_paths: list[str],
) -> None:
    """Download prepared CSVs from staging into the cloned repo."""
    for rel_path in file_paths:
        local_path = os.path.join(clone_dir, prepared_data_dir, rel_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        content = prepared_fs.read_bytes(rel_path)
        with open(local_path, 'wb') as f:
            f.write(content)
        logger.info('Downloaded %s (%d bytes)', rel_path, len(content))


def _clean_staging(
    prepared_fs: FileSystem,
    cleaned_fs: FileSystem,
) -> None:
    """Delete all files from the cleaned and prepared staging prefixes."""
    for fs, label in [(prepared_fs, 'prepared'), (cleaned_fs, 'cleaned')]:
        entries = fs.list_files('', recursive=True)
        for entry in entries:
            fs.delete(entry.path)
        logger.info('Deleted %d file(s) from %s staging', len(entries), label)
