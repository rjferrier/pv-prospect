"""DVC operations for data versioning."""

import logging
import os
from typing import Any

import yaml
from dvc.repo import Repo as DvcRepo

logger = logging.getLogger(__name__)

# DVC's scm_context emits interactive-user tips ("To track the changes with
# git, run: ...") after every add. Useless in a non-interactive job.
logging.getLogger('dvc.repo.scm_context').setLevel(logging.WARNING)


def inject_remote(dvc_data: dict[str, Any], remote_name: str) -> None:
    """Add ``remote`` to each entry in the ``outs`` list of a parsed .dvc file.

    DVC uses this field during ``dvc pull`` to resolve which remote to fetch
    from without requiring an explicit ``-r`` flag.
    """
    for entry in dvc_data.get('outs', []):
        entry['remote'] = remote_name


def dvc_add_files(
    instance_repo_dir: str,
    prepared_data_dir: str,
    file_paths: list[str],
    remote_name: str,
) -> list[str]:
    """Run ``dvc add`` on each file, returning generated ``.dvc`` file paths.

    Parameters
    ----------
    instance_repo_dir
        Absolute path to the cloned pv-prospect-instance repo.
    prepared_data_dir
        Relative directory within the instance repo (e.g. ``data/prepared``).
    file_paths
        Relative paths within *prepared_data_dir*
        (e.g. ``['weather.csv', 'pv/89665.csv']``).
    remote_name
        DVC remote name to embed in each generated ``.dvc`` file so that
        ``dvc pull`` resolves the remote without an explicit ``-r`` flag.

    Returns
    -------
    list[str]
        ``.dvc`` file paths relative to *instance_repo_dir*.
    """
    dvc_file_paths: list[str] = []
    with DvcRepo(root_dir=instance_repo_dir) as repo:
        for rel_path in file_paths:
            rel_target = os.path.join(prepared_data_dir, rel_path)
            abs_target = os.path.join(instance_repo_dir, rel_target)
            logger.info('dvc add %s', rel_target)
            repo.add(targets=abs_target)
            dvc_file_path = f'{rel_target}.dvc'
            _write_remote_to_dvc_file(
                os.path.join(instance_repo_dir, dvc_file_path), remote_name
            )
            dvc_file_paths.append(dvc_file_path)
    return dvc_file_paths


def _write_remote_to_dvc_file(abs_dvc_path: str, remote_name: str) -> None:
    with open(abs_dvc_path) as f:
        dvc_data = yaml.safe_load(f)
    inject_remote(dvc_data, remote_name)
    with open(abs_dvc_path, 'w') as f:
        yaml.safe_dump(dvc_data, f)


def dvc_push(
    instance_repo_dir: str,
    remote_name: str,
    dvc_file_paths: list[str],
) -> None:
    """Push DVC-tracked files to the named remote.

    Parameters
    ----------
    instance_repo_dir
        Absolute path to the cloned instance repo.
    remote_name
        DVC remote name (e.g. ``feature``).
    dvc_file_paths
        ``.dvc`` file paths to push (relative to *instance_repo_dir*).
    """
    logger.info('dvc push %d file(s) to remote %r', len(dvc_file_paths), remote_name)
    abs_targets = [os.path.join(instance_repo_dir, p) for p in dvc_file_paths]
    with DvcRepo(root_dir=instance_repo_dir) as repo:
        repo.push(targets=abs_targets, remote=remote_name)
