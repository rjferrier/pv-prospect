"""DVC operations for data versioning."""

import logging
import os

from dvc.repo import Repo as DvcRepo

logger = logging.getLogger(__name__)


def dvc_add_files(
    instance_repo_dir: str,
    prepared_data_dir: str,
    file_paths: list[str],
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

    Returns
    -------
    list[str]
        ``.dvc`` file paths relative to *instance_repo_dir*.
    """
    dvc_file_paths: list[str] = []
    with DvcRepo(root_dir=instance_repo_dir) as repo:
        for rel_path in file_paths:
            target = os.path.join(prepared_data_dir, rel_path)
            logger.info('dvc add %s', target)
            repo.add(targets=target)
            dvc_file_paths.append(f'{target}.dvc')
    return dvc_file_paths


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
    with DvcRepo(root_dir=instance_repo_dir) as repo:
        repo.push(targets=dvc_file_paths, remote=remote_name)
