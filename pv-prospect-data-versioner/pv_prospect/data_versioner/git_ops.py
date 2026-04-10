"""Git operations for data versioning."""

import logging
import os
import stat

import git

logger = logging.getLogger(__name__)


def setup_ssh(deploy_key: str, work_dir: str) -> dict[str, str]:
    """Write a deploy key to a temp file and return an env dict with GIT_SSH_COMMAND.

    Parameters
    ----------
    deploy_key
        SSH private key content.
    work_dir
        Directory to write the key file into.

    Returns
    -------
    dict[str, str]
        Environment variables to pass to git operations.
    """
    key_path = os.path.join(work_dir, 'deploy_key')
    with open(key_path, 'w') as f:
        f.write(deploy_key)
    os.chmod(key_path, stat.S_IRUSR)
    return {
        'GIT_SSH_COMMAND': f'ssh -i {key_path} -o StrictHostKeyChecking=no',
    }


def clone_instance_repo(
    repo_url: str,
    branch: str,
    clone_dir: str,
    env: dict[str, str],
) -> git.Repo:
    """Shallow-clone the instance repo without submodules.

    Parameters
    ----------
    repo_url
        Git remote URL (SSH or local path).
    branch
        Branch to clone.
    clone_dir
        Local directory to clone into.
    env
        Environment variables (e.g. ``GIT_SSH_COMMAND``).
    """
    logger.info('Cloning %s (branch %s) to %s', repo_url, branch, clone_dir)
    return git.Repo.clone_from(
        repo_url,
        clone_dir,
        branch=branch,
        depth=1,
        env=env,
    )


def git_commit_and_tag(
    repo: git.Repo,
    dvc_file_paths: list[str],
    tag: str,
    message: str,
) -> None:
    """Stage ``.dvc`` and ``.gitignore`` files, commit, and tag.

    DVC ``add`` creates or updates ``.gitignore`` entries alongside ``.dvc``
    files, so those are staged too.
    """
    repo.index.add(dvc_file_paths)

    gitignore_paths = _find_gitignore_files(repo, dvc_file_paths)
    if gitignore_paths:
        repo.index.add(gitignore_paths)

    logger.info('Committing: %s', message)
    repo.index.commit(message)
    repo.create_tag(tag, message=message)


def git_push(repo: git.Repo, env: dict[str, str]) -> None:
    """Push the current branch and tags to origin."""
    logger.info('Pushing to origin')
    with repo.git.custom_environment(**env):
        repo.remotes.origin.push()
        repo.remotes.origin.push(tags=True)


def _find_gitignore_files(
    repo: git.Repo,
    dvc_file_paths: list[str],
) -> list[str]:
    """Find ``.gitignore`` files in the same directories as ``.dvc`` files."""
    dirs = {os.path.dirname(p) for p in dvc_file_paths}
    gitignore_paths = []
    for d in dirs:
        gi = os.path.join(d, '.gitignore') if d else '.gitignore'
        full = os.path.join(str(repo.working_dir), gi)
        if os.path.exists(full):
            gitignore_paths.append(gi)
    return gitignore_paths
