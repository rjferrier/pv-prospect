import os
import stat
import tempfile
from unittest.mock import MagicMock, patch

from pv_prospect.data_versioner.git_ops import (
    clone_instance_repo,
    git_commit_and_tag,
    git_push,
    setup_ssh,
)


def test_setup_ssh_writes_key_file() -> None:
    with tempfile.TemporaryDirectory() as work_dir:
        setup_ssh('my-secret-key', work_dir)

        key_path = os.path.join(work_dir, 'deploy_key')
        assert os.path.exists(key_path)
        with open(key_path) as f:
            assert f.read() == 'my-secret-key'

        file_mode = os.stat(key_path).st_mode
        assert file_mode & stat.S_IRUSR
        assert not (file_mode & stat.S_IWUSR)
        assert not (file_mode & stat.S_IRGRP)
        assert not (file_mode & stat.S_IROTH)


def test_setup_ssh_returns_git_ssh_command() -> None:
    with tempfile.TemporaryDirectory() as work_dir:
        env = setup_ssh('key', work_dir)

        assert 'GIT_SSH_COMMAND' in env
        key_path = os.path.join(work_dir, 'deploy_key')
        assert key_path in env['GIT_SSH_COMMAND']
        assert 'StrictHostKeyChecking=no' in env['GIT_SSH_COMMAND']


@patch('pv_prospect.data_versioner.git_ops.git.Repo')
def test_clone_instance_repo_calls_clone_from(mock_repo_cls: MagicMock) -> None:
    mock_repo = MagicMock()
    mock_repo_cls.clone_from.return_value = mock_repo

    env = {'GIT_SSH_COMMAND': 'ssh -i /tmp/key'}
    result = clone_instance_repo(
        'git@github.com:user/repo.git', 'main', '/tmp/clone', env
    )

    mock_repo_cls.clone_from.assert_called_once_with(
        'git@github.com:user/repo.git',
        '/tmp/clone',
        branch='main',
        depth=1,
        env=env,
    )
    assert result is mock_repo


def test_git_commit_and_tag_stages_and_commits() -> None:
    mock_repo = MagicMock()
    mock_repo.working_dir = '/tmp/instance'

    dvc_files = ['data/prepared/weather.csv.dvc']
    git_commit_and_tag(mock_repo, dvc_files, 'data-v2026-04-07', 'Version 2026-04-07')

    mock_repo.index.add.assert_called_with(dvc_files)
    mock_repo.index.commit.assert_called_once_with('Version 2026-04-07')
    mock_repo.create_tag.assert_called_once_with(
        'data-v2026-04-07', message='Version 2026-04-07'
    )


def test_git_commit_and_tag_stages_gitignore_files() -> None:
    mock_repo = MagicMock()

    with tempfile.TemporaryDirectory() as work_dir:
        mock_repo.working_dir = work_dir
        # Create a .gitignore in the same directory as the .dvc file
        data_dir = os.path.join(work_dir, 'data', 'prepared')
        os.makedirs(data_dir)
        with open(os.path.join(data_dir, '.gitignore'), 'w') as f:
            f.write('/weather.csv\n')

        dvc_files = ['data/prepared/weather.csv.dvc']
        git_commit_and_tag(mock_repo, dvc_files, 'v1', 'msg')

    # Should have been called twice: once for .dvc files, once for .gitignore
    assert mock_repo.index.add.call_count == 2
    mock_repo.index.add.assert_any_call(dvc_files)
    mock_repo.index.add.assert_any_call(['data/prepared/.gitignore'])


def test_git_push_pushes_branch_and_tags() -> None:
    mock_repo = MagicMock()
    env = {'GIT_SSH_COMMAND': 'ssh -i /tmp/key'}

    git_push(mock_repo, env)

    mock_repo.git.custom_environment.assert_called_once_with(**env)
    assert mock_repo.remotes.origin.push.call_count == 2
