from datetime import date
from unittest.mock import MagicMock, patch

from pv_prospect.data_versioner.config import DataVersionerConfig
from pv_prospect.data_versioner.core import (
    _clean_staging,
    _download_prepared_files,
    version_data,
)
from pv_prospect.etl.storage import FileEntry


def _make_config() -> DataVersionerConfig:
    return DataVersionerConfig(
        staged_prepared_data_storage=MagicMock(),
        staged_cleaned_data_storage=MagicMock(),
        staged_prepared_batches_data_storage=MagicMock(),
        instance_repo_url='git@github.com:user/repo.git',
        instance_repo_branch='main',
        dvc_remote_name='feature',
        prepared_data_dir='data/prepared',
    )


@patch('pv_prospect.data_versioner.core._clean_staging')
@patch('pv_prospect.data_versioner.core.git_push')
@patch('pv_prospect.data_versioner.core.git_commit_and_tag')
@patch('pv_prospect.data_versioner.core.dvc_push')
@patch('pv_prospect.data_versioner.core.dvc_add_files')
@patch('pv_prospect.data_versioner.core.clone_instance_repo')
@patch('pv_prospect.data_versioner.core.setup_ssh')
@patch('pv_prospect.data_versioner.core.verify_readiness')
def test_version_data_orchestrates_full_flow(
    mock_verify: MagicMock,
    mock_setup_ssh: MagicMock,
    mock_clone: MagicMock,
    mock_dvc_add: MagicMock,
    mock_dvc_push: MagicMock,
    mock_git_commit: MagicMock,
    mock_git_push: MagicMock,
    mock_clean: MagicMock,
) -> None:
    mock_verify.return_value = ['weather.csv', 'pv/89665.csv']
    mock_setup_ssh.return_value = {'GIT_SSH_COMMAND': 'ssh -i /tmp/key'}
    mock_repo = MagicMock()
    mock_clone.return_value = mock_repo
    mock_dvc_add.return_value = [
        'data/prepared/weather.csv.dvc',
        'data/prepared/pv/89665.csv.dvc',
    ]

    prepared_fs = MagicMock()
    prepared_fs.read_bytes.return_value = b'csv-data'
    cleaned_fs = MagicMock()
    batches_fs = MagicMock()
    config = _make_config()

    version_data(
        prepared_fs, cleaned_fs, batches_fs, config, 'deploy-key', date(2026, 4, 7)
    )

    mock_verify.assert_called_once_with(prepared_fs, batches_fs)
    mock_setup_ssh.assert_called_once()
    mock_clone.assert_called_once()
    mock_dvc_add.assert_called_once()
    mock_dvc_push.assert_called_once_with(
        mock_dvc_add.call_args[0][0],  # clone_dir
        'feature',
        mock_dvc_add.return_value,
    )
    mock_git_commit.assert_called_once_with(
        mock_repo,
        mock_dvc_add.return_value,
        'data-v2026-04-07',
        'Version prepared data 2026-04-07',
    )
    mock_git_push.assert_called_once()
    mock_clean.assert_called_once_with(prepared_fs, cleaned_fs)


def test_download_prepared_files_writes_to_disk(tmp_path: MagicMock) -> None:
    prepared_fs = MagicMock()
    prepared_fs.read_bytes.side_effect = [b'weather-data', b'pv-data']

    clone_dir = str(tmp_path)
    _download_prepared_files(
        prepared_fs, clone_dir, 'data/prepared', ['weather.csv', 'pv/89665.csv']
    )

    weather_path = tmp_path / 'data' / 'prepared' / 'weather.csv'
    assert weather_path.read_bytes() == b'weather-data'

    pv_path = tmp_path / 'data' / 'prepared' / 'pv' / '89665.csv'
    assert pv_path.read_bytes() == b'pv-data'


def test_clean_staging_deletes_all_files() -> None:
    prepared_fs = MagicMock()
    prepared_fs.list_files.return_value = [
        FileEntry(
            id='weather.csv', name='weather.csv', path='weather.csv', parent_path=''
        ),
    ]
    cleaned_fs = MagicMock()
    cleaned_fs.list_files.return_value = [
        FileEntry(id='a.csv', name='a.csv', path='a.csv', parent_path=''),
        FileEntry(id='b.csv', name='b.csv', path='b.csv', parent_path=''),
    ]

    _clean_staging(prepared_fs, cleaned_fs)

    prepared_fs.delete.assert_called_once_with('weather.csv')
    assert cleaned_fs.delete.call_count == 2
    cleaned_fs.delete.assert_any_call('a.csv')
    cleaned_fs.delete.assert_any_call('b.csv')
