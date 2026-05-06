from unittest.mock import MagicMock, patch

from pv_prospect.data_versioner.dvc_ops import dvc_add_files, dvc_push


@patch('pv_prospect.data_versioner.dvc_ops.DvcRepo')
def test_dvc_add_files_calls_add_per_file(mock_dvc_repo_cls: MagicMock) -> None:
    mock_repo = MagicMock()
    mock_dvc_repo_cls.return_value.__enter__ = MagicMock(return_value=mock_repo)
    mock_dvc_repo_cls.return_value.__exit__ = MagicMock(return_value=False)

    result = dvc_add_files(
        '/tmp/instance',
        'data/prepared',
        ['weather.csv', 'pv/89665.csv'],
    )

    mock_dvc_repo_cls.assert_called_once_with(root_dir='/tmp/instance')
    assert mock_repo.add.call_count == 2
    mock_repo.add.assert_any_call(targets='/tmp/instance/data/prepared/weather.csv')
    mock_repo.add.assert_any_call(targets='/tmp/instance/data/prepared/pv/89665.csv')
    assert result == [
        'data/prepared/weather.csv.dvc',
        'data/prepared/pv/89665.csv.dvc',
    ]


@patch('pv_prospect.data_versioner.dvc_ops.DvcRepo')
def test_dvc_add_files_returns_empty_for_no_files(
    mock_dvc_repo_cls: MagicMock,
) -> None:
    mock_repo = MagicMock()
    mock_dvc_repo_cls.return_value.__enter__ = MagicMock(return_value=mock_repo)
    mock_dvc_repo_cls.return_value.__exit__ = MagicMock(return_value=False)

    result = dvc_add_files('/tmp/instance', 'data/prepared', [])

    assert result == []
    mock_repo.add.assert_not_called()


@patch('pv_prospect.data_versioner.dvc_ops.DvcRepo')
def test_dvc_push_calls_push_with_remote(mock_dvc_repo_cls: MagicMock) -> None:
    mock_repo = MagicMock()
    mock_dvc_repo_cls.return_value.__enter__ = MagicMock(return_value=mock_repo)
    mock_dvc_repo_cls.return_value.__exit__ = MagicMock(return_value=False)

    dvc_push(
        '/tmp/instance',
        'feature',
        ['data/prepared/weather.csv.dvc', 'data/prepared/pv/89665.csv.dvc'],
    )

    mock_dvc_repo_cls.assert_called_once_with(root_dir='/tmp/instance')
    mock_repo.push.assert_called_once_with(
        targets=[
            '/tmp/instance/data/prepared/weather.csv.dvc',
            '/tmp/instance/data/prepared/pv/89665.csv.dvc',
        ],
        remote='feature',
    )
