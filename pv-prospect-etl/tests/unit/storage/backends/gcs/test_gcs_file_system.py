"""Unit tests for GcsFileSystem."""

import unittest.mock as mock

import pytest
from pv_prospect.etl.storage.backends import GcsFileSystem


@pytest.fixture
def mock_storage():
    with mock.patch('google.cloud.storage.Client') as mock_client:
        yield mock_client


def test_list_files_filters_by_glob_pattern(mock_storage):
    # Setup mock blobs
    mock_bucket = mock_storage.return_value.bucket.return_value
    mock_blob1 = mock.Mock()
    mock_blob1.name = 'point_samples/sample_001.csv'
    mock_blob2 = mock.Mock()
    mock_blob2.name = 'point_samples/sample_002.csv'
    mock_blob3 = mock.Mock()
    mock_blob3.name = 'point_samples/other.txt'

    mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]

    fs = GcsFileSystem('my-bucket')

    # Test with the pattern that was failing
    results = fs.list_files('point_samples', 'sample_*.csv')

    names = [r.name for r in results]
    assert sorted(names) == ['sample_001.csv', 'sample_002.csv']
    assert 'other.txt' not in names


def test_list_files_with_prefix_and_pattern(mock_storage):
    mock_bucket = mock_storage.return_value.bucket.return_value
    mock_blob = mock.Mock()
    mock_blob.name = 'data/test_file.json'
    mock_bucket.list_blobs.return_value = [mock_blob]

    fs = GcsFileSystem('my-bucket', prefix='data')

    # prefix in list_files is relative to fs._prefix
    results = fs.list_files('', 'test_*.json')

    assert len(results) == 1
    assert results[0].name == 'test_file.json'
    assert results[0].path == 'test_file.json'


def test_list_files_returns_empty_on_no_match(mock_storage):
    mock_bucket = mock_storage.return_value.bucket.return_value
    mock_blob = mock.Mock()
    mock_blob.name = 'data/file.txt'
    mock_bucket.list_blobs.return_value = [mock_blob]

    fs = GcsFileSystem('my-bucket')

    results = fs.list_files('data', '*.csv')

    assert results == []
