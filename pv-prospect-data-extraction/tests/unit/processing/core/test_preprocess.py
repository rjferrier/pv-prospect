"""Tests for preprocess."""

from pv_prospect.data_extraction import DataSource
from pv_prospect.data_extraction.processing import preprocess

from .helpers import FakeFileSystem


def test_creates_timeseries_folder() -> None:
    staged_fs = FakeFileSystem()

    preprocess(staged_fs, DataSource.PVOUTPUT)

    assert staged_fs.created_folders == ['timeseries/pvoutput']


def test_returns_folder_ids() -> None:
    staged_fs = FakeFileSystem()

    result = preprocess(staged_fs, DataSource.PVOUTPUT)

    assert result == ['timeseries/pvoutput']
