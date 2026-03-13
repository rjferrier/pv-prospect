"""Tests for preprocess."""

from pv_prospect.data_extraction import SourceDescriptor
from pv_prospect.data_extraction.processing import preprocess

from .helpers import FakeFileSystem


def test_creates_timeseries_folder() -> None:
    staged_fs = FakeFileSystem({'pv_sites.csv': '', 'location_mapping.csv': ''})

    preprocess(
        lambda path: 'blob/path',
        FakeFileSystem(),
        staged_fs,
        'resources',
        SourceDescriptor.PVOUTPUT,
    )

    assert staged_fs.created_folders == ['timeseries/pvoutput']


def test_provisions_missing_resources() -> None:
    versioned_fs = FakeFileSystem(
        {
            'resolved:resources/pv_sites.csv.dvc': 'sites-content',
            'resolved:resources/location_mapping.csv.dvc': 'mapping-content',
        }
    )
    staged_fs = FakeFileSystem()

    preprocess(
        lambda path: f'resolved:{path}',
        versioned_fs,
        staged_fs,
        'resources',
        SourceDescriptor.PVOUTPUT,
    )

    assert staged_fs.written_texts['pv_sites.csv'] == 'sites-content'
    assert staged_fs.written_texts['location_mapping.csv'] == 'mapping-content'


def test_skips_existing_resources() -> None:
    staged_fs = FakeFileSystem({'pv_sites.csv': '', 'location_mapping.csv': ''})

    preprocess(
        lambda path: 'blob/path',
        FakeFileSystem(),
        staged_fs,
        'resources',
        SourceDescriptor.PVOUTPUT,
    )

    assert staged_fs.written_texts == {}


def test_returns_folder_ids() -> None:
    staged_fs = FakeFileSystem({'pv_sites.csv': '', 'location_mapping.csv': ''})

    result = preprocess(
        lambda path: 'blob/path',
        FakeFileSystem(),
        staged_fs,
        'resources',
        SourceDescriptor.PVOUTPUT,
    )

    assert result == ['timeseries/pvoutput']
