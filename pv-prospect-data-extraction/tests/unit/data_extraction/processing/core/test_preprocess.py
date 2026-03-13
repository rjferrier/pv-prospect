"""Tests for preprocess."""

from pv_prospect.data_extraction.extractors import SourceDescriptor
from pv_prospect.data_extraction.processing.core import preprocess

from .helpers import FakeExtractor, FakeLoader


def test_creates_timeseries_folder() -> None:
    loader = FakeLoader(existing_files={'pv_sites.csv', 'location_mapping.csv'})

    preprocess(
        lambda path: 'blob/path',
        FakeExtractor(),
        loader,
        'resources',
        SourceDescriptor.PVOUTPUT,
    )

    assert loader.created_folders == ['timeseries/pvoutput']


def test_provisions_missing_resources() -> None:
    versioned_extractor = FakeExtractor(
        {
            'resolved:resources/pv_sites.csv.dvc': 'sites-content',
            'resolved:resources/location_mapping.csv.dvc': 'mapping-content',
        }
    )
    loader = FakeLoader()

    preprocess(
        lambda path: f'resolved:{path}',
        versioned_extractor,
        loader,
        'resources',
        SourceDescriptor.PVOUTPUT,
    )

    assert loader.written_texts['pv_sites.csv'] == 'sites-content'
    assert loader.written_texts['location_mapping.csv'] == 'mapping-content'


def test_skips_existing_resources() -> None:
    loader = FakeLoader(existing_files={'pv_sites.csv', 'location_mapping.csv'})

    preprocess(
        lambda path: 'blob/path',
        FakeExtractor(),
        loader,
        'resources',
        SourceDescriptor.PVOUTPUT,
    )

    assert loader.written_texts == {}


def test_returns_folder_ids() -> None:
    loader = FakeLoader(existing_files={'pv_sites.csv', 'location_mapping.csv'})

    result = preprocess(
        lambda path: 'blob/path',
        FakeExtractor(),
        loader,
        'resources',
        SourceDescriptor.PVOUTPUT,
    )

    assert result == ['timeseries/pvoutput']
