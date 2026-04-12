import pytest
from pv_prospect.data_extraction.processing.sample_file import read_sample_file


class FakeFileSystem:
    def __init__(self, contents: str) -> None:
        self._contents = contents

    def read_text(self, path: str) -> str:
        return self._contents


def test_parses_lat_lon_rows() -> None:
    fs = FakeFileSystem('50.18,-5.2\n50.34,-5.04\n50.5,-4.88\n')

    locations = read_sample_file(fs, 'point_samples/sample_000.csv')

    assert locations == [
        '50.18,-5.2',
        '50.34,-5.04',
        '50.5,-4.88',
    ]


def test_skips_blank_lines() -> None:
    fs = FakeFileSystem('50.18,-5.2\n\n50.34,-5.04\n\n')

    locations = read_sample_file(fs, 'point_samples/sample_000.csv')

    assert len(locations) == 2


def test_tolerates_whitespace_around_coordinates() -> None:
    fs = FakeFileSystem(' 50.18 , -5.2 \n')

    locations = read_sample_file(fs, 'point_samples/sample_000.csv')

    assert locations == [
        '50.18,-5.2',
    ]


def test_empty_file_returns_empty_list() -> None:
    fs = FakeFileSystem('')

    locations = read_sample_file(fs, 'point_samples/sample_000.csv')

    assert locations == []


def test_malformed_row_raises() -> None:
    fs = FakeFileSystem('50.18\n')

    with pytest.raises(IndexError):
        read_sample_file(fs, 'point_samples/sample_000.csv')
