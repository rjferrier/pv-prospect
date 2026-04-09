"""Tests for sample_file_path."""

from pv_prospect.data_extraction.processing.sample_file import sample_file_path


def test_zero_padded_to_three_digits() -> None:
    assert sample_file_path(7) == 'point_samples/sample_007.csv'


def test_double_digit_index() -> None:
    assert sample_file_path(17) == 'point_samples/sample_017.csv'


def test_triple_digit_index() -> None:
    assert sample_file_path(128) == 'point_samples/sample_128.csv'


def test_zero_index() -> None:
    assert sample_file_path(0) == 'point_samples/sample_000.csv'
