"""Tests for resolve_path."""

from pathlib import Path

from pv_prospect.etl.storage.resolve.dvc import resolve_path


def test_resolves_dvc_file_to_gcs_cache_path(tmp_path: Path) -> None:
    dvc_file = tmp_path / 'resource.csv.dvc'
    dvc_file.write_text('outs:\n- md5: abcdef1234567890.dir\n  path: resource.csv\n')

    result = resolve_path(str(dvc_file))

    assert result == 'files/md5/ab/cdef1234567890'


def test_strips_dot_dir_suffix(tmp_path: Path) -> None:
    dvc_file = tmp_path / 'data.dvc'
    dvc_file.write_text('outs:\n- md5: 0011223344556677.dir\n  path: data\n')

    result = resolve_path(str(dvc_file))

    assert result == 'files/md5/00/11223344556677'


def test_handles_hash_without_dir_suffix(tmp_path: Path) -> None:
    dvc_file = tmp_path / 'file.dvc'
    dvc_file.write_text('outs:\n- md5: aabbccdd11223344\n  path: file.csv\n')

    result = resolve_path(str(dvc_file))

    assert result == 'files/md5/aa/bbccdd11223344'
