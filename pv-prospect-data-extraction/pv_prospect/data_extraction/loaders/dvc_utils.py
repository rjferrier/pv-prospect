"""
Utilities for reading DVC content-addressed cache manifests from GCS.

The DVC cache layout stores files as:
  files/md5/<xx>/<rest>          for individual files
  files/md5/<xx>/<rest>.dir     for directory manifests (JSON list of {relpath, md5})

These helpers parse a directory .dvc YAML to locate the manifest blob, then
resolve individual file MD5s from it.
"""
import json
import os

import yaml
from google.cloud import storage as gcs

GCS_BUCKET = os.environ.get('GCS_BUCKET', 'pv-prospect-data')


def get_blob_paths(dvc_file_path: str, resource_files: list[str]) -> dict[str, str]:
    """
    Parse a directory .dvc file and resolve each requested file to its GCS
    cache blob path.

    Args:
        dvc_file_path: Path to the directory .dvc YAML (e.g. /app/resources.dvc).
        resource_files: List of filenames to resolve (e.g. ['pv_sites.csv']).

    Returns:
        Dict mapping filename → GCS cache blob path (e.g. 'files/md5/ab/cdef...').

    Raises:
        FileNotFoundError: If a requested file isn't present in the manifest.
    """
    dir_hash = _read_dir_hash(dvc_file_path)

    client = gcs.Client()
    bucket = client.bucket(GCS_BUCKET)

    manifest_blob_path = md5_to_blob_path(dir_hash) + '.dir'
    manifest = json.loads(bucket.blob(manifest_blob_path).download_as_text())
    hash_by_relpath = {entry['relpath']: entry['md5'] for entry in manifest}

    result: dict[str, str] = {}
    for filename in resource_files:
        md5 = hash_by_relpath.get(filename)
        if md5 is None:
            raise FileNotFoundError(
                f"{filename} not found in DVC dir manifest (hash {dir_hash})"
            )
        result[filename] = md5_to_blob_path(md5)

    return result


def _read_dir_hash(dvc_file_path: str) -> str:
    """Parse a directory .dvc YAML and return the dir manifest MD5 (without .dir suffix)."""
    with open(dvc_file_path) as f:
        dvc_data = yaml.safe_load(f)
    return dvc_data['outs'][0]['md5'].removesuffix('.dir')


def md5_to_blob_path(md5: str) -> str:
    """Convert an MD5 hash to the GCS blob path in DVC's cache layout."""
    return f"files/md5/{md5[:2]}/{md5[2:]}"
