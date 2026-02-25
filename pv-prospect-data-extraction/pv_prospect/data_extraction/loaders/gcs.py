import csv
import io
import json
import os
from io import StringIO
from typing import Iterable

from google.cloud import storage as gcs

from pv_prospect.data_extraction.loaders.dvc_utils import get_blob_paths

GCS_BUCKET = os.environ.get('GCS_BUCKET', 'pv-prospect-data')
GCS_STAGING_PREFIX = 'staging'


class GcsClient:
    """Storage client that reads/writes blobs in a GCS bucket.

    All paths are scoped under a configurable *prefix* (e.g. ``staging``),
    so ``write_csv('timeseries/om/file.csv', …)`` lands at
    ``gs://<bucket>/staging/timeseries/om/file.csv``.
    """

    def __init__(self, bucket_name: str = GCS_BUCKET, prefix: str = GCS_STAGING_PREFIX) -> None:
        self._client = gcs.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._prefix = prefix.strip('/')

    # -- helpers ----------------------------------------------------------

    def _blob_path(self, relative_path: str) -> str:
        """Prepend the prefix to *relative_path*."""
        return f"{self._prefix}/{relative_path}" if self._prefix else relative_path

    # -- StorageClient protocol -------------------------------------------

    def create_folder(self, folder_path: str) -> str | None:
        """No-op on GCS (flat namespace). Returns the prefixed path."""
        full = self._blob_path(folder_path) + '/'
        print(f"    GCS folder (virtual): gs://{self._bucket.name}/{full}")
        return full

    def file_exists(self, file_path: str) -> bool:
        return self._bucket.blob(self._blob_path(file_path)).exists()

    def read_file(self, file_path: str) -> io.TextIOWrapper:
        blob = self._bucket.blob(self._blob_path(file_path))
        data = blob.download_as_bytes()
        return io.TextIOWrapper(io.BytesIO(data), encoding='utf-8')

    def write_csv(self, file_path: str, rows: Iterable[Iterable[str]], overwrite: bool = False) -> None:
        blob_path = self._blob_path(file_path)
        blob = self._bucket.blob(blob_path)

        if not overwrite and blob.exists():
            raise FileExistsError(f"Blob already exists: gs://{self._bucket.name}/{blob_path}")

        buf = StringIO()
        writer = csv.writer(buf)
        for row in rows:
            writer.writerow(row)

        blob.upload_from_string(buf.getvalue(), content_type='text/csv')
        print(f"    Written to: gs://{self._bucket.name}/{blob_path}")

    def write_metadata(self, csv_file_path: str, metadata: dict) -> None:
        if csv_file_path.lower().endswith('.csv'):
            metadata_path = csv_file_path[:-4] + '.json'
        else:
            metadata_path = csv_file_path + '.json'

        blob_path = self._blob_path(metadata_path)
        blob = self._bucket.blob(blob_path)
        blob.upload_from_string(
            json.dumps(metadata, indent=2, ensure_ascii=False),
            content_type='application/json',
        )
        print(f"    Written metadata to: gs://{self._bucket.name}/{blob_path}")

    def list_files(self, folder_path: str | None = None, pattern: str = "*", recursive: bool = False) -> list[dict]:
        prefix = self._blob_path(folder_path) + '/' if folder_path else self._prefix + '/'
        blobs = self._bucket.list_blobs(prefix=prefix)

        files = []
        for blob in blobs:
            name = blob.name.split('/')[-1]
            if not name:
                continue

            # Simple pattern matching
            if pattern != '*':
                if pattern.startswith('*.'):
                    ext = pattern[2:]
                    if not name.endswith(f'.{ext}'):
                        continue
                elif name != pattern:
                    continue

            # Strip the global prefix to get a "relative" path
            rel = blob.name
            if self._prefix and rel.startswith(self._prefix + '/'):
                rel = rel[len(self._prefix) + 1:]
            parent = '/'.join(rel.split('/')[:-1])

            files.append({
                'id': blob.name,
                'name': name,
                'path': rel,
                'parent_path': parent,
            })

        return files

    def rename_file(self, file_id_or_path: str, new_name: str) -> None:
        raise NotImplementedError("rename_file is not supported on GcsClient")

    def move_file(self, file_id_or_path: str, old_parent_or_new_path: str, new_parent_id: str | None = None) -> None:
        raise NotImplementedError("move_file is not supported on GcsClient")

    def trash_file(self, file_id_or_path: str) -> None:
        raise NotImplementedError("trash_file is not supported on GcsClient")

    def provision_supporting_resources(self, dvc_file_path: str, resource_files: list[str]) -> None:
        """
        Server-side copy supporting resource CSVs from the GCS DVC cache to
        the staging prefix — no data passes through the worker.

        Args:
            dvc_file_path: Path to the directory .dvc YAML (e.g. /app/resources.dvc).
            resource_files: List of filenames to provision (e.g. ['pv_sites.csv']).
        """
        blob_paths = get_blob_paths(dvc_file_path, resource_files)

        for filename, src_blob_path in blob_paths.items():
            dest_blob_path = self._blob_path(filename)
            src_blob = self._bucket.blob(src_blob_path)
            self._bucket.copy_blob(src_blob, self._bucket, dest_blob_path)
            print(f"    Copied {filename}: {src_blob_path} → {dest_blob_path}")

