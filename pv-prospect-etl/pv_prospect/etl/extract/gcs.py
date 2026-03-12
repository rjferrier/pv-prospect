import io
import json

import yaml  # type: ignore[import-untyped]

from pv_prospect.etl.clients.gcs import GcsClient


class GcsExtractor(GcsClient):
    """Storage client that reads blobs from a GCS bucket."""

    def read_file(self, file_path: str) -> io.TextIOWrapper:
        blob = self._bucket.blob(self._blob_path(file_path))
        data = blob.download_as_bytes()
        return io.TextIOWrapper(io.BytesIO(data), encoding='utf-8')

    def list_files(
        self,
        folder_path: str | None = None,
        pattern: str = '*',
        recursive: bool = False,
    ) -> list[dict]:
        prefix = (
            self._blob_path(folder_path) + '/'
            if folder_path
            else (self._prefix + '/' if self._prefix else '')
        )
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
                rel = rel[len(self._prefix) + 1 :]
            parent = '/'.join(rel.split('/')[:-1])

            files.append(
                {
                    'id': blob.name,
                    'name': name,
                    'path': rel,
                    'parent_path': parent,
                }
            )

        return files
