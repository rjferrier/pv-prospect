from dataclasses import dataclass
from typing import Any, Dict

from pv_prospect.etl.storage import FileEntry, StorageConfig


@dataclass
class GcsStorageConfig(StorageConfig):
    """GCS storage configuration."""

    bucket_name: str = ''

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], tracking: 'StorageConfig | None' = None
    ) -> 'GcsStorageConfig':
        return cls(
            bucket_name=data['bucket_name'],
            prefix=data.get('prefix', ''),
            tracking=tracking,
        )


class GcsFileSystem:
    """Thin I/O adapter for Google Cloud Storage."""

    def __init__(self, bucket_name: str, prefix: str = '') -> None:
        from google.cloud import storage as gcs  # noqa: PLC0415

        self._client = gcs.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._prefix = prefix.strip('/')

    def __str__(self) -> str:
        return f'GCS bucket: {self._bucket.name}/{self._prefix}'

    def _blob_path(self, relative_path: str) -> str:
        """Prepend the prefix to *relative_path*."""
        return f'{self._prefix}/{relative_path}' if self._prefix else relative_path

    def exists(self, path: str) -> bool:
        return self._bucket.blob(self._blob_path(path)).exists()

    def read_text(self, path: str) -> str:
        blob = self._bucket.blob(self._blob_path(path))
        return blob.download_as_bytes().decode('utf-8')

    def write_text(self, path: str, content: str) -> None:
        blob = self._bucket.blob(self._blob_path(path))
        blob.upload_from_string(content, content_type='text/plain')

    def read_bytes(self, path: str) -> bytes:
        blob = self._bucket.blob(self._blob_path(path))
        return blob.download_as_bytes()

    def write_bytes(self, path: str, content: bytes) -> None:
        blob = self._bucket.blob(self._blob_path(path))
        blob.upload_from_string(content, content_type='application/octet-stream')

    def delete(self, path: str) -> None:
        blob = self._bucket.blob(self._blob_path(path))
        blob.delete()

    def mkdir(self, path: str) -> None:
        pass  # GCS has a flat namespace

    def list_files(
        self, prefix: str, pattern: str = '*', recursive: bool = False
    ) -> list[FileEntry]:
        full_prefix = (
            self._blob_path(prefix) + '/'
            if prefix
            else (self._prefix + '/' if self._prefix else '')
        )
        blobs = self._bucket.list_blobs(prefix=full_prefix)

        files = []
        for blob in blobs:
            name = blob.name.split('/')[-1]
            if not name:
                continue

            if pattern != '*':
                if pattern.startswith('*.'):
                    ext = pattern[2:]
                    if not name.endswith(f'.{ext}'):
                        continue
                elif name != pattern:
                    continue

            rel = blob.name
            if self._prefix and rel.startswith(self._prefix + '/'):
                rel = rel[len(self._prefix) + 1 :]
            parent = '/'.join(rel.split('/')[:-1])

            files.append(
                FileEntry(
                    id=blob.name,
                    name=name,
                    path=rel,
                    parent_path=parent,
                )
            )

        return files
