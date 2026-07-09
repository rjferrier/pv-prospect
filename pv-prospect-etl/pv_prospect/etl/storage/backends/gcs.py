import fnmatch
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


# The default urllib3 connection pool size per host on google-cloud-storage's
# underlying Session is 10. Concurrent callers beyond that get "Connection
# pool is full, discarding connection" warnings and pay a fresh TLS handshake
# for each discarded slot. The in-container transform-backfill handler runs
# with a ThreadPoolExecutor of MAX_WORKERS=32 by default, so we size the pool
# to 64 — 2× headroom over the default worker count.
_HTTP_POOL_SIZE = 64


def build_match_glob(full_prefix: str, pattern: str) -> str | None:
    """Translate a basename *pattern* into a GCS ``matchGlob`` expression.

    GCS applies ``matchGlob`` server-side, so a listing pages over the
    objects that *match* rather than over every object under
    *full_prefix*. Client-side filtering pages over the whole subtree
    first and pays one Class A ``ListObjects`` op per 1,000 objects
    scanned — the cost is set by the size of the tree, not the size of
    the answer.

    ``**/`` matches any run of leading path segments including none, so
    one expression covers objects directly under *full_prefix* and those
    nested below it — matching the flat basename semantics of the
    client-side ``fnmatch``. Returns ``None`` for the match-everything
    pattern, where a glob would only constrain the listing to no purpose.
    """
    if pattern == '*':
        return None
    return f'{full_prefix}**/{pattern}'


class GcsFileSystem:
    """Thin I/O adapter for Google Cloud Storage."""

    def __init__(self, bucket_name: str, prefix: str = '') -> None:
        import google.cloud.storage as gcs  # noqa: PLC0415
        import google.cloud.storage_control_v2 as storage_control_v2  # noqa: PLC0415
        import requests.adapters  # type: ignore[import-untyped]  # noqa: PLC0415

        self._client = gcs.Client()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=_HTTP_POOL_SIZE, pool_maxsize=_HTTP_POOL_SIZE
        )
        self._client._http.mount('https://', adapter)
        self._client._http.mount('http://', adapter)
        self._bucket = self._client.bucket(bucket_name)
        self._control_client = storage_control_v2.StorageControlClient()
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

    def append_text(self, path: str, content: str) -> None:
        """Append text by read-modify-write.

        Safe only when at most one writer touches *path* at a time. The
        ledger guarantees this by partitioning by task hash.
        """
        blob = self._bucket.blob(self._blob_path(path))
        existing = blob.download_as_bytes().decode('utf-8') if blob.exists() else ''
        blob.upload_from_string(existing + content, content_type='text/plain')

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

    def rmdir(self, path: str) -> None:
        """Delete an empty folder.

        On HNS-enabled buckets, folders are first-class entities that
        persist after their last child object is removed; the Object API
        cannot delete them. This calls the Storage Control API's
        ``folders.delete`` endpoint, which requires the folder to be
        empty. NotFound is treated as a no-op so callers can rmdir
        unconditionally after a sweep.
        """
        from google.api_core.exceptions import NotFound  # noqa: PLC0415

        folder_name = self._blob_path(path).rstrip('/') + '/'
        full_name = self._control_client.folder_path(
            '_', self._bucket.name, folder_name
        )
        try:
            self._control_client.delete_folder(name=full_name)
        except NotFound:
            pass

    def list_files(
        self,
        prefix: str,
        pattern: str = '*',
        recursive: bool = False,
        start_offset: str = '',
    ) -> list[FileEntry]:
        full_prefix = (
            self._blob_path(prefix) + '/'
            if prefix
            else (self._prefix + '/' if self._prefix else '')
        )
        blobs = self._bucket.list_blobs(
            prefix=full_prefix,
            match_glob=build_match_glob(full_prefix, pattern),
            start_offset=self._blob_path(start_offset) if start_offset else None,
        )

        files = []
        for blob in blobs:
            name = blob.name.split('/')[-1]
            if not name:
                continue

            # matchGlob has already applied *pattern* server-side; this
            # re-check keeps the result identical to the local backend's
            # fnmatch should the two glob dialects ever disagree.
            if pattern != '*' and not fnmatch.fnmatch(name, pattern):
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
