from google.cloud import storage as gcs


class GcsClient:
    """Base client for interacting with a GCS bucket.

    Paths are optionally scoped under a prefix.
    """

    def __init__(self, bucket_name: str, prefix: str) -> None:
        self._client = gcs.Client()
        self._bucket = self._client.bucket(bucket_name)
        self._prefix = prefix.strip('/')

    def _blob_path(self, relative_path: str) -> str:
        """Prepend the prefix to *relative_path*."""
        return f"{self._prefix}/{relative_path}" if self._prefix else relative_path

    def file_exists(self, file_path: str) -> bool:
        return self._bucket.blob(self._blob_path(file_path)).exists()
