import csv
from io import StringIO
from typing import Iterable

from pv_prospect.common import VarMapping, map_from_env
from pv_prospect.etl.clients.gcs import GcsClient


class GcsLoader(GcsClient):
    """Storage client that writes blobs into a GCS bucket.

    All paths are scoped under a configurable *prefix* (e.g. ``staging``),
    so ``write_csv('timeseries/om/file.csv', …)`` lands at
    ``gs://<bucket>/staging/timeseries/om/file.csv``.
    """

    @classmethod
    def from_env(cls, bucket_name_env_var_name: str, prefix: str = '') -> 'GcsLoader':
        return map_from_env(
            cls,
            {'bucket_name': VarMapping(bucket_name_env_var_name, str)},
            prefix=prefix,
        )

    def create_folder(self, folder_path: str) -> str | None:
        """No-op on GCS (flat namespace). Returns the prefixed path."""
        full = self._blob_path(folder_path) + '/'
        print(f'    GCS folder (virtual): gs://{self._bucket.name}/{full}')
        return full

    def write_csv(
        self, file_path: str, rows: Iterable[Iterable[str]], overwrite: bool = False
    ) -> None:
        blob_path = self._blob_path(file_path)
        blob = self._bucket.blob(blob_path)

        if not overwrite and blob.exists():
            raise FileExistsError(
                f'Blob already exists: gs://{self._bucket.name}/{blob_path}'
            )

        buf = StringIO()
        writer = csv.writer(buf)
        for row in rows:
            writer.writerow(row)

        blob.upload_from_string(buf.getvalue(), content_type='text/csv')
        print(f'    Written to: gs://{self._bucket.name}/{blob_path}')

    def write_text(self, file_path: str, text: str, overwrite: bool = False) -> None:
        """Write text data to a GCS blob."""
        blob_path = self._blob_path(file_path)
        blob = self._bucket.blob(blob_path)

        if not overwrite and blob.exists():
            raise FileExistsError(
                f'Blob already exists: gs://{self._bucket.name}/{blob_path}'
            )

        blob.upload_from_string(text, content_type='text/plain')
        print(f'    Written to: gs://{self._bucket.name}/{blob_path}')
