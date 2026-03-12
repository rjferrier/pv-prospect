import io
from typing import Optional

import pandas as pd
from google.cloud import storage as gcs


class GcsIoHelper:
    """Helper for reading CSVs and writing Parquet files directly from/to GCS."""

    def __init__(self, raw_bucket_name: str, model_bucket_name: str):
        self._client = gcs.Client()
        self._raw_bucket = self._client.bucket(raw_bucket_name)
        self._model_bucket = self._client.bucket(model_bucket_name)

    def read_csv_from_raw(self, blob_path: str) -> Optional[pd.DataFrame]:
        """Read a CSV from the raw data bucket into a pandas DataFrame."""
        blob = self._raw_bucket.blob(blob_path)
        if not blob.exists():
            return None

        # Download into memory
        data = blob.download_as_bytes()
        with io.BytesIO(data) as buf:
            return pd.read_csv(buf, encoding='utf-8')

    def write_parquet_to_raw(self, df: pd.DataFrame, blob_path: str) -> None:
        """Write a DataFrame to the raw bucket as a Parquet file (for intermediate clean steps)."""
        with io.BytesIO() as buf:
            df.to_parquet(buf, engine='pyarrow', index=False)
            blob = self._raw_bucket.blob(blob_path)
            blob.upload_from_string(
                buf.getvalue(), content_type='application/octet-stream'
            )
            print(f'    Written to: gs://{self._raw_bucket.name}/{blob_path}')

    def write_parquet_to_model(self, df: pd.DataFrame, blob_path: str) -> None:
        """Write a DataFrame to the model bucket as a Parquet file (for final process steps)."""
        with io.BytesIO() as buf:
            df.to_parquet(buf, engine='pyarrow', index=False)
            blob = self._model_bucket.blob(blob_path)
            blob.upload_from_string(
                buf.getvalue(), content_type='application/octet-stream'
            )
            print(f'    Written to: gs://{self._model_bucket.name}/{blob_path}')

    def file_exists_in_raw(self, blob_path: str) -> bool:
        return self._raw_bucket.blob(blob_path).exists()
