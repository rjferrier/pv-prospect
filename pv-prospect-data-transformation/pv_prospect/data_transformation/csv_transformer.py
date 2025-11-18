from pathlib import Path
from typing import Iterable

from pyspark.sql import SparkSession, DataFrame



class CsvTransformer:
    """
    A utility class for reading and combining CSV files using Apache Spark.

    Attributes:
        CSV_LOAD_BATCH_SIZE: Number of CSV files to process in each batch during fallback loading.
    """
    CSV_LOAD_BATCH_SIZE = 5

    def __init__(self, spark: SparkSession):
        """
        Initialize the CsvTransformer with a Spark session.

        Args:
            spark: Active SparkSession instance for reading CSV files.
        """
        self._spark = spark

    def read_and_combine_csv_rows(self, directory: Path, filename_or_filenames: str | list[str]) -> DataFrame | None:
        """
        Read and combine multiple CSV files efficiently using Spark's native capabilities.

        This approach avoids creating large task binaries by:
        1. Reading multiple files at once when they have the same schema
        2. Using Spark's built-in file reading capabilities
        3. Avoiding accumulation of many DataFrame objects in memory

        Args:
            directory: Base directory containing the CSV file(s).
            filename_or_filenames: Single filename string or list of filename strings to read.

        Returns:
            Combined DataFrame containing all rows from the CSV file(s), or None if all reads failed.

        Raises:
            Exception: Re-raises exception if reading a single file fails.
        """
        if not filename_or_filenames:
            return None

        if isinstance(filename_or_filenames, str):
            path_or_paths = str(directory / filename_or_filenames)
        else:
            path_or_paths = [str(directory / f) for f in filename_or_filenames]

        try:
            df = self._read_csv(path_or_paths)

        except Exception as e:
            print(f"  Error reading CSV files: {e}")
            if not isinstance(path_or_paths, list):
                raise

            print(f"  Falling back to individual file reading...")
            df = self._combine_in_batches(path_or_paths)
            if df is not None:
                print(f"  Successfully loaded files using fallback method")

        return df

    def _combine_in_batches(self, file_paths: list[str]) -> DataFrame | None:
        """
        Fallback method to read files individually and union them in batches.

        This approach processes files in batches to avoid large task binaries
        that can occur when combining many DataFrames at once. Files are read
        in groups defined by CSV_LOAD_BATCH_SIZE and combined using unionByName.

        Args:
            file_paths: List of absolute file path strings to read.

        Returns:
            Combined DataFrame containing all rows from successfully read files,
            or None if all batches failed to load.
        """
        batch_df = None

        for i in range(0, len(file_paths), self.CSV_LOAD_BATCH_SIZE):
            paths = file_paths[i:i + self.CSV_LOAD_BATCH_SIZE]
            try:
                df = self._read_csv(paths)

                if batch_df is None:
                    batch_df = df
                else:
                    batch_df = batch_df.unionByName(df, allowMissingColumns=False)

            except Exception as batch_error:
                print(f"  Error reading batch {i // self.CSV_LOAD_BATCH_SIZE + 1}: {batch_error}")
                continue

        if batch_df is not None:
            print(f"  Successfully loaded files using fallback method")

        return batch_df

    def _read_csv(self, path_or_paths: str | list[str]) -> DataFrame:
        """
        Read CSV file(s) using Spark with standard settings.

        Configuration:
        - header=True: First row contains column names
        - inferSchema=True: Automatically infer column data types
        - encoding='utf-8': Use UTF-8 character encoding

        Args:
            path_or_paths: Single file path string or list of file path strings to read.

        Returns:
            DataFrame loaded from the CSV file(s).

        Raises:
            Exception: If the CSV file(s) cannot be read or parsed.
        """
        return self._spark.read.csv(
            path_or_paths,
            header=True,
            inferSchema=True,
            encoding='utf-8'
        )
