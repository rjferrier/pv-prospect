from pathlib import Path
from typing import Iterable

from pyspark.sql import SparkSession, DataFrame



class CsvTransformer:
    CSV_LOAD_BATCH_SIZE = 5

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def read_and_combine_csv_rows(self, path_or_paths: Path | Iterable[Path]) -> DataFrame | None:
        """
        Read and combine multiple CSV files efficiently using Spark's native capabilities.

        This approach avoids creating large task binaries by:
        1. Reading multiple files at once when they have the same schema
        2. Using Spark's built-in file reading capabilities
        3. Avoiding accumulation of many DataFrame objects in memory

        Args:
            path_or_paths: CSV file or list of files paths to read

        Returns:
            Combined DataFrame or None if all reads failed
        """
        if not path_or_paths:
            return None

        try:
            df = self._read_csv(path_or_paths)

        except Exception as e:
            print(f"  Error reading CSV files: {e}")
            if isinstance(path_or_paths, Path):
                raise

            print(f"  Falling back to individual file reading...")
            df = self._combine_in_batches(path_or_paths)
            if df is not None:
                print(f"  Successfully loaded files using fallback method")

        return df

    def _read_csv(self, path_or_paths: Path | list[Path]) -> DataFrame:
        """
        Read CSV file(s) using Spark with standard settings.

        Args:
            path_or_paths: Single file path or list of file paths to read.

        Returns:
            DataFrame: The loaded DataFrame.
        """
        return self._spark.read.csv(
            path_or_paths,
            header=True,
            inferSchema=True,
            encoding='utf-8'
        )

    def _combine_in_batches(self, file_paths: list[Path]) -> DataFrame | None:
        """
        Fallback method to read files individually and union them in batches.

        This approach processes files in batches to avoid large task binaries
        that can occur when combining many DataFrames at once.

        Args:
            file_paths: List of file path strings to read

        Returns:
            Combined DataFrame or None if all batches failed
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
