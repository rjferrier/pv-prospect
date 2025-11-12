from contextlib import contextmanager
from pathlib import Path
import argparse
from collections import defaultdict
import re

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

CSV_LOAD_BATCH_SIZE = 5


def combine_csvs(source_path: str, output_path: str | None = None, recursive: bool = False) -> list[str]:
    """
    Combine CSV files from a local folder using Spark, grouping them by family (prefix).
    A CSV family is defined by the filename format {prefix}_YYYYMMDD.csv.

    Optionally recurse through subdirectories.

    Args:
        source_path (str): The path to the local folder containing CSV files (e.g., 'data/pvoutput').
        output_path (str, optional): Path where to save the output CSV files. Must be a folder path.
                                    If None, saves in the same folder as the input files.
        recursive (bool): If True, recursively process all subdirectories that contain CSV files.

    Returns:
        list[str]: List of paths to the created CSV files (one per family found).

    Raises:
        ValueError: If no CSV files are found in the folder (and not recursive).
        FileNotFoundError: If the folder doesn't exist.
    """
    source_path_obj = Path(source_path)

    if not source_path_obj.exists():
        raise FileNotFoundError(f"Folder not found: {source_path}")

    if not source_path_obj.is_dir():
        raise ValueError(f"Path is not a directory: {source_path}")

    output_path_obj = Path(output_path) if output_path else None
    strategy = combine_csvs_recursively if recursive else combine_csvs_single_folder

    with get_spark_session() as spark:
        return strategy(spark, source_path_obj, output_path_obj)


@contextmanager
def get_spark_session():
    """
    Create and manage a Spark session as a context manager.

    Configures Spark with optimizations for CSV processing and ensures proper cleanup
    by stopping the session when the context exits.

    Yields:
        SparkSession: A configured Spark session for CSV processing.
    """
    spark = SparkSession.builder \
        .appName("CombineCSVs") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()

    yield spark

    spark.stop()


def combine_csvs_recursively(spark: SparkSession, source_path: Path, output_path: Path | None) -> list[str]:
    """
    Recursively process all subdirectories containing CSV files and combine them by family.

    Searches through the entire directory tree starting from source_path to find all
    folders that contain CSV files directly (not in subfolders), then processes each
    folder using combine_csvs_in_folder.

    Args:
        spark (SparkSession): The Spark session to use for processing.
        source_path (Path): The root path to search for folders containing CSV files.
        output_path (Path | None): Path where to save the output CSV files. Must be a folder path.
                                    If None, saves in the same folder as the input files.

    Returns:
        list[str]: List of paths to all created CSV files across all processed folders.

    Raises:
        ValueError: If no CSV files are found in the entire folder tree.
    """
    # Find all directories that contain CSV files (directly, not in subfolders)
    folders_with_csvs = set()
    for csv_file in Path(source_path).rglob('*.csv'):
        if not csv_file.stem.endswith('_combined'):
            folders_with_csvs.add(csv_file.parent)

    if not folders_with_csvs:
        raise ValueError(f"No CSV files found in folder tree: {source_path}")
    print(f"Found {len(folders_with_csvs)} folders with CSV files")

    # For each folder, combine CSVs by family
    created_files = []
    for csv_folder in sorted(folders_with_csvs):
        print(f"\nProcessing folder: {csv_folder}")
        if output_path is None:
            folder_output_path = None
        else:
            output_path.mkdir(parents=True, exist_ok=True)
            folder_output_path = output_path
        results = combine_csvs_in_folder(spark, csv_folder, folder_output_path)
        created_files.extend(results)

    return created_files



def combine_csvs_single_folder(spark: SparkSession, source_path: Path, output_path: Path | None) -> list[str]:
    """
    Process CSV files in a single folder (non-recursive wrapper).

    This is a wrapper around combine_csvs_in_folder that raises an error if no CSV files
    are found, making it suitable for single-folder processing mode.

    Args:
        spark (SparkSession): The Spark session to use for processing.
        source_path (Path): The path to the folder containing CSV files.
        output_path (Path | None): Path where to save the output CSV files. Must be a folder path.
                                    If None, saves in the same folder as the input files.

    Returns:
        list[str]: List of paths to the created CSV files (one per family).

    Raises:
        ValueError: If no CSV files are found in the folder.
    """
    # Process only the specified folder
    results = combine_csvs_in_folder(spark, source_path, output_path)

    if not results:
        raise ValueError(f"No CSV files found in folder: {source_path.absolute()}")

    return results


def combine_csvs_in_folder(spark: SparkSession, source_path: Path, output_path: Path | None) -> list[str]:
    """
    Combine CSV files from a single folder using Spark, grouping them by family (prefix).
    A CSV family is defined by the filename format {prefix}_YYYYMMDD.csv.

    For each family, all matching CSV files are combined into a single CSV file.
    Only CSVs directly in the given folder are combined (no recursion).

    Args:
        spark (SparkSession): The Spark session to use for processing.
        source_path (Path): The path to the folder containing CSV files.
        output_path (Path | None): Path where to save the output CSV files. Must be a folder path.
                                    If None, saves in the same folder as the input files.

    Returns:
        list[str]: List of paths to the created CSV files (one per family), or empty list if no CSV files found.
    """
    # Only combine CSVs directly in this folder (not in subfolders)
    folder_csv_files = sorted([f for f in source_path.glob('*.csv') if not f.stem.endswith('_combined')])

    if not folder_csv_files:
        return []

    # Group CSV files by family (prefix)
    # Pattern: {prefix}_YYYYMMDD.csv where YYYYMMDD is 8 digits
    families = defaultdict(list)
    date_pattern = re.compile(r'^(.+)_(\d{8})$')

    for csv_file in folder_csv_files:
        match = date_pattern.match(csv_file.stem)
        if match:
            prefix = match.group(1)
            families[prefix].append(csv_file)
        else:
            # Files that don't match the pattern are treated as their own family
            families[csv_file.stem].append(csv_file)

    created_files = []

    for prefix, files in sorted(families.items()):
        output_name = f"{prefix}.csv"

        if output_path is None:
            output_file = source_path / output_name
        else:
            # output_path should be a directory
            output_path.mkdir(parents=True, exist_ok=True)
            output_file = output_path / output_name

        print(f"Processing family: {prefix} ({len(files)} file(s))")

        # Read and combine CSV files efficiently
        combined_df = CsvTransformer(spark).read_and_combine_csvs(files)

        if combined_df is None:
            # Create an empty file if all files failed to load
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_text('')
            print(f"  Created empty output file {output_file}")
            created_files.append(str(output_file))
            continue

        # Get total row count
        row_count = combined_df.count()

        # Write the combined dataframe to a single CSV file
        # Using coalesce(1) to write to a single file
        temp_output = output_file.parent / f"{output_file.stem}_temp"
        combined_df.coalesce(1).write.csv(
            str(temp_output),
            header=True,
            mode='overwrite',
            encoding='utf-8'
        )

        # Spark writes to a directory with partition files, so we need to move the actual CSV
        csv_parts = list(temp_output.glob('*.csv'))
        if csv_parts:
            # Move the CSV file to the desired location
            csv_parts[0].rename(output_file)
            # Clean up the temp directory
            import shutil
            shutil.rmtree(temp_output)
            print(f"  Successfully created {output_file} with {row_count} data rows")
        else:
            print(f"  Warning: No CSV file generated for {prefix}")
            continue

        created_files.append(str(output_file))

    return created_files


class CsvTransformer:
    def __init__(self, spark: SparkSession):
        self._spark = spark

    def read_and_combine_csvs(self, csv_files: list[Path]) -> DataFrame | None:
        """
        Read and combine multiple CSV files efficiently using Spark's native capabilities.

        This approach avoids creating large task binaries by:
        1. Reading multiple files at once when they have the same schema
        2. Using Spark's built-in file reading capabilities
        3. Avoiding accumulation of many DataFrame objects in memory

        Args:
            csv_files: List of CSV file paths to read

        Returns:
            Combined DataFrame or None if all reads failed
        """
        if not csv_files:
            return None

        print(f"  Reading and combining {len(csv_files)} CSV file(s)...")

        # Convert paths to strings
        file_paths = [str(f) for f in sorted(csv_files)]

        # Spark can read multiple files at once - this is much more efficient
        # than reading them individually and prevents large task binary warnings
        path_or_paths = file_paths[0] if len(file_paths) == 1 else file_paths

        try:
            df = self._read_csv(path_or_paths)
            print(f"  Successfully loaded {len(csv_files)} file(s)")

        except Exception as e:
            print(f"  Error reading CSV files: {e}")
            print(f"  Falling back to individual file reading...")
            df = self._combine_in_batches(file_paths)
            if df is not None:
                print(f"  Successfully loaded files using fallback method")

        return df

    def _combine_in_batches(self, file_paths: list[str]) -> DataFrame | None:
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

        for i in range(0, len(file_paths), CSV_LOAD_BATCH_SIZE):
            paths = file_paths[i:i + CSV_LOAD_BATCH_SIZE]
            try:
                df = self._read_csv(paths)

                if batch_df is None:
                    batch_df = df
                else:
                    batch_df = batch_df.unionByName(df, allowMissingColumns=False)

            except Exception as batch_error:
                print(f"  Error reading batch {i // CSV_LOAD_BATCH_SIZE + 1}: {batch_error}")
                continue

        if batch_df is not None:
            print(f"  Successfully loaded files using fallback method")

        return batch_df

    def _read_csv(self, path_or_paths: str | list[str]) -> DataFrame:
        return self._spark.read.csv(
            path_or_paths,
            header=True,
            inferSchema=True,
            encoding='utf-8'
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Combine CSV files by family (prefix) in a folder using Apache Spark. A CSV family has the format {prefix}_YYYYMMDD.csv")
    parser.add_argument('folder', type=str, help='Path to the folder containing CSV files (e.g., data/pvoutput)')
    parser.add_argument('-o', '--output', type=str, default=None, help='Path to folder where combined CSV files will be saved (optional)')
    parser.add_argument('-r', '--recursive', action='store_true', help='Recursively process all subdirectories')

    args = parser.parse_args()
    result = combine_csvs(args.folder, args.output, args.recursive)

    print(f"\n{'='*60}")
    print(f"Created {len(result)} combined CSV file(s)")
    for file_path in result:
        print(f"  - {file_path}")
