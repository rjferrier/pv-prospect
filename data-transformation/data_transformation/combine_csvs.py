from pathlib import Path
import argparse
from collections import defaultdict
import re

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def get_spark_session(app_name: str = "CombineCSVs") -> SparkSession:
    """
    Create or get a Spark session for CSV processing.

    Args:
        app_name: Name for the Spark application

    Returns:
        SparkSession: Configured Spark session
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()


def read_and_combine_csvs(spark_session: SparkSession, csv_files: list[Path]) -> DataFrame | None:
    """
    Read and combine multiple CSV files efficiently using Spark's native capabilities.

    This approach avoids creating large task binaries by:
    1. Reading multiple files at once when they have the same schema
    2. Using Spark's built-in file reading capabilities
    3. Avoiding accumulation of many DataFrame objects in memory

    Args:
        spark_session: Spark session to use for reading
        csv_files: List of CSV file paths to read

    Returns:
        Combined DataFrame or None if all reads failed
    """
    if not csv_files:
        return None

    print(f"  Reading and combining {len(csv_files)} CSV file(s)...")

    # Convert paths to strings
    file_paths = [str(f) for f in sorted(csv_files)]

    try:
        # Spark can read multiple files at once - this is much more efficient
        # than reading them individually and prevents large task binary warnings
        if len(file_paths) == 1:
            df = spark_session.read.csv(
                file_paths[0],
                header=True,
                inferSchema=True,
                encoding='utf-8'
            )
        else:
            # Read multiple files at once - Spark handles this efficiently
            # Use a comma-separated list of paths
            df = spark_session.read.csv(
                file_paths,
                header=True,
                inferSchema=True,
                encoding='utf-8'
            )

        print(f"  Successfully loaded {len(csv_files)} file(s)")
        return df

    except Exception as e:
        print(f"  Error reading CSV files: {e}")
        print(f"  Falling back to individual file reading...")

        # Fallback: read files individually and union them
        # But do it in batches to avoid large task binaries
        batch_size = 5
        combined_df = None

        for i in range(0, len(file_paths), batch_size):
            batch = file_paths[i:i+batch_size]
            try:
                batch_df = spark_session.read.csv(
                    batch,
                    header=True,
                    inferSchema=True,
                    encoding='utf-8'
                )

                if combined_df is None:
                    combined_df = batch_df
                else:
                    combined_df = combined_df.unionByName(batch_df, allowMissingColumns=False)

            except Exception as batch_error:
                print(f"  Error reading batch {i//batch_size + 1}: {batch_error}")
                continue

        if combined_df is not None:
            print(f"  Successfully loaded files using fallback method")

        return combined_df


def combine_csvs_in_folder(folder_path: Path, output_path: Path | None = None, spark: SparkSession | None = None) -> list[str]:
    """
    Combine CSV files from a single folder using Spark, grouping them by family (prefix).
    A CSV family is defined by the filename format {prefix}_YYYYMMDD.csv.

    For each family, all matching CSV files are combined into a single CSV file.
    Only CSVs directly in the given folder are combined (no recursion).

    Args:
        folder_path (Path): The path to the folder containing CSV files.
        output_path (Path, optional): Path where to save the output CSV files. Must be a folder path.
        spark (SparkSession, optional): Spark session to use. If None, creates a new one.

    Returns:
        list[str]: List of paths to the created CSV files (one per family), or empty list if no CSV files found.
    """
    # Only combine CSVs directly in this folder (not in subfolders)
    csv_files = sorted([f for f in folder_path.glob('*.csv') if not f.stem.endswith('_combined')])

    if not csv_files:
        return []

    # Get or create Spark session
    spark_session = spark if spark is not None else get_spark_session()

    # Group CSV files by family (prefix)
    # Pattern: {prefix}_YYYYMMDD.csv where YYYYMMDD is 8 digits
    families = defaultdict(list)
    date_pattern = re.compile(r'^(.+)_(\d{8})$')

    for csv_file in csv_files:
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
            output_file = folder_path / output_name
        else:
            # output_path should be a directory
            output_path.mkdir(parents=True, exist_ok=True)
            output_file = output_path / output_name

        print(f"Processing family: {prefix} ({len(files)} file(s))")

        # Read and combine CSV files efficiently
        combined_df = read_and_combine_csvs(spark_session, files)

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


def combine_csvs(folder_path: str, output_path: str | None = None, recursive: bool = False) -> list[str]:
    """
    Combine CSV files from a local folder using Spark, grouping them by family (prefix).
    A CSV family is defined by the filename format {prefix}_YYYYMMDD.csv.

    Optionally recurse through subdirectories.

    Args:
        folder_path (str): The path to the local folder containing CSV files (e.g., 'data/pvoutput').
        output_path (str, optional): Path where to save the output CSV files. Must be a folder path.
                                    If None, saves in the same folder as the input files.
        recursive (bool): If True, recursively process all subdirectories that contain CSV files.

    Returns:
        list[str]: List of paths to the created CSV files (one per family found).

    Raises:
        ValueError: If no CSV files are found in the folder (and not recursive).
        FileNotFoundError: If the folder doesn't exist.
    """
    folder = Path(folder_path)

    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    if not folder.is_dir():
        raise ValueError(f"Path is not a directory: {folder_path}")

    # Create a single Spark session for all processing
    spark = get_spark_session()

    try:
        created_files = []

        if recursive:
            # Find all directories that contain CSV files (directly, not in subfolders)
            folders_with_csvs = set()
            for csv_file in Path(folder_path).rglob('*.csv'):
                if not csv_file.stem.endswith('_combined'):
                    folders_with_csvs.add(csv_file.parent)
            if not folders_with_csvs:
                raise ValueError(f"No CSV files found in folder tree: {folder_path}")
            print(f"Found {len(folders_with_csvs)} folders with CSV files")
            # For each folder, combine CSVs by family
            for csv_folder in sorted(folders_with_csvs):
                print(f"\nProcessing folder: {csv_folder}")
                if output_path is None:
                    folder_output_path = None
                else:
                    output_path_obj = Path(output_path)
                    output_path_obj.mkdir(parents=True, exist_ok=True)
                    folder_output_path = output_path_obj
                results = combine_csvs_in_folder(csv_folder, folder_output_path, spark)
                created_files.extend(results)
        else:
            # Process only the specified folder
            output_path_obj = Path(output_path) if output_path else None
            results = combine_csvs_in_folder(folder, output_path_obj, spark)

            if not results:
                raise ValueError(f"No CSV files found in folder: {folder_path}")

            created_files.extend(results)

        return created_files

    finally:
        # Stop the Spark session
        spark.stop()


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
