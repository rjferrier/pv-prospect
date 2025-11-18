import argparse
import re
from contextlib import contextmanager
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from pv_prospect.data_transformation.csv_transformer import CsvTransformer
from pv_prospect.data_transformation.two_key_dict import TwoKeyDefaultDict

# Pattern: {data_source}_{pv_site_id}_{YYYYMMDD} where pv_site_id and YYYYMMDD are digits
CSV_FILENAME_PATTERN = re.compile(r'^(.+)_(\d+)_(\d{8})$')


def main(source_path: str, output_path: str | None = None, recursive: bool = False) -> list[str]:
    """
    Combine CSV files from a local folder using Spark, grouping them by data source and PV site.
    A CSV family is defined by the filename format {data_source}_{pv_site_id}_{YYYYMMDD}.csv.

    Optionally recurse through subdirectories.

    Args:
        source_path (str): The path to the local folder containing CSV files (e.g., 'data/pvoutput').
        output_path (str, optional): Path where to save the output CSV files. Must be a folder path.
                                    If None, saves in the same folder as the input files.
        recursive (bool): If True, recursively process all subdirectories that contain CSV files.

    Returns:
        list[str]: List of paths to the created CSV files (one per family found).

    Raises:
        ValueError: If no CSV files are found in the folder.
        FileNotFoundError: If the folder doesn't exist.
    """
    source_path_obj = Path(source_path)

    if not source_path_obj.exists():
        raise FileNotFoundError(f"Folder not found: {source_path}")

    if not source_path_obj.is_dir():
        raise ValueError(f"Path is not a directory: {source_path}")

    output_path_obj = Path(output_path) if output_path else None
    data_collection_strategy = collect_data_recursively if recursive else collect_data

    with get_spark_session() as spark:
        combined_dataframe = data_collection_strategy(spark, source_path_obj)

        family_names_and_dataframes = ... # TODO
        results = coalesce_csv_families(output_path, source_path)

    if not results:
        raise ValueError(f"No CSV files found in folder: {source_path_obj.absolute()}")


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


def collect_data_recursively(spark: SparkSession, source_path: Path) -> TwoKeyDefaultDict[str, str, DataFrame]:
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
    combined_dataframes = TwoKeyDefaultDict()
    for csv_folder in sorted(folders_with_csvs):
        dataframes = collect_data(spark, csv_folder)
        combined_dataframes.update(dataframes)

    return combined_dataframes


def collect_data(spark: SparkSession, source_folder_path: Path) -> TwoKeyDefaultDict[str, str, DataFrame]:
    """
    Collect and combine CSV files from a single folder using Spark, grouping them by family.
    A CSV family is defined by the filename format {data_source}_{pv_site_id}_{YYYYMMDD}.csv.

    For each family, all matching CSV files are combined into a single DataFrame.
    Only CSVs directly in the given folder are processed (no recursion).

    Args:
        spark (SparkSession): The Spark session to use for processing.
        source_folder_path (Path): The path to the folder containing CSV files.

    Returns:
        TwoKeyDefaultDict[str, str, DataFrame]: A dictionary mapping (data_source, pv_site_id)
                                                 to combined DataFrames, or empty dict if no CSV files found.
    """
    # Only combine CSVs directly in this folder (not in subfolders)
    folder_csv_filenames = sorted([f for f in source_folder_path.glob('*.csv') if not f.stem.endswith('_combined')])

    print(f"\n{source_folder_path} -- {len(folder_csv_filenames)} CSV files found")

    if not folder_csv_filenames:
        return TwoKeyDefaultDict()

    csv_filenames = TwoKeyDefaultDict(list)

    for filename in folder_csv_filenames:
        match = CSV_FILENAME_PATTERN.match(filename.stem)
        if match:
            data_source, pv_site_id = match.group(1), match.group(2)
            csv_filenames[data_source, pv_site_id].append(filename)
        else:
            print(f"    Skipped {filename}")

    def to_dataframe(data_source_: str, pv_site_id_: str, filenames_: list[Path]) -> DataFrame:
        print(f'  {data_source_}, {pv_site_id_}: Reading and combining {len(filenames_)} files')
        return CsvTransformer(spark).read_and_combine_csvs_rowwise(filenames_)

    return csv_filenames.map_values(to_dataframe)


def coalesce_csv_families(
        filename_stems_and_dataframes: list[tuple[str, DataFrame]],
        output_folder_path: Path
) -> list[str]:

    created_files = []

    for family_name, dataframe in filename_stems_and_dataframes:
        output_name = f"{family_name}.csv"

        # output_folder_path should be a directory
        output_folder_path.mkdir(parents=True, exist_ok=True)
        output_path = output_folder_path / output_name

        if dataframe is None:
            # Create an empty file if all files failed to load
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text('')
            print(f"  Created empty output file {output_path}")
            created_files.append(str(output_path))
            continue

        # Get total row count
        row_count = dataframe.count()

        # Write the combined dataframe to a single CSV file
        # Using coalesce(1) to write to a single file
        temp_output = output_path.parent / f"{output_path.stem}_temp"
        dataframe.coalesce(1).write.csv(
            str(temp_output),
            header=True,
            mode='overwrite',
            encoding='utf-8'
        )

        # Spark writes to a directory with partition files, so we need to move the actual CSV
        csv_parts = list(temp_output.glob('*.csv'))
        if csv_parts:
            # Move the CSV file to the desired location
            csv_parts[0].rename(output_path)
            # Clean up the temp directory
            import shutil
            shutil.rmtree(temp_output)
            print(f"  Successfully created {output_path} with {row_count} data rows")
        else:
            print(f"  Warning: No CSV file generated for {family_name}")
            continue

        created_files.append(str(output_path))

    return created_files


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Combine CSV files by family in a folder using Apache Spark. A CSV family has the format {data_source}_{pv_site_id}_{YYYYMMDD}.csv")
    parser.add_argument('folder', type=str, help='Path to the folder containing CSV files (e.g., data/pvoutput)')
    parser.add_argument('-o', '--output', type=str, default=None, help='Path to folder where combined CSV files will be saved (optional)')
    parser.add_argument('-r', '--recursive', action='store_true', help='Recursively process all subdirectories')

    args = parser.parse_args()
    result = main(args.folder, args.output, args.recursive)

    print(f"\n{'='*60}")
    print(f"Created {len(result)} combined CSV file(s)")
    for file_path in result:
        print(f"  - {file_path}")
