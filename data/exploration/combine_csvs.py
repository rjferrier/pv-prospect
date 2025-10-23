from pathlib import Path
import argparse
from collections import defaultdict
import re

import pandas as pd


def combine_csvs_in_folder(folder_path: Path, output_path: Path | None = None, base_folder: Path | None = None) -> list[str]:
    """
    Combine CSV files from a single folder, grouping them by family (prefix).
    A CSV family is defined by the filename format {prefix}_YYYYMMDD.csv.

    For each family, all matching CSV files are combined into a single CSV file.
    Only CSVs directly in the given folder are combined (no recursion).

    Args:
        folder_path (Path): The path to the folder containing CSV files.
        output_path (Path, optional): Path where to save the output CSV files. Must be a folder path.
        base_folder (Path, optional): Base folder for relative path calculation (used in recursive mode).

    Returns:
        list[str]: List of paths to the created CSV files (one per family), or empty list if no CSV files found.
    """
    # Only combine CSVs directly in this folder (not in subfolders)
    csv_files = sorted([f for f in folder_path.glob('*.csv') if not f.stem.endswith('_combined')])

    if not csv_files:
        return []

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

        # Load each CSV file and collect into a list of DataFrames
        dataframes = []
        for csv_file in sorted(files):
            print(f"Loading {csv_file.name}...")
            df = pd.read_csv(csv_file)
            dataframes.append(df)

        # Combine all DataFrames for this family
        print(f"Combining {len(dataframes)} CSV files for family '{prefix}' in {folder_path}...")
        combined_df = pd.concat(dataframes, ignore_index=True)

        # Write to CSV
        print(f"Writing to {output_file}...")
        combined_df.to_csv(output_file, index=False)

        print(f"Successfully created {output_file} with {len(combined_df)} rows")
        created_files.append(str(output_file))

    return created_files


def combine_csvs(folder_path: str, output_path: str | None = None, recursive: bool = False) -> list[str]:
    """
    Combine CSV files from a local folder, grouping them by family (prefix).
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
            results = combine_csvs_in_folder(csv_folder, folder_output_path, base_folder=Path(folder_path))
            created_files.extend(results)
    else:
        # Process only the specified folder
        output_path_obj = Path(output_path) if output_path else None
        results = combine_csvs_in_folder(folder, output_path_obj)

        if not results:
            raise ValueError(f"No CSV files found in folder: {folder_path}")

        created_files.extend(results)

    return created_files


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Combine CSV files by family (prefix) in a folder. A CSV family has the format {prefix}_YYYYMMDD.csv")
    parser.add_argument('folder', type=str, help='Path to the folder containing CSV files (e.g., data/pvoutput)')
    parser.add_argument('-o', '--output', type=str, default=None, help='Path to folder where combined CSV files will be saved (optional)')
    parser.add_argument('-r', '--recursive', action='store_true', help='Recursively process all subdirectories')

    args = parser.parse_args()
    result = combine_csvs(args.folder, args.output, args.recursive)

    print(f"\n{'='*60}")
    print(f"Created {len(result)} combined CSV file(s)")
    for file_path in result:
        print(f"  - {file_path}")
