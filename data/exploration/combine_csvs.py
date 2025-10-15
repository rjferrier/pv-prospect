import datetime
from pathlib import Path
import argparse

import pandas as pd


def combine_csvs_in_folder(folder_path: Path, output_path: Path | None = None, base_folder: Path | None = None) -> str | None:
    """
    Combine all CSV files from a single folder into a single CSV file.
    Only CSVs directly in the given folder are combined (no recursion).

    The output filename is derived from the constituent CSV files by removing the date suffix.
    For example, 'pvoutput_89665_20240730.csv' becomes 'pvoutput_89665.csv'.

    Args:
        folder_path (Path): The path to the folder containing CSV files.
        output_path (Path, optional): Path where to save the output CSV. Can be a folder path or a full file path.
        base_folder (Path, optional): Base folder for relative path calculation (used in recursive mode).

    Returns:
        str | None: Path to the created CSV file, or None if no CSV files found.
    """
    # Only combine CSVs directly in this folder (not in subfolders)
    csv_files = sorted([f for f in folder_path.glob('*.csv') if not f.stem.endswith('_combined')])

    if not csv_files:
        return None

    # Determine output filename from the first CSV file
    # Remove date suffix (last underscore-separated segment if it looks like a date)
    first_file = csv_files[0]
    name_parts = first_file.stem.split('_')

    # Check if last part looks like a date (8 digits)
    if len(name_parts) > 1 and name_parts[-1].isdigit() and len(name_parts[-1]) == 8:
        output_name = '_'.join(name_parts[:-1]) + '.csv'
    else:
        output_name = first_file.stem + '_combined.csv'

    # No longer adding folder prefix - just use the base filename

    if output_path is None:
        output_file = folder_path / output_name
    else:
        # Check if output_path is a directory or a file path
        if output_path.is_dir() or (not output_path.exists() and not output_path.suffix):
            # It's a directory - use the auto-generated filename
            output_path.mkdir(parents=True, exist_ok=True)
            output_file = output_path / output_name
        else:
            # It's a file path - use it directly
            output_file = output_path
            # Create parent directory if it doesn't exist
            output_file.parent.mkdir(parents=True, exist_ok=True)

    # Load each CSV file and collect into a list of DataFrames
    dataframes = []
    for csv_file in csv_files:
        print(f"Loading {csv_file.name}...")
        df = pd.read_csv(csv_file)
        dataframes.append(df)

    # Combine all DataFrames
    print(f"Combining {len(dataframes)} CSV files in {folder_path}...")
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Write to CSV
    print(f"Writing to {output_file}...")
    combined_df.to_csv(output_file, index=False)

    print(f"Successfully created {output_file} with {len(combined_df)} rows")
    return str(output_file)


def combine_csvs(folder_path: str, output_path: str | None = None, recursive: bool = False) -> list[str]:
    """
    Combine all CSV files from a local folder into a single CSV file.
    Optionally recurse through subdirectories.

    The output filename is derived from the constituent CSV files by removing the date suffix.
    For example, 'pvoutput_89665_20240730.csv' becomes 'pvoutput_89665.csv'.

    Args:
        folder_path (str): The path to the local folder containing CSV files (e.g., 'data/pvoutput').
        output_path (str, optional): Path where to save the output CSV. Can be a folder path or a full file path.
                                    If a folder path, the auto-generated filename will be used.
                                    If None, saves in the same folder as the input files.
        recursive (bool): If True, recursively process all subdirectories that contain CSV files.

    Returns:
        list[str]: List of paths to the created CSV files.

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
        # For each folder, only combine CSVs directly in that folder
        for csv_folder in sorted(folders_with_csvs):
            print(f"\nProcessing folder: {csv_folder}")
            if output_path is None:
                folder_output_path = None
            else:
                output_path_obj = Path(output_path)
                output_path_obj.mkdir(parents=True, exist_ok=True)
                folder_output_path = output_path_obj
            result = combine_csvs_in_folder(csv_folder, folder_output_path, base_folder=Path(folder_path))
            if result:
                created_files.append(result)
    else:
        # Process only the specified folder
        output_path_obj = Path(output_path) if output_path else None
        result = combine_csvs_in_folder(folder, output_path_obj)

        if result is None:
            raise ValueError(f"No CSV files found in folder: {folder_path}")

        created_files.append(result)

    return created_files


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Combine all CSV files in a folder into a single CSV file.")
    parser.add_argument('folder', type=str, help='Path to the folder containing CSV files (e.g., data/pvoutput)')
    parser.add_argument('-o', '--output', type=str, default=None, help='Path to save the combined CSV file (optional)')
    parser.add_argument('-r', '--recursive', action='store_true', help='Recursively process all subdirectories')

    args = parser.parse_args()
    result = combine_csvs(args.folder, args.output, args.recursive)

    print(f"\n{'='*60}")
    print(f"Summary: Created {len(result)} combined CSV file(s)")
    print(f"{'='*60}")
