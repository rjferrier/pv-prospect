from pathlib import Path

import pandas as pd


CSV_LOAD_BATCH_SIZE = 5


def read_and_combine_csv_rows(directory: Path, filename_or_filenames: str | list[str]) -> pd.DataFrame | None:
    """
    Read and combine multiple CSV files efficiently using pandas.

    This function reads CSV files and concatenates them into a single DataFrame.
    It first attempts to read all files at once, and falls back to batch
    processing if that fails.

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
        file_paths = [str(directory / filename_or_filenames)]
    else:
        file_paths = [str(directory / f) for f in filename_or_filenames]

    try:
        df = _read_and_concat_csv(file_paths)

    except Exception as e:
        print(f"  Error reading CSV files: {e}")
        if len(file_paths) == 1:
            raise

        print(f"  Falling back to individual file reading...")
        df = _combine_in_batches(file_paths)
        if df is not None:
            print(f"  Successfully loaded files using fallback method")

    return df


def _combine_in_batches(file_paths: list[str]) -> pd.DataFrame | None:
    """
    Fallback function to read files individually and concatenate them in batches.

    This approach processes files in batches to avoid memory issues
    that can occur when loading many large files at once. Files are read
    in groups defined by CSV_LOAD_BATCH_SIZE and combined using pd.concat.

    Args:
        file_paths: List of absolute file path strings to read.

    Returns:
        Combined DataFrame containing all rows from successfully read files,
        or None if all batches failed to load.
    """
    batch_df = None

    for i in range(0, len(file_paths), CSV_LOAD_BATCH_SIZE):
        paths = file_paths[i:i + CSV_LOAD_BATCH_SIZE]
        try:
            df = _read_and_concat_csv(paths)

            if batch_df is None:
                batch_df = df
            else:
                batch_df = pd.concat([batch_df, df], ignore_index=True)

        except Exception as batch_error:
            print(f"  Error reading batch {i // CSV_LOAD_BATCH_SIZE + 1}: {batch_error}")
            continue

    if batch_df is not None:
        print(f"  Successfully loaded files using fallback method")

    return batch_df


def _read_and_concat_csv(file_paths: list[str]) -> pd.DataFrame:
    """
    Read and concatenate CSV file(s) using pandas with standard settings.

    Configuration:
    - encoding='utf-8': Use UTF-8 character encoding
    - ignore_index=True: Create a new index for the concatenated DataFrame

    Args:
        file_paths: List of file path strings to read.

    Returns:
        DataFrame loaded and concatenated from the CSV file(s).

    Raises:
        Exception: If the CSV file(s) cannot be read or parsed.
    """
    dfs = []
    for path in file_paths:
        df = pd.read_csv(path, encoding='utf-8')
        dfs.append(df)

    if len(dfs) == 1:
        return dfs[0]

    return pd.concat(dfs, ignore_index=True)
