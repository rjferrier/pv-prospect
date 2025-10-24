import pandas as pd
from pathlib import Path


def load_folder_as_dataframe(folder_path: str) -> pd.DataFrame:
    """
    Load all CSV files from a local folder and concatenate them into a single pandas DataFrame.

    Args:
        folder_path (str): The path to the local folder containing CSV files (e.g., 'data/pvoutput').

    Returns:
        pd.DataFrame: Combined DataFrame from all CSV files in the folder.

    Raises:
        ValueError: If no CSV files are found in the folder.
        FileNotFoundError: If the folder doesn't exist.
    """
    folder = Path(folder_path)

    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    if not folder.is_dir():
        raise ValueError(f"Path is not a directory: {folder_path}")

    # Find all CSV files in the folder
    csv_files = sorted(folder.glob('*.csv'))

    if not csv_files:
        raise ValueError(f"No CSV files found in folder: {folder_path}")

    # Load each CSV file and collect into a list of DataFrames
    dataframes = []
    for csv_file in csv_files:
        print(f"Loading {csv_file.name}...")
        df = pd.read_csv(csv_file)
        dataframes.append(df)

    # Combine all DataFrames
    print(f"Combining {len(dataframes)} CSV files...")
    combined_df = pd.concat(dataframes, ignore_index=True)

    return combined_df


