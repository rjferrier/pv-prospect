import pandas as pd
from src.loaders.gdrive import DATA_FOLDER_NAME, GDriveClient


def load_csv(path: str) -> pd.DataFrame:
    """
    Load a CSV file from Google Drive into a pandas DataFrame.

    Args:
        path (str): The path to the file in Google Drive (e.g., 'pvoutput/12345.csv').
        The 'data' folder is assumed to be the root.

    Returns:
        pd.DataFrame: The loaded CSV data.
    """
    client = GDriveClient.build_service()
    resolved_file_path = client.resolve_path(f'{DATA_FOLDER_NAME}/{path}')

    # Download the file to a stream and read it with pandas
    with client.download_to_stream(resolved_file_path) as stream:
        return pd.read_csv(stream)


def load_folder_as_dataframe(folder_path: str) -> pd.DataFrame:
    """
    Download all CSV files from a Google Drive folder and combine them into a single pandas DataFrame.

    Args:
        folder_path (str): The path to the folder in Google Drive (e.g., 'pvoutput').
                          The 'data' folder is assumed to be the root.

    Returns:
        pd.DataFrame: Combined DataFrame from all CSV files in the folder.

    Raises:
        ValueError: If no CSV files are found in the folder.
    """
    client = GDriveClient.build_service()

    # Navigate to the folder and get its ID
    from src.loaders.gdrive import ResolvedFilePath
    parts = [p for p in folder_path.split('/') if p]
    parent_id = None

    # Start with DATA_FOLDER_NAME
    data_folder = ResolvedFilePath(name=DATA_FOLDER_NAME, parent_id=None)
    parent_id = client.create_or_get_folder(data_folder)

    # Navigate through the rest of the path
    for part in parts:
        folder = ResolvedFilePath(name=part, parent_id=parent_id)
        parent_id = client.create_or_get_folder(folder)

    # Search for all CSV files in the folder
    from src.loaders.gdrive import CSV_MIME_TYPE
    search_path = ResolvedFilePath(name=None, parent_id=parent_id)
    csv_files = client.search(search_path, mime_type=CSV_MIME_TYPE)

    if not csv_files:
        raise ValueError(f"No CSV files found in folder: {folder_path}")

    # Download each CSV and collect into a list of DataFrames
    dataframes = []
    for file_info in csv_files:
        file_name = file_info['name']
        print(f"Loading {file_name}...")

        # Create a ResolvedFilePath for this file
        file_path = ResolvedFilePath(name=file_name, parent_id=parent_id)

        # Download and read the CSV
        with client.download_to_stream(file_path, mime_type=CSV_MIME_TYPE) as stream:
            df = pd.read_csv(stream)
            dataframes.append(df)

    # Combine all DataFrames
    print(f"Combining {len(dataframes)} CSV files...")
    combined_df = pd.concat(dataframes, ignore_index=True)

    return combined_df
