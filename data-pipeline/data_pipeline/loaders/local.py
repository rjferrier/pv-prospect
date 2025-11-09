import csv
import json
from pathlib import Path
from typing import Iterable


class LocalStorageClient:
    """Client for storing files in a local directory instead of Google Drive."""
    
    def __init__(self, base_dir: str):
        """
        Initialize the local storage client.
        
        Args:
            base_dir: The base directory where files will be stored
        """
        self.base_dir = Path(base_dir).resolve()
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def create_folder(self, folder_path: str) -> None:
        """
        Create a folder structure in the local storage.

        Args:
            folder_path: The relative path to the folder (e.g., 'data/pvoutput')
        """
        full_path = self.base_dir / folder_path
        full_path.mkdir(parents=True, exist_ok=True)
        print(f"    Created folder: {full_path}")

    def file_exists(self, file_path: str) -> bool:
        """
        Check if a file exists in the local storage.
        
        Args:
            file_path: The relative path to the file (e.g., 'data/pvoutput/file.csv')
            
        Returns:
            True if the file exists, False otherwise
        """
        full_path = self.base_dir / file_path
        return full_path.exists()
    
    def write_csv(self, file_path: str, rows: Iterable[Iterable[str]], overwrite: bool = False) -> None:
        """
        Write CSV data to a local file.
        
        Args:
            file_path: The relative path to the file (e.g., 'data/pvoutput/file.csv')
            rows: The CSV data as an iterable of rows
            overwrite: If True, overwrite an existing file; otherwise raise FileExistsError
        """
        full_path = self.base_dir / file_path
        
        # Create parent directories if they don't exist
        full_path.parent.mkdir(parents=True, exist_ok=True)

        # If file exists and overwrite is False, raise
        if full_path.exists() and not overwrite:
            raise FileExistsError(f"File already exists: {full_path}")

        # Write the CSV file (opening with 'w' will overwrite if overwrite=True)
        with open(full_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for row in rows:
                writer.writerow(row)
        
        print(f"    Written to: {full_path}")
    
    def write_metadata(self, csv_file_path: str, metadata: dict) -> None:
        """
        Write JSON metadata to a local file alongside the corresponding CSV file.
        
        Args:
            csv_file_path: The CSV file path (used to derive the metadata filename)
            metadata: The metadata to write as JSON
        """
        # Derive metadata filename from CSV path (replace .csv with .json)
        if csv_file_path.lower().endswith('.csv'):
            metadata_path = csv_file_path[:-4] + '.json'
        else:
            metadata_path = csv_file_path + '.json'
        
        full_path = self.base_dir / metadata_path
        
        # Create parent directories if they don't exist
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write the JSON file
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"    Written metadata to: {full_path}")
