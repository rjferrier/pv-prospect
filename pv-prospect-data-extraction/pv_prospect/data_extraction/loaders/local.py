import csv
import io
import json
import shutil
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
    
    def create_folder(self, folder_path: str) -> str | None:
        """
        Create a folder structure in the local storage.

        Args:
            folder_path: The relative path to the folder (e.g., 'data/pvoutput')

        Returns:
            str | None: The full path to the folder if it was created, or None if it already existed
        """
        full_path = self.base_dir / folder_path

        # Check if folder already exists
        if full_path.exists():
            return None

        full_path.mkdir(parents=True, exist_ok=True)
        print(f"    Created folder: {full_path}")
        return str(full_path)

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
    
    def read_file(self, file_path: str) -> io.TextIOWrapper:
        """
        Read a file from local storage and return it as a text stream.

        Args:
            file_path: The relative path to the file (e.g., 'data/pvoutput/file.csv')

        Returns:
            io.TextIOWrapper: A text stream containing the file content with UTF-8 encoding

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        full_path = self.base_dir / file_path

        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")

        return open(full_path, 'r', encoding='utf-8')

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

    def list_files(self, folder_path: str | None = None, pattern: str = "*", recursive: bool = False) -> list[dict]:
        """
        List files in a local directory.

        Args:
            folder_path: The relative path to the folder (e.g., 'data/pvoutput'). If None, lists from base_dir.
            pattern: Glob pattern for matching files (e.g., '*.csv')
            recursive: If True, recursively list files in subdirectories

        Returns:
            List of dicts with 'id' (path), 'name', 'path' (relative to base_dir), and 'parent_path'
        """
        if folder_path:
            search_dir = self.base_dir / folder_path
        else:
            search_dir = self.base_dir

        if not search_dir.exists():
            return []

        files = []
        glob_pattern = f"**/{pattern}" if recursive else pattern

        for file_path in search_dir.glob(glob_pattern):
            if file_path.is_file():
                relative_path = file_path.relative_to(self.base_dir)
                parent_path = str(relative_path.parent) if relative_path.parent != Path('.') else ''

                files.append({
                    'id': str(relative_path),  # Use relative path as ID
                    'name': file_path.name,
                    'path': str(relative_path),
                    'parent_path': parent_path,
                    'created_time': file_path.stat().st_ctime,
                })

        return files

    def rename_file(self, file_path: str, new_name: str) -> None:
        """
        Rename a file in local storage.

        Args:
            file_path: The current relative path to the file
            new_name: The new name for the file (not a full path, just the filename)
        """
        full_path = self.base_dir / file_path

        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")

        new_path = full_path.parent / new_name
        full_path.rename(new_path)

    def move_file(self, file_path: str, new_parent_path: str) -> None:
        """
        Move a file to a different directory in local storage.

        Args:
            file_path: The current relative path to the file
            new_parent_path: The relative path to the new parent directory
        """
        full_path = self.base_dir / file_path

        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")

        new_parent_full = self.base_dir / new_parent_path
        new_parent_full.mkdir(parents=True, exist_ok=True)

        new_path = new_parent_full / full_path.name
        full_path.rename(new_path)

    def trash_file(self, file_path: str) -> None:
        """
        Move a file to a .trash subdirectory in local storage.

        Args:
            file_path: The relative path to the file
        """
        full_path = self.base_dir / file_path

        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")

        # Create a .trash directory
        trash_dir = self.base_dir / '.trash'
        trash_dir.mkdir(exist_ok=True)

        # Move to trash, preserving directory structure
        relative_path = Path(file_path)
        trash_dest = trash_dir / relative_path
        trash_dest.parent.mkdir(parents=True, exist_ok=True)

        # If file already exists in trash, add a timestamp
        if trash_dest.exists():
            import time
            timestamp = int(time.time())
            trash_dest = trash_dest.parent / f"{trash_dest.stem}_{timestamp}{trash_dest.suffix}"

        shutil.move(str(full_path), str(trash_dest))
