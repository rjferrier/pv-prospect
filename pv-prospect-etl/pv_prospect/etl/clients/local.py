from pathlib import Path


class LocalClient:
    """Base client for storing and reading files in a local directory."""

    def __init__(self, base_dir: str):
        """
        Initialize the local storage client.

        Args:
            base_dir: The base directory where files will be stored or read from
        """
        self.base_dir = Path(base_dir).resolve()

    def file_exists(self, file_path: str) -> bool:
        """Check if a file exists in the local storage."""
        full_path = self.base_dir / file_path
        return full_path.exists()
