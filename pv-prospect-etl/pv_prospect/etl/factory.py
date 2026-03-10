from pv_prospect.etl.extractors.gcs import GcsExtractor
from pv_prospect.etl.extractors.local import LocalExtractor
from pv_prospect.etl.extractors.protocol import Extractor
from pv_prospect.etl.loaders.gcs import GcsLoader
from pv_prospect.etl.loaders.local import LocalLoader
from pv_prospect.etl.loaders.protocol import Loader


def get_loader(local_dir: str | None) -> Loader:
    """
    Get a loader for file operations.

    Args:
        local_dir: If provided, returns a LocalLoader using this as the base directory.
                   If None, returns a GcsLoader.

    Returns:
        Loader: Either a LocalLoader or GcsLoader
    """
    if local_dir:
        print(f"Using local loader at {local_dir}")
        return LocalLoader(local_dir)
    else:
        print("Using GCS loader")
        return GcsLoader()


def get_extractor(local_dir: str | None) -> Extractor:
    """
    Get an extractor for reading operations.

    Args:
        local_dir: If provided, returns a LocalExtractor using this as the base directory.
                   If None, returns a GcsExtractor.

    Returns:
        Extractor: Either a LocalExtractor or GcsExtractor
    """
    if local_dir:
        print(f"Using local extractor at {local_dir}")
        return LocalExtractor(local_dir)
    else:
        print("Using GCS extractor")
        return GcsExtractor()
