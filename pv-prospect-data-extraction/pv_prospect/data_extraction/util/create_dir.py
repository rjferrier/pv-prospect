import argparse
from typing import Optional

from pv_prospect.data_extraction.loaders import get_storage_client


def create_dir(source_file: Optional[str], destination_path: Optional[str], local_dir: Optional[str]) -> None:
    """Create a directory in storage, using destination_path or falling back to source_file."""
    dest = _derive_dest_for_create(destination_path, source_file)

    storage_client = get_storage_client(local_dir)
    created = storage_client.create_folders(dest)
    if created:
        print(f"✓ Created directory: {dest}")
    else:
        print(f"Directory already exists: {dest}")


def _derive_dest_for_create(destination_path: Optional[str], source_file: Optional[str]) -> str:
    """Derive the destination path used for create-dir mode.

    Prefers destination_path if provided, otherwise falls back to source_file.
    Raises ValueError if neither is provided.
    """
    dest = destination_path or source_file
    if not dest:
        raise ValueError('destination_path is required when creating a directory')
    return dest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create a directory in storage (local or Google Drive).')
    parser.add_argument('source_file', nargs='?', help='Optional: a single positional that may be used as destination when --dest is omitted')
    parser.add_argument('destination_path', nargs='?', help='Destination path to create (e.g., "data/pvoutput")')
    parser.add_argument('--local-dir', help='Use local directory instead of Google Drive')

    args = parser.parse_args()

    try:
        create_dir(args.source_file, args.destination_path, args.local_dir)
    except Exception as e:
        print(f"Error: {e}")
        exit(1)
