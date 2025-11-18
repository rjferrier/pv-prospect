import argparse
from pathlib import Path

from pv_prospect.data_extraction.loaders import get_storage_client


def upload_file(
    source_file: str,
    destination_path: str,
    local_dir: str | None = None,
    overwrite: bool = False,
):
    """
    Upload a CSV file to storage (Google Drive or local directory).

    Args:
        source_file: Path to the local file to upload
        destination_path: Destination path in storage (e.g., 'data/pvoutput/file.csv')
        local_dir: If provided, upload to local directory instead of Google Drive
        overwrite: If True, overwrite existing files
    """
    source_path = Path(source_file)

    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_file}")

    if not source_path.is_file():
        raise ValueError(f"Source path is not a file: {source_file}")

    # Get the appropriate storage client
    storage_client = get_storage_client(local_dir)

    # Check if file already exists
    if storage_client.file_exists(destination_path) and not overwrite:
        raise FileExistsError(
            f"File already exists at destination: {destination_path}\n"
            "Use --overwrite flag to replace it."
        )

    # Read the source file
    with open(source_path, 'rb') as f:
        content = f.read()

    # Determine if it's a CSV file
    is_csv = destination_path.lower().endswith('.csv')

    if is_csv:
        # For CSV files, parse and use write_csv
        import csv
        import io
        text_content = content.decode('utf-8')
        reader = csv.reader(io.StringIO(text_content))
        rows = list(reader)

        print(f"Uploading CSV file: {source_file} -> {destination_path}")
        storage_client.write_csv(destination_path, rows, overwrite=overwrite)
        print(f"✓ Successfully uploaded {len(rows)} rows")
    else:
        # For other files, we'll need to implement a generic write method
        # For now, raise an error suggesting CSV files
        raise NotImplementedError(
            "Currently only CSV files are supported. "
            "Please ensure your destination path ends with .csv"
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Upload a CSV file to storage (Google Drive or local directory)'
    )
    parser.add_argument(
        'source_file',
        help='Path to the local CSV file to upload'
    )
    parser.add_argument(
        'destination_path',
        help='Destination path in storage (e.g., "data/pvoutput/file.csv")'
    )
    parser.add_argument(
        '--local-dir',
        help='Upload to local directory instead of Google Drive'
    )
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Overwrite existing files'
    )

    args = parser.parse_args()

    try:
        upload_file(args.source_file, args.destination_path, args.local_dir, args.overwrite)
    except Exception as e:
        print(f"Error: {e}")
        exit(1)
