import argparse
from pathlib import Path

from pv_prospect.etl import Extractor, Loader
from pv_prospect.etl.storage.factory import AnyStorageConfig, get_filesystem


def upload_file(
    source_file: str,
    destination_path: str,
    local_dir: str | None = None,
    overwrite: bool = False,
) -> None:
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
        raise FileNotFoundError(f'Source file not found: {source_file}')

    if not source_path.is_file():
        raise ValueError(f'Source path is not a file: {source_file}')

    storage_config: AnyStorageConfig
    if local_dir:
        from pv_prospect.etl.storage.backends.local import LocalStorageConfig

        storage_config = LocalStorageConfig(prefix=local_dir)
    else:
        from pv_prospect.common.config_parser import get_config
        from pv_prospect.data_extraction.config import DataExtractionConfig
        from pv_prospect.data_sources import get_config_dir as get_ds_config_dir
        from pv_prospect.etl import get_config_dir as get_etl_config_dir

        storage_config = get_config(
            DataExtractionConfig,
            base_config_dirs=[get_etl_config_dir(), get_ds_config_dir()],
        ).staged_raw_data_storage

    fs = get_filesystem(storage_config)
    extractor = Extractor(fs)
    loader = Loader(fs)

    # Check if file already exists
    if extractor.file_exists(destination_path) and not overwrite:
        raise FileExistsError(
            f'File already exists at destination: {destination_path}\n'
            'Use --overwrite flag to replace it.'
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

        print(f'Uploading CSV file: {source_file} -> {destination_path}')
        loader.write_csv(destination_path, rows, overwrite=overwrite)
        print(f'✓ Successfully uploaded {len(rows)} rows')
    else:
        # For other files, we'll need to implement a generic write method
        # For now, raise an error suggesting CSV files
        raise NotImplementedError(
            'Currently only CSV files are supported. '
            'Please ensure your destination path ends with .csv'
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Upload a CSV file to storage (Google Drive or local directory)'
    )
    parser.add_argument('source_file', help='Path to the local CSV file to upload')
    parser.add_argument(
        'destination_path',
        help='Destination path in storage (e.g., "data/pvoutput/file.csv")',
    )
    parser.add_argument(
        '--local-dir', help='Upload to local directory instead of Google Drive'
    )
    parser.add_argument(
        '--overwrite', action='store_true', help='Overwrite existing files'
    )

    args = parser.parse_args()

    try:
        upload_file(
            args.source_file, args.destination_path, args.local_dir, args.overwrite
        )
    except Exception as e:
        print(f'Error: {e}')
        exit(1)
