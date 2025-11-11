import argparse
from collections import defaultdict
from loaders.gdrive import GDriveClient, ResolvedFilePath


def get_folder_id(client: GDriveClient, folder_path: str | None) -> str | None:
    """
    Get folder ID from a path like 'pvoutput'.
    Returns the root data folder ID  if folder_path is None.
    """
    if folder_path is None:
        return None

    parts = folder_path.split('/')
    parent_id = None

    for part in parts:
        resolved_path = ResolvedFilePath(name=part, parent_id=parent_id)
        parent_id = client.get_folder(resolved_path)

    return parent_id


def list_files_recursive(client: GDriveClient, folder_id: str | None, mime_type: str | None = None, prefix: str = "") -> list[dict]:
    """
    Recursively list all files in the specified folder, including subdirectories.
    Returns list of dicts with 'id', 'name', 'path' (relative path from search root), 'parent_id', and 'createdTime'.
    If folder_id is None, searches from root.
    """
    files_with_paths = []

    # Get all files in current folder
    search_path = ResolvedFilePath(parent_id=folder_id)
    files = client.search(search_path, mime_type=mime_type)

    for file in files:
        file_path = f"{prefix}{file['name']}" if prefix else file['name']
        files_with_paths.append({
            'id': file['id'],
            'name': file['name'],
            'path': file_path,
            'parent_id': folder_id if folder_id else file.get('parents', [None])[0],
            'createdTime': file.get('createdTime', '')
        })

    # Get all subfolders and recurse
    folder_search_path = ResolvedFilePath(parent_id=folder_id)
    folders = client.search(folder_search_path, mime_type='application/vnd.google-apps.folder')
    for folder in folders:
        folder_path = f"{prefix}{folder['name']}/" if prefix else f"{folder['name']}/"
        files_with_paths.extend(
            list_files_recursive(client, folder['id'], mime_type, folder_path)
        )

    return files_with_paths


def remove_duplicate_csvs(folder_path: str | None, dry_run: bool = False):
    """
    Remove duplicate CSV files with identical names in the same folder.
    Keeps the latest file (by creation time) and removes all others.

    Args:
        folder_path: Path to folder in Google Drive (e.g., 'data/pvoutput'). If None, searches from root.
        dry_run: If True, only print what would be deleted without actually deleting
    """
    client = GDriveClient.build_service()
    parent_folder_id = get_folder_id(client, folder_path)
    
    # Only search for CSV files
    files = list_files_recursive(client, parent_folder_id, mime_type='text/csv')

    # Group files by (parent_id, name) to find duplicates in the same folder
    files_by_location = defaultdict(list)
    for file in files:
        key = (file['parent_id'], file['name'])
        files_by_location[key].append(file)

    deleted_count = 0

    for (parent_id, name), file_group in files_by_location.items():
        if len(file_group) > 1:
            # Sort by creation time in reverse to keep the latest
            file_group.sort(key=lambda f: f['createdTime'], reverse=True)

            # Keep the first (latest), mark rest as duplicates
            kept_file = file_group[0]
            duplicates = file_group[1:]

            print(f"\nFound {len(duplicates)} duplicate(s) of '{kept_file['path']}'")
            print(f"  Keeping: {kept_file['id']} (created {kept_file['createdTime']})")

            for dup in duplicates:
                if dry_run:
                    print(f"  Would delete: {dup['id']} (created {dup['createdTime']})")
                else:
                    try:
                        client.trash_file(dup['id'])
                        print(f"  Deleted: {dup['id']} (created {dup['createdTime']})")
                    except Exception as e:
                        print(f"  Error deleting {dup['id']}: {e}")
                        continue

                deleted_count += 1

    print(f"\n{'Would delete' if dry_run else 'Deleted'} {deleted_count} duplicate CSV file(s)")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Remove duplicate CSV files with identical names in the same folder from Google Drive'
    )
    parser.add_argument(
        'folder_path',
        nargs='?',
        default=None,
        help='Path to folder in Google Drive (e.g., "pvoutput"). If omitted, searches from the root data folder.'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )

    args = parser.parse_args()

    remove_duplicate_csvs(
        args.folder_path,
        args.dry_run
    )
