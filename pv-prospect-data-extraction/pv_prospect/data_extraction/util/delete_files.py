import re
import argparse
from pv_prospect.data_extraction.loaders.gdrive import GDriveClient, ResolvedFilePath


def get_folder_id(client: GDriveClient, folder_path: str | None) -> str | None:
    """
    Get folder ID from a path like 'data/pvoutput' or just 'pvoutput'.
    Returns None if folder_path is None (searches from root).
    """
    if folder_path is None:
        return None

    parts = folder_path.split('/')
    parent_id = None

    for part in parts:
        resolved_path = ResolvedFilePath(name=part, parent_id=parent_id)
        parent_id = client.get_folder(resolved_path)

    return parent_id


def list_files_recursive(client: GDriveClient, folder_id: str | None, mime_type: str | None = None, prefix: str = "", include_trashed: bool = False) -> list[dict]:
    """
    Recursively list all files in the specified folder, including subdirectories.
    Returns list of dicts with 'id', 'name', 'path' (relative path from search root), and 'parent_id'.
    If folder_id is None, searches from root.
    """
    files_with_paths = []

    # Get all files in current folder
    search_path = ResolvedFilePath(parent_id=folder_id)
    files = client.search(search_path, mime_type=mime_type, include_trashed=include_trashed)

    for file in files:
        file_path = f"{prefix}{file['name']}" if prefix else file['name']
        files_with_paths.append({
            'id': file['id'],
            'name': file['name'],
            'path': file_path,
            'parent_id': folder_id if folder_id else file.get('parents', [None])[0]
        })

    # Get all subfolders and recurse
    folder_search_path = ResolvedFilePath(parent_id=folder_id)
    folders = client.search(folder_search_path, mime_type='application/vnd.google-apps.folder', include_trashed=include_trashed)
    for folder in folders:
        folder_path = f"{prefix}{folder['name']}/" if prefix else f"{folder['name']}/"
        files_with_paths.extend(
            list_files_recursive(client, folder['id'], mime_type, folder_path, include_trashed)
        )

    return files_with_paths


def delete_files(folder_path: str | None, pattern: str, mime_type: str | None = None, dry_run: bool = False, permanent: bool = False):
    """
    Delete files in a Google Drive folder that match a regex pattern.

    Args:
        folder_path: Path to folder in Google Drive (e.g., 'data/pvoutput'). If None, searches from root.
        pattern: Regex pattern to match against file paths (can include directories)
        mime_type: Optional MIME type filter
        dry_run: If True, only print what would be deleted without actually deleting
        permanent: If True, permanently delete files; if False, move to trash
    """
    client = GDriveClient.build_service()
    root_folder_id = get_folder_id(client, folder_path)
    files = list_files_recursive(client, root_folder_id, mime_type)

    regex = re.compile(pattern)
    deleted_count = 0
    skipped_count = 0

    for file in files:
        file_id = file['id']
        file_path = file['path']

        if regex.search(file_path):
            if dry_run:
                action = "permanently delete" if permanent else "trash"
                print(f"Would {action}: {file_path} (id: {file_id})")
                deleted_count += 1
            else:
                try:
                    if permanent:
                        client.delete_file(file_id)
                        print(f"Permanently deleted: {file_path} (id: {file_id})")
                    else:
                        client.trash_file(file_id)
                        print(f"Trashed: {file_path} (id: {file_id})")
                    deleted_count += 1
                except Exception as e:
                    print(f"Error deleting {file_path} (id: {file_id}): {e}")
        else:
            skipped_count += 1

    print(f"\n{'Would delete' if dry_run else 'Deleted'} {deleted_count} file(s)")
    print(f"Skipped {skipped_count} file(s) that didn't match the pattern")


def restore_files(folder_path: str | None, pattern: str, mime_type: str | None = None, dry_run: bool = False):
    """
    Restore files from trash in a Google Drive folder that match a regex pattern.

    Args:
        folder_path: Path to folder in Google Drive (e.g., 'data/pvoutput'). If None, searches from root.
        pattern: Regex pattern to match against file paths (can include directories)
        mime_type: Optional MIME type filter
        dry_run: If True, only print what would be restored without actually restoring
    """
    client = GDriveClient.build_service()
    root_folder_id = get_folder_id(client, folder_path)
    # Must include trashed files when restoring
    files = list_files_recursive(client, root_folder_id, mime_type, include_trashed=True)

    regex = re.compile(pattern)
    restored_count = 0
    skipped_count = 0

    for file in files:
        file_id = file['id']
        file_path = file['path']

        if regex.search(file_path):
            if dry_run:
                print(f"Would restore: {file_path} (id: {file_id})")
                restored_count += 1
            else:
                try:
                    client.restore_file(file_id)
                    print(f"Restored: {file_path} (id: {file_id})")
                    restored_count += 1
                except Exception as e:
                    print(f"Error restoring {file_path} (id: {file_id}): {e}")
        else:
            skipped_count += 1

    print(f"\n{'Would restore' if dry_run else 'Restored'} {restored_count} file(s)")
    print(f"Skipped {skipped_count} file(s) that didn't match the pattern")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Delete or restore files in a Google Drive folder that match a regex pattern'
    )
    parser.add_argument(
        'folder_path',
        nargs='?',
        default=None,
        help='Path to folder in Google Drive (e.g., "data/pvoutput"). If omitted, searches from root.'
    )
    parser.add_argument(
        'pattern',
        help='Regex pattern to match against file paths'
    )
    parser.add_argument(
        '--mime-type',
        help='Optional MIME type filter (e.g., "text/csv")',
        default=None
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted/restored without actually performing the action'
    )
    parser.add_argument(
        '--permanent',
        action='store_true',
        help='Permanently delete files instead of moving to trash (use with caution!)'
    )
    parser.add_argument(
        '--restore',
        action='store_true',
        help='Restore files from trash instead of deleting'
    )

    args = parser.parse_args()

    # Check for conflicting options
    if args.restore and args.permanent:
        print("ERROR: Cannot use --restore and --permanent together")
        exit(1)

    if args.restore:
        # Restore mode
        restore_files(
            args.folder_path,
            args.pattern,
            args.mime_type,
            args.dry_run
        )
    else:
        # Delete mode
        # Safety check: require --dry-run or explicit confirmation for permanent deletion
        if args.permanent and not args.dry_run:
            print("WARNING: You are about to PERMANENTLY delete files. This cannot be undone!")
            confirm = input("Type 'yes' to confirm: ")
            if confirm.lower() != 'yes':
                print("Aborted.")
                exit(1)

        delete_files(
            args.folder_path,
            args.pattern,
            args.mime_type,
            args.dry_run,
            args.permanent
        )

