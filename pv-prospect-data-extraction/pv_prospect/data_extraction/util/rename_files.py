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


def list_files_recursive(client: GDriveClient, folder_id: str | None, mime_type: str | None = None, prefix: str = "") -> list[dict]:
    """
    Recursively list all files in the specified folder, including subdirectories.
    Returns list of dicts with 'id', 'name', 'path' (relative path from search root), and 'parent_id'.
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
            'parent_id': folder_id if folder_id else file.get('parents', [None])[0]
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


def _split_path(path: str) -> tuple[str, str]:
    """
    Split a path into directory and filename.
    Returns (directory, filename). Directory is empty string if no directory.
    Examples:
        'file.txt' -> ('', 'file.txt')
        'dir/file.txt' -> ('dir', 'file.txt')
        'dir1/dir2/file.txt' -> ('dir1/dir2', 'file.txt')
    """
    if '/' in path:
        parts = path.rsplit('/', 1)
        return parts[0], parts[1]
    return '', path


def process_files(folder_path: str | None, pattern: str, replacement: str, mime_type: str | None = None, dry_run: bool = False, delete_on_duplicate: bool = False):
    """
    Rename files in a Google Drive folder according to a regex pattern.
    Supports directory changes in pattern/replacement (e.g., 'olddir/(.*)', 'newdir/\1').

    Args:
        folder_path: Path to folder in Google Drive (e.g., 'data/pvoutput'). If None, searches from root.
        pattern: Regex pattern to match against file paths (can include directories)
        replacement: Replacement string (can use regex groups like \\1, \\2, and specify new directory)
        mime_type: Optional MIME type filter
        dry_run: If True, only print what would be renamed without actually renaming
        delete_on_duplicate: If True, delete existing files with the same name instead of skipping
    """
    client = GDriveClient.build_service()
    root_folder_id = get_folder_id(client, folder_path)
    files = list_files_recursive(client, root_folder_id, mime_type)

    regex = re.compile(pattern)
    renamed_count = 0
    deleted_count = 0

    # Build a map of existing file paths to file IDs for duplicate detection
    existing_files = {f['path']: f for f in files}

    for file in files:
        file_id = file['id']
        old_path = file['path']

        new_path = regex.sub(replacement, old_path)

        if new_path != old_path:
            # Parse old and new paths
            old_dir, old_name = _split_path(old_path)
            new_dir, new_name = _split_path(new_path)

            # Check if target file already exists (and it's not the same file)
            existing_target = existing_files.get(new_path)
            if existing_target and existing_target['id'] != file_id:
                if delete_on_duplicate:
                    if dry_run:
                        print(f"Would delete duplicate: {new_path} (id: {existing_target['id']})")
                        deleted_count += 1
                    else:
                        try:
                            client.trash_file(existing_target['id'])
                            print(f"Deleted duplicate: {new_path} (id: {existing_target['id']})")
                            # Remove from map so we don't try to process it later
                            del existing_files[new_path]
                            deleted_count += 1
                        except Exception as e:
                            print(f"Error deleting duplicate {existing_target['id']}: {e}")
                            continue
                else:
                    print(f"Skipping: {old_path} -> {new_path} (target already exists)")
                    continue

            if dry_run:
                print(f"Would rename: {old_path} -> {new_path}")
            else:
                # Determine if we need to move to a different folder
                if old_dir != new_dir:
                    # Build new parent path
                    if folder_path and new_dir:
                        new_parent_path = f"{folder_path}/{new_dir}"
                    elif new_dir:
                        new_parent_path = new_dir
                    else:
                        new_parent_path = folder_path

                    new_parent_id = get_folder_id(client, new_parent_path) if new_parent_path else None
                    if new_parent_id is None:
                        new_parent_id = root_folder_id
                    client.move_file(file_id, file['parent_id'], new_parent_id)
                    print(f"Moved: {old_path} -> {new_path}")

                # Rename if name changed
                if old_name != new_name:
                    client.rename_file(file_id, new_name)
                    if old_dir == new_dir:
                        print(f"Renamed: {old_path} -> {new_path}")

                # Update the existing_files map with the new path
                existing_files[new_path] = file

            renamed_count += 1
        else:
            print(f"Skipping: {old_path} (no match)")

    print(f"\n{'Would rename' if dry_run else 'Renamed/moved'} {renamed_count} file(s)")
    if delete_on_duplicate and deleted_count > 0:
        print(f"{'Would delete' if dry_run else 'Deleted'} {deleted_count} duplicate file(s)")



if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Rename files in a Google Drive folder using regex pattern matching'
    )
    parser.add_argument(
        'folder_path',
        nargs='?',
        default=None,
        help='Path to folder in Google Drive (e.g., "data/pvoutput"). If omitted, searches from root.'
    )
    parser.add_argument(
        'pattern',
        help='Regex pattern to match against filenames'
    )
    parser.add_argument(
        'replacement',
        help='Replacement string (can use regex groups like \\1, \\2)'
    )
    parser.add_argument(
        '--mime-type',
        help='Optional MIME type filter (e.g., "text/csv")',
        default=None
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be renamed without actually renaming'
    )
    parser.add_argument(
        '--delete-on-duplicate',
        action='store_true',
        help='Delete existing files if renaming would cause a duplicate'
    )

    args = parser.parse_args()

    process_files(
        args.folder_path,
        args.pattern,
        args.replacement,
        args.mime_type,
        args.dry_run,
        args.delete_on_duplicate
    )
