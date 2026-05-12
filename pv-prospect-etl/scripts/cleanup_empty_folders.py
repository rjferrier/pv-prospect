"""One-off sweep that deletes empty HNS folders under given prefixes.

Earlier consolidate runs left behind empty per-workflow folders at
``tracking/ledger/<run_date>/<workflow>/`` and
``tracking/logs/<run_date>/<workflow>/`` because ``GcsFileSystem.rmdir``
was only deleting legacy zero-byte folder-marker blobs, which HNS
buckets do not use. The fix in ``GcsFileSystem.rmdir`` prevents new
debris, but this script clears what already accumulated.

Run once after deploying the fix:

    poetry run python scripts/cleanup_empty_folders.py pv-prospect-staging \\
        tracking/ledger tracking/logs

Folders that are not empty raise FAILED_PRECONDITION and are skipped
with a log line. Folders that have already been deleted raise NOT_FOUND
and are silently ignored.
"""

import argparse
import logging
import sys

from google.api_core.exceptions import FailedPrecondition, NotFound
from google.cloud import storage_control_v2

logger = logging.getLogger(__name__)


def sweep_prefix(
    client: storage_control_v2.StorageControlClient,
    bucket_name: str,
    prefix: str,
) -> tuple[int, int]:
    """Attempt to delete every folder under *prefix*.

    Returns ``(deleted, skipped)`` counts. ``skipped`` covers folders
    that were not empty.
    """
    parent = f'projects/_/buckets/{bucket_name}'
    request = storage_control_v2.ListFoldersRequest(
        parent=parent, prefix=prefix.rstrip('/') + '/'
    )
    folders = list(client.list_folders(request=request))

    # Delete deepest-first so a parent becomes empty after its children
    # are removed.
    folders.sort(key=lambda f: f.name.count('/'), reverse=True)

    deleted = 0
    skipped = 0
    for folder in folders:
        try:
            client.delete_folder(name=folder.name)
        except NotFound:
            continue
        except FailedPrecondition:
            logger.info('Skipping non-empty folder: %s', folder.name)
            skipped += 1
            continue
        deleted += 1
        logger.info('Deleted %s', folder.name)
    return deleted, skipped


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('bucket', help='Target bucket name (HNS-enabled)')
    parser.add_argument(
        'prefixes',
        nargs='+',
        help='One or more folder prefixes to sweep (e.g. tracking/ledger)',
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format='%(message)s')
    client = storage_control_v2.StorageControlClient()

    total_deleted = 0
    total_skipped = 0
    for prefix in args.prefixes:
        deleted, skipped = sweep_prefix(client, args.bucket, prefix)
        total_deleted += deleted
        total_skipped += skipped

    logger.info(
        'Done. Deleted %d empty folder(s); left %d non-empty folder(s) alone.',
        total_deleted,
        total_skipped,
    )
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
