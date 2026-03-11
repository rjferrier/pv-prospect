from pathlib import Path, PurePosixPath

import yaml


def resolve_path(source_or_tracking_path: str) -> str:
    """
    Parse a local .dvc file and resolve the GCS cache blob path.
    """
    with open(source_or_tracking_path) as f:
        dvc_data = yaml.safe_load(f)
    dir_hash = dvc_data['outs'][0]['md5'].removesuffix('.dir')
    return f'files/md5/{dir_hash[:2]}/{dir_hash[2:]}'
