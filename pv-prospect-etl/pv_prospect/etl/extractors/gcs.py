import io
import json
import yaml

from pv_prospect.etl.clients.gcs import GcsClient
from pv_prospect.common import map_from_env, VarMapping


class GcsExtractor(GcsClient):
    """Storage client that reads blobs from a GCS bucket."""

    def __init__(self, bucket_name: str, prefix: str = None) -> None:
        super().__init__(bucket_name=bucket_name, prefix=prefix)

    @classmethod
    def from_env(cls, bucket_name_env_var_name: str, prefix: str = '') -> 'GcsLoader':
        return map_from_env(cls, {
            'bucket_name': VarMapping(bucket_name_env_var_name, str)
        }, prefix=prefix)

    def read_file(self, file_path: str) -> io.TextIOWrapper:
        blob = self._bucket.blob(self._blob_path(file_path))
        data = blob.download_as_bytes()
        return io.TextIOWrapper(io.BytesIO(data), encoding='utf-8')

    def list_files(self, folder_path: str | None = None, pattern: str = "*", recursive: bool = False) -> list[dict]:
        prefix = self._blob_path(folder_path) + '/' if folder_path else (self._prefix + '/' if self._prefix else '')
        blobs = self._bucket.list_blobs(prefix=prefix)

        files = []
        for blob in blobs:
            name = blob.name.split('/')[-1]
            if not name:
                continue

            # Simple pattern matching
            if pattern != '*':
                if pattern.startswith('*.'):
                    ext = pattern[2:]
                    if not name.endswith(f'.{ext}'):
                        continue
                elif name != pattern:
                    continue

            # Strip the global prefix to get a "relative" path
            rel = blob.name
            if self._prefix and rel.startswith(self._prefix + '/'):
                rel = rel[len(self._prefix) + 1:]
            parent = '/'.join(rel.split('/')[:-1])

            files.append({
                'id': blob.name,
                'name': name,
                'path': rel,
                'parent_path': parent,
            })

        return files

    def resolve_dvc_blob_paths(self, dvc_file_path: str, resource_files: list[str]) -> dict[str, str]:
        """
        Parse a directory .dvc file and resolve each requested file to its GCS
        cache blob path.
        """
        with open(dvc_file_path) as f:
            dvc_data = yaml.safe_load(f)
        dir_hash = dvc_data['outs'][0]['md5'].removesuffix('.dir')

        manifest_blob_path = f"files/md5/{dir_hash[:2]}/{dir_hash[2:]}.dir"
        manifest_text = self._bucket.blob(manifest_blob_path).download_as_text()
        manifest = json.loads(manifest_text)

        hash_by_relpath = {entry['relpath']: entry['md5'] for entry in manifest}

        result = {}
        for filename in resource_files:
            md5 = hash_by_relpath.get(filename)
            if md5 is None:
                raise FileNotFoundError(
                    f"{filename} not found in DVC dir manifest (hash {dir_hash})"
                )
            result[filename] = f"files/md5/{md5[:2]}/{md5[2:]}"

        return result
