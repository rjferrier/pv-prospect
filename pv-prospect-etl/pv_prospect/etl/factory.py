from pv_prospect.etl.storage_config import (
    AnyStorageConfig, LocalStorageConfig, GcsStorageConfig
)

from .extract import (
    Extractor, LocalExtractor, GcsExtractor
)
from .load import (
    Loader, LocalLoader, GcsLoader
)


def get_loader(config: AnyStorageConfig) -> Loader:
    if isinstance(config, LocalStorageConfig):
        return LocalLoader(config.base_dir)

    if isinstance(config, GcsStorageConfig):
        return GcsLoader(config.bucket_name, config.prefix)

    raise _not_implemented(config)


def get_extractor(config: AnyStorageConfig) -> Extractor:
    if isinstance(config, LocalStorageConfig):
        return LocalExtractor(config.base_dir)

    if isinstance(config, GcsStorageConfig):
        return GcsExtractor(config.bucket_name, config.prefix)

    raise _not_implemented(config)


def _not_implemented(discriminator: object):
    return NotImplementedError(f"type of {discriminator} ({type(discriminator)}) is not recognised")