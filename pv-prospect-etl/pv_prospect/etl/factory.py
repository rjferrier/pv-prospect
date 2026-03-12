from pv_prospect.etl.storage_config import (
    AnyStorageConfig,
    GcsStorageConfig,
    LocalStorageConfig,
)

from .extract import Extractor, GcsExtractor, LocalExtractor
from .load import GcsLoader, Loader, LocalLoader


def get_loader(config: AnyStorageConfig) -> Loader:
    if isinstance(config, LocalStorageConfig):
        return LocalLoader(config.prefix)

    if isinstance(config, GcsStorageConfig):
        return GcsLoader(config.bucket_name, config.prefix)

    raise _not_implemented(config)


def get_extractor(config: AnyStorageConfig) -> Extractor:
    if isinstance(config, LocalStorageConfig):
        return LocalExtractor(config.prefix)

    if isinstance(config, GcsStorageConfig):
        return GcsExtractor(config.bucket_name, config.prefix)

    raise _not_implemented(config)


def _not_implemented(discriminator: object) -> NotImplementedError:
    return NotImplementedError(
        f'type of {discriminator} ({type(discriminator)}) is not recognised'
    )
