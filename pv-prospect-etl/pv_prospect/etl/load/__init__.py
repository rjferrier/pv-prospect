from .gcs import GcsLoader
from .local import LocalLoader
from .protocol import Loader

__all__ = ['Loader', 'LocalLoader', 'GcsLoader']
