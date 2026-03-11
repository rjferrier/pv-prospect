from .protocol import Loader
from .local import LocalLoader
from .gcs import GcsLoader

__all__ = [
    'Loader',
    'LocalLoader',
    'GcsLoader'
]