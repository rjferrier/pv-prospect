from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class DataSource:
    descriptor: str
    extractor_factory: Callable
