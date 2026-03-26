from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class FileEntry:
    id: str
    name: str
    path: str
    parent_path: str
    created_time: float | None = None


class FileSystem(Protocol):
    def __str__(self) -> str: ...

    def exists(self, path: str) -> bool: ...

    def read_text(self, path: str) -> str: ...

    def write_text(self, path: str, content: str) -> None: ...

    def read_bytes(self, path: str) -> bytes: ...

    def write_bytes(self, path: str, content: bytes) -> None: ...

    def mkdir(self, path: str) -> None: ...

    def delete(self, path: str) -> None: ...

    def list_files(
        self, prefix: str, pattern: str = '*', recursive: bool = False
    ) -> list[FileEntry]: ...


@dataclass
class StorageConfig(metaclass=ABCMeta):
    prefix: str = field(default='')
    tracking: 'StorageConfig | None' = field(default=None)

    @classmethod
    @abstractmethod
    def from_dict(
        cls, data: dict, tracking: 'StorageConfig | None' = None
    ) -> 'StorageConfig': ...
