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

    def append_text(self, path: str, content: str) -> None: ...

    def read_bytes(self, path: str) -> bytes: ...

    def write_bytes(self, path: str, content: bytes) -> None: ...

    def mkdir(self, path: str) -> None: ...

    def rmdir(self, path: str) -> None: ...

    def delete(self, path: str) -> None: ...

    def list_files(
        self,
        prefix: str,
        pattern: str = '*',
        recursive: bool = False,
        start_offset: str = '',
    ) -> list[FileEntry]:
        """List entries under *prefix* whose name matches *pattern*.

        *start_offset* bounds the listing from below: only entries whose
        path sorts lexicographically at or after it are returned. On a
        date-partitioned tree this scans from a date rather than from the
        beginning, so the work done scales with the window of interest
        instead of the whole history. It is expressed in the same path
        space as the returned :class:`FileEntry` paths — relative to the
        filesystem root, not to *prefix*.
        """
        ...


@dataclass
class StorageConfig(metaclass=ABCMeta):
    prefix: str = field(default='')
    tracking: 'StorageConfig | None' = field(default=None)

    @classmethod
    @abstractmethod
    def from_dict(
        cls, data: dict, tracking: 'StorageConfig | None' = None
    ) -> 'StorageConfig': ...
