from typing import Protocol

from .location import Location


class Site(Protocol):
    def __str__(self) -> str: ...

    @property
    def id(self) -> str: ...

    @property
    def location(self) -> Location: ...
