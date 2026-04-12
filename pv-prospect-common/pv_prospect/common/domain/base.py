from typing import Protocol

from .location import Location


class Site(Protocol):
    @property
    def id(self) -> str: ...

    @property
    def location(self) -> Location: ...
