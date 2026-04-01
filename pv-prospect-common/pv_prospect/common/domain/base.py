from typing import Protocol


class Entity(Protocol):
    @property
    def id(self) -> str: ...
