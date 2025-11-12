from collections import defaultdict
from collections.abc import KeysView
from copy import copy
from typing import Generic, TypeVar, Optional, Callable, overload, Union

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')
W = TypeVar('W')


class TwoKeyDefaultDict(defaultdict[tuple[T, U], V], Generic[T, U, V]):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._keys: tuple[set[T], set[U]] = set(), set()

    def __getitem__(self, double_key: tuple[T, U]) -> V:
        return super().__getitem__(double_key)

    def __setitem__(self, double_key: tuple[T, U], value: V) -> None:
        super().__setitem__(double_key, value)
        self._keys[0].add(double_key[0])
        self._keys[1].add(double_key[1])

    def __delitem__(self, double_key: tuple[T, U]) -> None:
        super().__delitem__(double_key)
        # Rebuild _keys to ensure integrity after deletion
        self._rebuild_keys()

    @overload
    def keys(self) -> KeysView[tuple[T, U]]: ...

    @overload
    def keys(self, dim: int) -> KeysView[Union[T, U]]: ...

    def keys(self, dim: Optional[int] = None):
        if dim is None:
            return super().keys()
        # Return the set directly as a KeysView-like object
        # Sets support the same interface for iteration and membership
        return self._keys[dim]

    def pop(self, double_key: tuple[T, U], *args) -> V:
        result = super().pop(double_key, *args)
        self._rebuild_keys()
        return result

    def popitem(self) -> tuple[tuple[T, U], V]:
        result = super().popitem()
        self._rebuild_keys()
        return result

    def clear(self) -> None:
        super().clear()
        self._keys = set(), set()

    def update(self, *args, **kwargs) -> None:
        super().update(*args, **kwargs)
        self._rebuild_keys()

    def setdefault(self, double_key: tuple[T, U], default: Optional[V] = None) -> V:
        result = super().setdefault(double_key, default)
        self._keys[0].add(double_key[0])
        self._keys[1].add(double_key[1])
        return result

    def _rebuild_keys(self) -> None:
        """Rebuild the _keys sets from the current dictionary keys."""
        self._keys = set(), set()
        for double_key in self.keys():
            self._keys[0].add(double_key[0])
            self._keys[1].add(double_key[1])

    def map_values(self, mapper: Callable[[V], W]) -> 'TwoKeyDefaultDict[T, U, W]':
        result = copy(self)
        for k, v in self.items():
            result[k] = mapper(v)
        return result
