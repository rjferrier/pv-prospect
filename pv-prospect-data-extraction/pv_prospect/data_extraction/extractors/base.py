from typing import Protocol
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TimeSeriesDescriptor(Protocol):
    def __str__(self) -> str:
        ...


@dataclass(frozen=True)
class TimeSeries:
    descriptor: TimeSeriesDescriptor
    rows: list[list[str]]
