from dataclasses import dataclass
from typing import Protocol


class TimeSeriesDescriptor(Protocol):
    def __str__(self) -> str: ...


@dataclass(frozen=True)
class TimeSeries:
    descriptor: TimeSeriesDescriptor
    rows: list[list[str]]
