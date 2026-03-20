from typing import Protocol

from .openmeteo import OpenMeteoTimeSeriesDescriptor
from .pvoutput import PVOutputTimeSeriesDescriptor


class TimeSeriesDescriptor(Protocol):
    def __str__(self) -> str: ...


__all__ = [
    'TimeSeriesDescriptor',
    'OpenMeteoTimeSeriesDescriptor',
    'PVOutputTimeSeriesDescriptor',
]
