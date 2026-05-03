from dataclasses import dataclass, field
from datetime import date
from typing import Any, Collection, Protocol


@dataclass(frozen=True)
class TimeSeries:
    rows: list[list[str]]
    metadata: dict[str, Any] | None = field(default=None)


class TimeSeriesDataExtractor(Protocol):
    def extract(
        self,
        sites: Collection[Any],
        date_: date,
        end_date: date | None = None,
    ) -> list[TimeSeries]: ...
