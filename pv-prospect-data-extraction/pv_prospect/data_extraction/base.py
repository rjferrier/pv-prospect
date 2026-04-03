from dataclasses import dataclass, field
from datetime import date
from typing import Any, Collection, Protocol

from pv_prospect.common.domain import Entity


@dataclass(frozen=True)
class TimeSeries:
    entity: Entity
    rows: list[list[str]]
    metadata: dict[str, Any] | None = field(default=None)


class TimeSeriesDataExtractor(Protocol):
    def extract(
        self,
        entities: Collection[Any],
        date_: date,
        end_date: date | None = None,
    ) -> list[TimeSeries]: ...
