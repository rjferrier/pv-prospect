from dataclasses import dataclass
from datetime import date
from typing import Any, Collection, Protocol

from pv_prospect.common import PVSite
from pv_prospect.data_sources import TimeSeriesDescriptor


@dataclass(frozen=True)
class TimeSeries:
    descriptor: TimeSeriesDescriptor
    rows: list[list[str]]


class TimeSeriesDataExtractor(Protocol):
    def get_time_series_descriptors(
        self, pv_site: PVSite
    ) -> list[TimeSeriesDescriptor]: ...

    def extract(
        self,
        time_series_descriptors: Collection[Any],
        date_: date,
        end_date: date | None = None,
    ) -> list[TimeSeries]: ...
