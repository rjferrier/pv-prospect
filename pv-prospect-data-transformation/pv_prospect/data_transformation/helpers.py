import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import ClassVar, Optional, Iterator


class DataSource(Enum):
    OPENMETEO_HISTORICAL = "openmeteo-historical"
    OPENMETEO_QUARTERHOURLY = "openmeteo-quarterhourly"
    PV_OUTPUT = "pvoutput"


@dataclass
class RawDataFileMetadata:
    # Pattern: {data_source}_{pv_site_id}_{YYYYMMDD} where pv_site_id and YYYYMMDD are digits
    FILENAME_PATTERN: ClassVar[re.Pattern] = re.compile(r'^(.+)_(\d+)_(\d{8}).csv$')
    FILE_SUFFIX: ClassVar[str] = '.csv'
    data_source: DataSource
    pv_site_id: int
    date_: date

    @classmethod
    def from_filename(cls, filename: str) -> Optional['RawDataFileMetadata']:
        match = cls.FILENAME_PATTERN.match(filename)
        if not match:
            return None
        data_source = DataSource(match.group(1))
        pv_site_id = int(match.group(2))
        from_date = datetime.strptime(match.group(3), '%Y%m%d').date()
        return cls(data_source, pv_site_id, from_date)

    @property
    def period_in_days(self):
        return 7 if self.data_source is DataSource.OPENMETEO_HISTORICAL else 1

    @property
    def from_date(self):
        return self.date_

    @property
    def to_date(self):
        return self.date_ + timedelta(days=self.period_in_days)

    def get_date_range(self) -> Iterator[date]:
        return (self.date_ + timedelta(days=i) for i in range(self.period_in_days))

    def get_file_name(self) -> Path:
        stem = '_'.join((self.data_source.value, str(self.pv_site_id), date_to_str(self.date_)))
        return stem + self.FILE_SUFFIX

    def replace(
            self,
            data_source: Optional[DataSource] = None,
            pv_site_id: Optional[int] = None,
            date_: Optional[date] = None
    ) -> 'RawDataFileMetadata':
        data_source = data_source or self.data_source
        pv_site_id = pv_site_id or self.pv_site_id
        date_ = date_ or self.date_
        return self.__class__(data_source, pv_site_id, date_)


def date_to_str(date_: date) -> str:
    return "%04d%02d%02d" % (date_.year, date_.month, date_.day)
