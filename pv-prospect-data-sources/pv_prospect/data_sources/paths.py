from datetime import date

from pv_prospect.common.domain import AnyEntity, DateRange, Entity

from .data_source import DataSource
from .resolvers import resolve_subfolder


def format_date(date_: date) -> str:
    """Format a date as YYYYMMDD."""
    return '%04d%02d%02d' % (date_.year, date_.month, date_.day)


def identify_time_series(
    data_source: DataSource,
    entity: Entity,
    date_range: DateRange,
) -> str:
    substrings = [
        str(data_source).replace('/', '-'),
        str(entity.id),
        format_date(date_range.start),
    ]
    if len(date_range) > 1:
        substrings.append(format_date(date_range.end))
    return '_'.join(substrings)


def build_time_series_csv_file_path(
    time_series_folder: str,
    data_source: DataSource,
    entity: AnyEntity,
    date_range: DateRange,
) -> str:
    """Build the CSV file path for a time series extraction."""
    subfolder = resolve_subfolder(data_source.type, entity)
    filename = identify_time_series(data_source, entity, date_range) + '.csv'
    return _build_path(time_series_folder, data_source, subfolder, filename)


def _build_path(*args: object) -> str:
    return '/'.join(str(a) for a in args)
