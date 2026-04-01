from datetime import date

from pv_prospect.common.domain import Entity

from .data_source import DataSource


def format_date(date_: date) -> str:
    """Format a date as YYYYMMDD."""
    return '%04d%02d%02d' % (date_.year, date_.month, date_.day)


def identify_time_series(
    data_source: DataSource,
    entity: Entity,
    date_: date,
) -> str:
    return '_'.join(
        (
            str(data_source).replace('/', '-'),
            str(entity.id),
            format_date(date_),
        )
    )


def build_time_series_csv_file_path(
    time_series_folder: str,
    data_source: DataSource,
    entity: Entity,
    date_: date,
) -> str:
    """Build the CSV file path for a time series extraction."""
    filename = identify_time_series(data_source, entity, date_) + '.csv'
    return _build_path(time_series_folder, data_source, entity.id, filename)


def _build_path(*args: object) -> str:
    return '/'.join(str(a) for a in args)
