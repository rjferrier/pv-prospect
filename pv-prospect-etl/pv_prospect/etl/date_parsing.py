"""Date string parsing, DateRange construction, and env-var helpers.

Shared by CLI runners and Cloud Run entrypoints across sub-projects.
"""

from datetime import date, timedelta

from pv_prospect.common.domain import DateRange

_MONTH_FORMAT_LEN = 7  # 'YYYY-MM'


class DegenerateDateRange(ValueError):
    def __init__(self, start: date, end: date) -> None:
        self.start = start
        self.end = end
        super().__init__(f'Empty date range: {start} to {end}')


def parse_date(date_str: str) -> date:
    """Parse a date string.

    Accepts ``'today'``, ``'yesterday'``, ``YYYY-MM-DD``, or ``YYYY-MM``
    (interpreted as the first day of the month).
    """
    lower = date_str.lower()
    if lower == 'today':
        return date.today()
    if lower == 'yesterday':
        return date.today() - timedelta(days=1)
    if _is_month_format(date_str):
        year, month = date_str.split('-')
        return date(int(year), int(month), 1)
    return date.fromisoformat(date_str)


def build_date_range(
    start: str | None = None,
    end: str | None = None,
) -> DateRange:
    """Build a :class:`DateRange` from optional start/end date strings.

    *start* defaults to yesterday.  *end* is exclusive and defaults to the
    day after *start* (or the first of the next month when *start* is in
    ``YYYY-MM`` format).  When *end* is given in ``YYYY-MM`` format it is
    interpreted as the first of the following month.
    """
    if start is None:
        start_date = date.today() - timedelta(days=1)
        start_is_month = False
    else:
        start_is_month = _is_month_format(start)
        start_date = parse_date(start)

    if end is None:
        if start_is_month:
            end_date = _first_of_next_month(start_date)
        else:
            end_date = start_date + timedelta(days=1)
    else:
        if _is_month_format(end):
            end_date = _first_of_next_month(parse_date(end))
        else:
            end_date = parse_date(end)

    if start_date >= end_date:
        raise DegenerateDateRange(start_date, end_date)

    return DateRange(start_date, end_date)


def _is_month_format(date_str: str) -> bool:
    return (
        len(date_str) == _MONTH_FORMAT_LEN
        and date_str[4] == '-'
        and date_str.count('-') == 1
    )


def _first_of_next_month(d: date) -> date:
    next_m = d.month % 12 + 1
    y = d.year + (d.month // 12)
    return date(y, next_m, 1)
