from datetime import date, timedelta
from enum import Enum
from typing import NamedTuple, Callable


class Period(Enum):
    DAY = 1
    WEEK = 2


class DateRange(NamedTuple):
    """Represents a date range with start, end, and description."""
    start: date
    end: date | None = None

    def __str__(self) -> str:
        str_ = self.start.strftime('%Y-%m-%d')
        if self.end:
            str_ += " to " + self.end.strftime('%Y-%m-%d')
        return str_

    def split_by(self, period: Period, reverse: bool = False) -> list['DateRange']:
        method = self._get_splitting_method(period)
        result = method()

        if reverse:
            result.reverse()

        return result

    def _get_splitting_method(self, period: Period) -> Callable[[], list['DateRange']]:
        match period:
            case Period.DAY:
                return self._to_single_day_ranges
            case Period.WEEK:
                return self._to_week_ranges

    def _to_single_day_ranges(self) -> list['DateRange']:
        """Generate individual single-day DateRanges from this date range.

        Args:
            reverse: If True, reverse the order of dates

        Returns:
            List of DateRange objects, each representing a single day
        """
        day_count = (self.end - self.start).days
        dates = [self.start + timedelta(days=i) for i in range(day_count)]

        return [DateRange(date_) for date_ in dates]

    def _to_week_ranges(self) -> list['DateRange']:
        """Generate weekly DateRanges from this date range.
        Only processes whole weeks (Monday to Sunday) that fall entirely within the date range.

        Args:
            reverse: If True, reverse the order of weeks

        Returns:
            List of DateRange objects, each representing a week (Monday to Sunday)
        """
        # Find the first Monday on or after start_date
        start_offset = self.start.weekday()  # 0=Monday, 6=Sunday
        if start_offset == 0:
            # Already a Monday
            first_monday = self.start
        else:
            # Move to next Monday
            first_monday = self.start + timedelta(days=(7 - start_offset))

        # Find the last Sunday on or before end_date
        end_offset = self.end.weekday()  # 0=Monday, 6=Sunday
        if end_offset == 6:
            # Already a Sunday
            last_sunday = self.end
        else:
            # Move back to previous Sunday
            last_sunday = self.end - timedelta(days=(end_offset + 1))

        # Generate week ranges
        current = first_monday
        week_ranges = []

        while current <= last_sunday:
            # Each week runs from Monday (current) to Sunday (current + 6 days)
            week_end_date = current + timedelta(days=6)

            # Only include if the full week (ending on Sunday) fits within range
            if week_end_date <= last_sunday:
                week_ranges.append(DateRange(current, week_end_date + timedelta(days=1)))  # +1 to make end exclusive

            # Move to next Monday
            current = current + timedelta(days=7)

        return week_ranges
