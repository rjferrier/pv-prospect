from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum


class Period(Enum):
    DAY = 1
    WEEK = 2


@dataclass(frozen=True)
class DateRange:
    """Represents a date range with start and end (exclusive)."""
    start: date
    end: date

    def __str__(self) -> str:
        return self.start.strftime('%Y-%m-%d') + " to " + self.end.strftime('%Y-%m-%d')

    @classmethod
    def of_single_day(cls, date_: date) -> 'DateRange':
        """Create a DateRange representing a single day.

        Args:
            date_: The date to create a range for

        Returns:
            DateRange with start=date_ and end=date_+1 (exclusive end)
        """
        return cls(date_, date_ + timedelta(days=1))

    def split_by(self, period: Period) -> list['DateRange']:
        match period:
            case Period.DAY:
                return self._to_single_day_ranges()
            case Period.WEEK:
                return self._to_week_ranges()

    def _to_single_day_ranges(self) -> list['DateRange']:
        """Generate individual single-day DateRanges from this date range.

        Returns:
            List of DateRange objects, each representing a single day
        """
        day_count = (self.end - self.start).days
        dates = [self.start + timedelta(days=i) for i in range(day_count)]

        return [DateRange.of_single_day(date_) for date_ in dates]

    def _to_week_ranges(self) -> list['DateRange']:
        """Generate weekly DateRanges from this date range.
        Only processes whole weeks (Monday to Sunday) that fall entirely within the date range.

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
