from dataclasses import dataclass
from enum import Enum

from pv_prospect.common.domain import DateRange, Site
from pv_prospect.data_extraction import DataSource


@dataclass(frozen=True)
class Task:
    """Represents a data extraction task for a specific locatable, source, and date range."""

    data_source: DataSource
    site: Site
    date_range: DateRange

    def __str__(self) -> str:
        return (
            'Task('
            f'data_source={self.data_source}, '
            f'site={self.site}, '
            f'date_range={self.date_range}'
            ')'
        )


class ResultType(Enum):
    """Possible outcomes of a task execution."""

    SUCCESS = 'success'
    FAILURE = 'failure'
    SKIPPED_DRY_RUN = 'skipped_dry_run'


@dataclass(frozen=True)
class FailureDetails:
    """Details about a task failure."""

    error: Exception


@dataclass(frozen=True)
class Result:
    """Result of executing a task."""

    task: Task
    type: ResultType
    failure_details: FailureDetails | None = None

    @classmethod
    def success(cls, task: Task) -> 'Result':
        """Create a success result."""
        return cls(task=task, type=ResultType.SUCCESS)

    @classmethod
    def failure(cls, task: Task, error: Exception) -> 'Result':
        """Create a failure result."""
        return cls(
            task=task,
            type=ResultType.FAILURE,
            failure_details=FailureDetails(error=error),
        )

    @classmethod
    def skipped_dry_run(cls, task: Task) -> 'Result':
        """Create a skipped (dry run) result."""
        return cls(task=task, type=ResultType.SKIPPED_DRY_RUN)
