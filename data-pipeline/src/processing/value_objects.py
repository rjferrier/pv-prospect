from dataclasses import dataclass
from enum import Enum

from domain import DateRange
from domain.pv_site import PVSite


@dataclass(frozen=True)
class Task:
    """Represents a data extraction task for a specific site, source, and date range."""
    source_descriptor: str
    date_range: DateRange
    pv_site: PVSite


class ResultType(Enum):
    """Possible outcomes of a task execution."""
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED_EXISTING = "skipped_existing"
    SKIPPED_DRY_RUN = "skipped_dry_run"


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
    def success(cls, task: Task) -> "Result":
        """Create a success result."""
        return cls(task=task, type=ResultType.SUCCESS)

    @classmethod
    def failure(cls, task: Task, error: Exception) -> "Result":
        """Create a failure result."""
        return cls(task=task, type=ResultType.FAILURE, failure_details=FailureDetails(error=error))

    @classmethod
    def skipped_existing(cls, task: Task) -> "Result":
        """Create a skipped (existing file) result."""
        return cls(task=task, type=ResultType.SKIPPED_EXISTING)

    @classmethod
    def skipped_dry_run(cls, task: Task) -> "Result":
        """Create a skipped (dry run) result."""
        return cls(task=task, type=ResultType.SKIPPED_DRY_RUN)
