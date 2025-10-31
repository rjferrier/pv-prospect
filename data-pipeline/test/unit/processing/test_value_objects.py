"""Module-style tests for processing.value_objects."""

from datetime import date

from processing.value_objects import Task, Result, ResultType, FailureDetails
from domain.date_range import DateRange
from extractors.data_sources import SourceDescriptor


def test_task_creation():
    task = Task(
        source_descriptor=SourceDescriptor.PVOUTPUT,
        pv_system_id=12345,
        date_range=DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))
    )

    assert task.source_descriptor == SourceDescriptor.PVOUTPUT
    assert task.pv_system_id == 12345
    assert task.date_range.start == date(2024, 1, 15)


def test_task_str_representation():
    task = Task(
        source_descriptor=SourceDescriptor.OPENMETEO_HOURLY,
        pv_system_id=67890,
        date_range=DateRange(start=date(2024, 1, 15), end=date(2024, 1, 20))
    )

    result = str(task)

    assert "source_descriptor=openmeteo/hourly" in result
    assert "pv_system_id=67890" in result
    assert "date_range=" in result


def test_task_frozen():
    task = Task(
        source_descriptor=SourceDescriptor.PVOUTPUT,
        pv_system_id=12345,
        date_range=DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))
    )

    import pytest
    with pytest.raises(Exception):  # FrozenInstanceError
        task.pv_system_id = 99999


def test_success_factory_method():
    task = Task(
        source_descriptor=SourceDescriptor.PVOUTPUT,
        pv_system_id=12345,
        date_range=DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))
    )

    result = Result.success(task)

    assert result.task == task
    assert result.type == ResultType.SUCCESS
    assert result.failure_details is None


def test_failure_factory_method():
    task = Task(
        source_descriptor=SourceDescriptor.PVOUTPUT,
        pv_system_id=12345,
        date_range=DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))
    )
    error = ValueError("Test error")

    result = Result.failure(task, error)

    assert result.task == task
    assert result.type == ResultType.FAILURE
    assert result.failure_details is not None
    assert result.failure_details.error == error


def test_skipped_existing_factory_method():
    task = Task(
        source_descriptor=SourceDescriptor.PVOUTPUT,
        pv_system_id=12345,
        date_range=DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))
    )

    result = Result.skipped_existing(task)

    assert result.task == task
    assert result.type == ResultType.SKIPPED_EXISTING
    assert result.failure_details is None


def test_skipped_dry_run_factory_method():
    task = Task(
        source_descriptor=SourceDescriptor.PVOUTPUT,
        pv_system_id=12345,
        date_range=DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))
    )

    result = Result.skipped_dry_run(task)

    assert result.task == task
    assert result.type == ResultType.SKIPPED_DRY_RUN
    assert result.failure_details is None


def test_result_frozen():
    task = Task(
        source_descriptor=SourceDescriptor.PVOUTPUT,
        pv_system_id=12345,
        date_range=DateRange(start=date(2024, 1, 15), end=date(2024, 1, 16))
    )
    result = Result.success(task)

    import pytest
    with pytest.raises(Exception):  # FrozenInstanceError
        result.type = ResultType.FAILURE


def test_failure_details_creation_and_frozen():
    error = RuntimeError("Something went wrong")
    details = FailureDetails(error=error)

    assert details.error == error
    assert isinstance(details.error, RuntimeError)

    import pytest
    with pytest.raises(Exception):
        details.error = RuntimeError("new error")
