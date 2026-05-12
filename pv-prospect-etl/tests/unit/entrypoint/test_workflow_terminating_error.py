"""Tests for WorkflowTerminatingError."""

import pytest
from pv_prospect.etl import WorkflowTerminatingError


def test_is_an_exception_subclass() -> None:
    assert issubclass(WorkflowTerminatingError, Exception)


def test_can_be_raised_and_caught() -> None:
    with pytest.raises(WorkflowTerminatingError, match='rate-limited'):
        raise WorkflowTerminatingError('rate-limited')


def test_preserves_message() -> None:
    err = WorkflowTerminatingError('upstream API exhausted')

    assert str(err) == 'upstream API exhausted'
