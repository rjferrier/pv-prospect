"""Tests for run_entrypoint."""

import pytest
from pv_prospect.etl import (
    EXIT_OK,
    EXIT_TASK_FAILED,
    EXIT_WORKFLOW_TERMINATING,
    WorkflowTerminatingError,
    run_entrypoint,
)


def test_exits_zero_when_main_returns_cleanly() -> None:
    def main() -> None:
        return None

    with pytest.raises(SystemExit) as exc_info:
        run_entrypoint(main)

    assert exc_info.value.code == EXIT_OK


def test_exits_one_on_general_exception() -> None:
    def main() -> None:
        raise RuntimeError('boom')

    with pytest.raises(SystemExit) as exc_info:
        run_entrypoint(main)

    assert exc_info.value.code == EXIT_TASK_FAILED


def test_exits_two_on_workflow_terminating_error() -> None:
    def main() -> None:
        raise WorkflowTerminatingError('rate-limited')

    with pytest.raises(SystemExit) as exc_info:
        run_entrypoint(main)

    assert exc_info.value.code == EXIT_WORKFLOW_TERMINATING


def test_logs_unhandled_exception(caplog: pytest.LogCaptureFixture) -> None:
    def main() -> None:
        raise RuntimeError('boom')

    with caplog.at_level('ERROR'), pytest.raises(SystemExit):
        run_entrypoint(main)

    assert 'Unhandled exception' in caplog.text


def test_logs_workflow_terminating_error(caplog: pytest.LogCaptureFixture) -> None:
    def main() -> None:
        raise WorkflowTerminatingError('rate-limited')

    with caplog.at_level('ERROR'), pytest.raises(SystemExit):
        run_entrypoint(main)

    assert 'Workflow-terminating error' in caplog.text


def test_propagates_system_exit_from_main() -> None:
    def main() -> None:
        raise SystemExit(42)

    with pytest.raises(SystemExit) as exc_info:
        run_entrypoint(main)

    assert exc_info.value.code == 42
