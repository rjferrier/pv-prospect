"""Cloud Run Job entrypoint helpers.

Defines the workflow exit-code policy and a sentinel exception
application code can raise to signal that the surrounding Cloud
Workflow should abort rather than continue with other tasks.

Exit codes
----------
0
    ``main()`` returned without exception.
1
    ``main()`` raised a general exception. The workflow records the
    task as failed and continues with the next task in the manifest.
2
    ``main()`` raised :class:`WorkflowTerminatingError`. The workflow
    records the task as failed and aborts (skipping remaining tasks
    and any cursor-commit step).
"""

import logging
import sys
from typing import Callable, NoReturn

EXIT_OK = 0
EXIT_TASK_FAILED = 1
EXIT_WORKFLOW_TERMINATING = 2

logger = logging.getLogger(__name__)


class WorkflowTerminatingError(Exception):
    """Signal that the surrounding workflow should abort.

    Raise from application code for failure modes where continuing
    other tasks in the same workflow run is harmful or pointless --
    for example, sustained rate-limiting from an upstream API. Any
    other failure should be a regular exception (task fails, workflow
    continues to the next task).
    """


def run_entrypoint(main: Callable[[], None]) -> NoReturn:
    """Invoke *main* and exit the process per the workflow exit-code policy.

    Always terminates the process via :func:`sys.exit`.
    """
    try:
        main()
    except WorkflowTerminatingError:
        logger.exception('Workflow-terminating error')
        sys.exit(EXIT_WORKFLOW_TERMINATING)
    except Exception:
        logger.exception('Unhandled exception')
        sys.exit(EXIT_TASK_FAILED)
    sys.exit(EXIT_OK)
