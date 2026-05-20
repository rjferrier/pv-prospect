"""Application-level logging configuration."""

import logging
import os

# Only these named levels are honoured for ``LOG_LEVEL``. Notably this
# excludes ``NOTSET`` (0): an effective level of 0 makes the root logger
# emit *everything*, including DEBUG, which floods Cloud Logging.
_LEVELS: dict[str, int] = {
    'CRITICAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
}
_DEFAULT_LEVEL = logging.INFO


def resolve_log_level(value: str | None) -> int:
    """Map a ``LOG_LEVEL`` string to a :mod:`logging` level constant.

    Accepts the named levels ``CRITICAL``/``ERROR``/``WARNING``/``INFO``/
    ``DEBUG`` (case-insensitive, surrounding whitespace ignored).
    Anything else -- unset, empty, a stray ``NOTSET``, or a typo --
    resolves to ``INFO``, so a malformed value can never silently drop
    the root logger to a level that floods Cloud Logging with DEBUG
    output.
    """
    name = (value or '').strip().upper()
    return _LEVELS.get(name, _DEFAULT_LEVEL)


def configure_logging() -> None:
    """Configure the root logger from the ``LOG_LEVEL`` environment variable.

    Defaults to INFO. Cloud Run captures the process streams and routes
    them to Cloud Logging, so a single stream handler is sufficient.

    The resolved level is set on the root logger *and* on every root
    handler. The handler cap is the load-bearing part: the root level
    alone is not enough, because a library that lowers the root level
    after we run would let sub-threshold (e.g. DEBUG) records through to
    Cloud Logging. Capping the handler enforces the threshold at the
    point of output regardless of what happens to the root level.
    """
    level = resolve_log_level(os.environ.get('LOG_LEVEL'))
    root = logging.getLogger()
    root.setLevel(level)

    if not root.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(
            logging.Formatter('%(levelname)s %(name)s: %(message)s')
        )
        root.addHandler(stream_handler)

    for handler in root.handlers:
        handler.setLevel(level)
