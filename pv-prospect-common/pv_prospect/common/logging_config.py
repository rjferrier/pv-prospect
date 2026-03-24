"""Application-level logging configuration."""

import logging
import os


def configure_logging() -> None:
    """Configure the root logger from the LOG_LEVEL environment variable.

    Defaults to INFO. Cloud Run captures stdout and routes it to Cloud
    Logging, so a simple stream handler is sufficient.

    Sets the level unconditionally so this is not a no-op when another
    library has already attached a handler to the root logger.
    """
    level = getattr(
        logging,
        os.environ.get('LOG_LEVEL', 'INFO').upper(),
        logging.INFO,
    )
    root = logging.getLogger()
    root.setLevel(level)
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(levelname)s %(name)s: %(message)s'))
        root.addHandler(handler)
