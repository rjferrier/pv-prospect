"""Readiness verification for the data versioning pipeline."""

import logging

from pv_prospect.etl.storage import FileSystem

logger = logging.getLogger(__name__)

WEATHER_FILE = 'weather.csv'
PV_PREFIX = 'pv'


class ReadinessError(Exception):
    """Raised when prepared data is not ready for versioning."""


def verify_readiness(
    prepared_fs: FileSystem,
    batches_fs: FileSystem,
) -> list[str]:
    """Verify prepared data is ready for versioning.

    Checks that ``weather.csv`` exists, at least one ``pv/*.csv`` exists,
    and no unassembled batch files remain.

    Returns a list of relative file paths to version
    (e.g. ``['weather.csv', 'pv/89665.csv']``).

    Raises :class:`ReadinessError` if any check fails.
    """
    errors: list[str] = []

    if not prepared_fs.exists(WEATHER_FILE):
        errors.append(f'{WEATHER_FILE} not found in prepared storage')

    pv_files = prepared_fs.list_files(PV_PREFIX, '*.csv')
    if not pv_files:
        errors.append(f'No CSV files found in {PV_PREFIX}/')

    weather_batches = batches_fs.list_files('weather', '*.csv')
    pv_batches = batches_fs.list_files('pv', '*.csv')
    if weather_batches or pv_batches:
        batch_count = len(weather_batches) + len(pv_batches)
        errors.append(
            f'{batch_count} unassembled batch file(s) remain in prepared-batches'
        )

    if errors:
        raise ReadinessError('; '.join(errors))

    paths = [WEATHER_FILE] + [entry.path for entry in pv_files]
    logger.info('Readiness verified: %d file(s) to version', len(paths))
    return paths
