"""Readiness verification for the data versioning pipeline."""

import logging
import re

from pv_prospect.etl.storage import FileSystem

logger = logging.getLogger(__name__)

WEATHER_PREFIX = 'weather'
PV_PREFIX = 'pv'

# Partition-file path shapes, mirroring what the data-transformation
# ``assemble`` step writes (see that package's ``core.py`` and the
# top-level README's prepared-data layout):
#   weather/weather_{start}_{end}_{gv}-{NN}.csv
#   pv/{site}/pv_{site}_{start}_{end}.csv
# ``{start}`` / ``{end}`` are ISO ``YYYY-MM-DD``; ``{gv}`` the
# grid-definition version; ``{NN}`` the zero-padded grid-point sample
# index; ``{site}`` a PV system id. The versioner does not depend on the
# transformation package, so the shape is pinned here as a regex; the
# backreference in the PV pattern requires the directory's site to match
# the filename's.
_DATE = r'\d{4}-\d{2}-\d{2}'
_WEATHER_PARTITION = re.compile(
    rf'{WEATHER_PREFIX}/weather_{_DATE}_{_DATE}_\d+-\d{{2}}\.csv'
)
_PV_PARTITION = re.compile(rf'{PV_PREFIX}/(\d+)/pv_\1_{_DATE}_{_DATE}\.csv')


class ReadinessError(Exception):
    """Raised when prepared data is not ready for versioning."""


def _collect_partition_files(
    prepared_fs: FileSystem, prefix: str, pattern: re.Pattern[str]
) -> tuple[list[str], list[str]]:
    """List the CSVs under *prefix* and split them into (valid, errors).

    A file whose path does not match *pattern* — a stray file, or an
    old-format master left from before the partitioned layout — is
    reported as an error so it blocks versioning rather than being
    snapshotted silently.
    """
    valid: list[str] = []
    errors: list[str] = []
    for entry in prepared_fs.list_files(prefix, '*.csv', recursive=True):
        if pattern.fullmatch(entry.path):
            valid.append(entry.path)
        else:
            errors.append(f'Unexpected file in {prefix}/: {entry.path}')
    return valid, errors


def verify_readiness(
    prepared_fs: FileSystem,
    batches_fs: FileSystem,
) -> list[str]:
    """Verify prepared data is ready for versioning.

    Walks the ``weather/`` and ``pv/`` partition-file trees, checks every
    file matches the expected content-named shape, confirms at least one
    file is present to version, and confirms no unassembled batch files
    remain. The versioner is a pure snapshotter, so it versions whatever
    partition files are present — it does not require any particular
    corpus to be populated, only that the files it finds are well-formed.

    Returns the sorted list of relative partition-file paths to version
    (e.g. ``['pv/89665/pv_89665_2026-05-01_2026-05-08.csv',
    'weather/weather_2026-05-01_2026-05-15_0-07.csv']``).

    All failing conditions accumulate into a single :class:`ReadinessError`
    so an operator sees every problem in one pass.
    """
    errors: list[str] = []

    weather_paths, weather_errors = _collect_partition_files(
        prepared_fs, WEATHER_PREFIX, _WEATHER_PARTITION
    )
    pv_paths, pv_errors = _collect_partition_files(
        prepared_fs, PV_PREFIX, _PV_PARTITION
    )
    paths = weather_paths + pv_paths
    errors.extend(weather_errors)
    errors.extend(pv_errors)

    if not paths and not errors:
        errors.append('No prepared partition files found to version')

    weather_batches = batches_fs.list_files('weather', '*.csv')
    pv_batches = batches_fs.list_files('pv', '*.csv')
    if weather_batches or pv_batches:
        batch_count = len(weather_batches) + len(pv_batches)
        errors.append(
            f'{batch_count} unassembled batch file(s) remain in prepared-batches'
        )

    if errors:
        raise ReadinessError('; '.join(errors))

    paths.sort()
    logger.info('Readiness verified: %d file(s) to version', len(paths))
    return paths
