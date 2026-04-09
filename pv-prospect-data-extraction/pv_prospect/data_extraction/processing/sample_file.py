"""Point-sample file loader for grid-point weather backfill.

Sample files are header-less CSVs containing ``latitude,longitude`` rows,
one per grid point. They live on the resources filesystem under
:data:`SAMPLE_FILES_DIR` as ``sample_NNN.csv`` (zero-padded to three digits).
"""

import csv
import io

from pv_prospect.common.domain import GridPoint, Location
from pv_prospect.etl.storage import FileSystem

SAMPLE_FILES_DIR = 'point_samples'


def sample_file_path(index: int) -> str:
    """Return the resources-relative path for sample file ``index``."""
    return f'{SAMPLE_FILES_DIR}/sample_{index:03d}.csv'


def read_sample_file(fs: FileSystem, path: str) -> list[GridPoint]:
    """Read a sample CSV and return its grid points.

    Blank lines are skipped. Any leading/trailing whitespace on coordinates
    is tolerated.
    """
    text = fs.read_text(path)
    reader = csv.reader(io.StringIO(text))
    grid_points: list[GridPoint] = []
    for row in reader:
        if not row or not row[0].strip():
            continue
        grid_points.append(
            GridPoint(Location.from_coordinates(row[0].strip(), row[1].strip()))
        )
    return grid_points


def count_sample_files(fs: FileSystem) -> int:
    """Count the number of ``sample_*.csv`` files in :data:`SAMPLE_FILES_DIR`."""
    return len(fs.list_files(SAMPLE_FILES_DIR, 'sample_*.csv'))
