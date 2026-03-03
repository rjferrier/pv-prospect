"""Cloud Run Job entrypoint.

Reads task parameters from environment variables set by the Cloud Workflow
and calls the corresponding core function.

Environment variables
---------------------
JOB_TYPE
    ``preprocess`` or ``extract_and_load``

For **preprocess**:
    SOURCE_DESCRIPTOR — e.g. ``openmeteo/quarterhourly``

For **extract_and_load**:
    SOURCE_DESCRIPTOR — e.g. ``openmeteo/quarterhourly``
    PV_SYSTEM_ID      — integer system id
    START_DATE        — ISO date ``YYYY-MM-DD``
    END_DATE          — ISO date ``YYYY-MM-DD``
    OVERWRITE         — ``true`` or ``false`` (default ``false``)
    DRY_RUN           — ``true`` or ``false`` (default ``false``)
"""
import os
import sys
from datetime import date

from pv_prospect.common import DateRange
from pv_prospect.data_extraction.extractors import SourceDescriptor
from pv_prospect.data_extraction.processing import core


def _env_bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).lower() in ('true', '1', 'yes')


def main() -> None:
    job_type = os.environ.get('JOB_TYPE', '')

    if job_type == 'preprocess':
        source_descriptor = SourceDescriptor(os.environ['SOURCE_DESCRIPTOR'])

        print(f"[entrypoint] preprocess: {source_descriptor}")
        core.preprocess(
            source_descriptor=source_descriptor,
            local_dir=None,          # Cloud Run always writes to GCS
        )

    elif job_type == 'extract_and_load':
        source_descriptor = SourceDescriptor(os.environ['SOURCE_DESCRIPTOR'])
        pv_system_id = int(os.environ['PV_SYSTEM_ID'])
        start_date = date.fromisoformat(os.environ['START_DATE'])
        end_date = date.fromisoformat(os.environ['END_DATE'])
        overwrite = _env_bool('OVERWRITE')
        dry_run = _env_bool('DRY_RUN')

        date_range = DateRange(start_date, end_date)
        print(f"[entrypoint] extract_and_load: {source_descriptor}, "
              f"site={pv_system_id}, {date_range}")

        result = core.extract_and_load(
            source_descriptor=source_descriptor,
            pv_system_id=pv_system_id,
            date_range=date_range,
            local_dir=None,
            overwrite=overwrite,
            dry_run=dry_run,
        )
        print(f"[entrypoint] result: {result.type.value}")

    else:
        print(f"[entrypoint] ERROR: unknown JOB_TYPE={job_type!r}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
