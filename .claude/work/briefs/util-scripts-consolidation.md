# Consolidate operational scripts into util/

## Why

Several operational/admin scripts currently live inside application packages
rather than the purpose-built `util/` directory:

- `pv-prospect-data-extraction/pv_prospect/data_extraction/util/` — GCS
  admin scripts (`create_dir`, `delete_files`, `find_data_gaps`,
  `remove_duplicate_csvs`, `rename_files`, `upload_file`). Note: `retry.py`
  is a production helper (it has unit tests under `tests/unit/util/retry/`)
  and should stay in the package.
- `pv-prospect-etl/scripts/cleanup_empty_folders.py` — one-off HNS folder
  sweep.

These should be moved into `util/` following the established pattern
(`check-workflow`, `seed-validation-window`): each tool is a directory with a
flat `.py` script, `pyproject.toml`, `poetry.lock`, and `README.md`.

## Grouping heuristic

Avoid a proliferation of venvs by grouping scripts that share the same
dependencies into a single package. The extraction admin scripts likely share
GCS + common deps and can be grouped into one `util/extraction-admin/`
package. The ETL cleanup script can go there too if dependencies align, or
form its own package if diverging.

## What

1. Audit the imports of each script to determine groupings.
2. Create the new util package(s) with the grouped scripts.
3. Delete the originals from their current locations.
4. Update any README or runbook references.
