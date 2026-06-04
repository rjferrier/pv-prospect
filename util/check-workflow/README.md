# check-workflow

Compare manifests vs ledgers for all workflows on a given run date.

## Setup

```bash
poetry install
```

## Usage

```bash
poetry run check-workflow [<date>]
```

`<date>` is `YYYY-MM-DD` (default: today UTC). Requires `gsutil` on `PATH`
and GCS credentials (e.g. `gcloud auth application-default login`).
