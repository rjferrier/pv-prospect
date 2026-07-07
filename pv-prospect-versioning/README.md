# pv-prospect-versioning

Shared git + DVC + SSH operations for versioning a DVC-tracked repo. These are
the building blocks the **data-versioner** uses to snapshot prepared data, and
that the **model-trainer** will reuse to version trained model artefacts:

- `setup_ssh` — write a deploy key and produce a `GIT_SSH_COMMAND` env.
- `clone_instance_repo` — shallow-clone the instance repo over SSH.
- `set_commit_identity`, `git_commit_and_tag`, `git_push` — non-interactive
  commit/tag/push of the generated `.dvc` (and `.gitignore`) files.
- `inject_remote`, `dvc_add_files`, `dvc_push` — DVC add/push with a per-output
  `remote` field so `dvc pull` resolves the remote without an explicit `-r`.
- `dvc_pull` — pull DVC-tracked files from a named remote (used by the model-trainer
  bootstrap to fetch the prepared corpus at a `data-v<date>` tag).

## Contents

- [Why this is a package](#why-this-is-a-package)
- [API](#api)
- [Development](#development)

## Why this is a package

The data-versioner and the (future) model-trainer both need these operations;
keeping a single implementation here — rather than copying them per consumer —
is the `system-design.md` "extract shared code" rule applied.

It is a standalone package (rather than living in `pv-prospect-etl`) so that the
heavy `dvc[gs]` + `gitpython` stack stays out of the packages that install
`pv-prospect-etl` but never version anything (`pv-prospect-data-extraction`,
`pv-prospect-data-transformation`). Only the versioner and trainer depend on it.

## API

```python
from pv_prospect.versioning import (
    setup_ssh, clone_instance_repo, set_commit_identity,
    git_commit_and_tag, git_push,
    inject_remote, dvc_add_files, dvc_pull, dvc_push,
)
```

## Development

```bash
poetry install
poetry run pytest tests/
poetry run ruff check .
poetry run mypy . --ignore-missing-imports --disallow-untyped-defs \
    --explicit-package-bases --namespace-packages
```
