# pv-prospect-model-trainer

Trains the PV and weather models from a versioned corpus snapshot and writes
the promoted-artifact store that the Prediction API loads at start-up.

## Two run modes

### Bootstrap (local dev, Phase 2)

Clones `pv-prospect-instance` at a `data-v<date>` tag, pulls the prepared
corpus via DVC, trains both models, and writes the promoted-artifact store
to a local directory:

```bash
python -m pv_prospect.model_trainer bootstrap \
    --data-version 2026-05-31 \
    --output-dir /path/to/models
```

Output store layout (read by the Prediction API):

```
<output-dir>/
    promoted/
        pv/       ← model.pt, feature_spec.json, training_config.json, eval_report.json
        weather/  ← same 4 files
    current.json  ← metadata pointer { pv: {...}, weather: {...} }
```

### Scheduled job (Cloud Run, Phase 3+)

The same package will grow a scheduled-job mode (no separate home needed)
that chains off the weekly versioning workflow, adds the promotion gate, and
pushes metrics to Cloud Monitoring.

## Prerequisites

### Instance access (never committed here)

The bootstrap needs read access to `pv-prospect-instance`:

1. **Instance repo URL** — `instance_repo_url` in your `config-local.yaml`
   (or set `RUNTIME_ENV=default` to use the bundled `config-default.yaml`
   which points to the SSH URL).
2. **SSH deploy key** (for remote URLs) — set `GITHUB_DEPLOY_KEY` env var to
   the private key content. Not required when `instance_repo_url` is a
   `file:///` local path.
3. **GCS credentials** — `gcloud auth application-default login` or a service
   account key; needed by `dvc pull` to fetch prepared data from
   `gs://pv-prospect-versioned-feature`.

### Local dev config

Create a `config-local.yaml` in a directory of your choice and point to it
with `CONFIG_DIR`:

```yaml
# config-local.yaml
instance_repo_url: 'file:///home/you/Projects/pv-prospect-instance'
```

```bash
CONFIG_DIR=/path/to/config-dir RUNTIME_ENV=local \
  python -m pv_prospect.model_trainer bootstrap \
    --data-version 2026-05-31 \
    --output-dir /path/to/models
```

## Repo-separation invariant

`pv-prospect` (this repo, public) is *machinery*; `pv-prospect-instance`
(private) owns the *data and model artifacts*. The bootstrap clones the
instance repo to access the corpus, but nothing private is ever committed
here. Trained artifacts live under `models/` in the instance repo (or in
`--output-dir` for local dev).

## Development

```bash
poetry install
poetry run pytest tests/
poetry run ruff check .
poetry run mypy . --ignore-missing-imports --disallow-untyped-defs \
    --explicit-package-bases --namespace-packages
```
