# pv-prospect-model-trainer

Trains the PV and weather models from a versioned corpus snapshot and either writes
a local artifact store (bootstrap mode) or promotes the result to the instance repo
and the model GCS bucket (promote mode / Cloud Run job mode).

## Contents

- [Producing the models](#producing-the-models)
- [Prerequisites](#prerequisites)
- [Repo-separation invariant](#repo-separation-invariant)
- [Development](#development)

## Producing the models

### 1. Bootstrap (local dev)

Clones `pv-prospect-instance` at a `data-v<date>` tag, pulls the prepared corpus
via DVC, trains both models, and writes a promoted-artifact store to a local
directory:

```bash
python -m pv_prospect.model_trainer bootstrap \
    --data-version 2026-05-31 \
    --output-dir /tmp/pv-prospect-models
```

Output store layout (loaded by `pv-prospect-app` at startup):

```
<output-dir>/
    promoted/
        pv/       ← model.pt, feature_spec.json, training_config.json, eval_report.json
        weather/  ← same 4 files
    current.json  ← metadata pointer { pv: {...}, weather: {...} }
    provenance.json ← lineage record (data tag SHA, metrics) — input to promote
```

### 2. Promote (publish to instance repo and GCS)

Takes a bootstrapped store and publishes it:
- Clones `pv-prospect-instance` at the main branch.
- Copies 4-file artifacts to `models/{pv,weather}/`, runs `dvc add`, pushes
  to `gs://pv-prospect-versioned-model` (DVC content-addressed lineage).
- Commits `models/current.json` + `models/provenance.json`, tags `model-v<date>`.
- If `model_bucket_name` is configured, also copies the serving artifacts to
  `gs://<bucket>/promoted/` (plain path read by the Prediction API at startup).

```bash
python -m pv_prospect.model_trainer promote \
    --store-dir /tmp/pv-prospect-models
```

### 3. Scheduled job (Cloud Run — automated)

The `model-trainer` Cloud Run Job runs both steps automatically, chained off the
weekly data-versioning workflow. It additionally applies a **promotion gate**:
before promoting, it compares the new model's PV clamped-power R² against the
incumbent (read from `gs://<bucket>/current.json`). If the new model degrades by
more than `promotion_tolerance` (default 0.02), it is rejected and the incumbent
keeps serving. Training metrics are emitted to Cloud Monitoring in both branches.

## Prerequisites

### Instance access (never committed to `pv-prospect`)

1. **Instance repo URL** — `instance_repo_url` in `config-default.yaml` (bundled)
   or a `config-local.yaml` override (see below).
2. **SSH deploy key** (for remote URLs) — set `GITHUB_DEPLOY_KEY` env var to the
   private key content. Not required when `instance_repo_url` is a `file:///` path.
3. **GCS credentials** — `gcloud auth application-default login` or a service account
   key; needed by `dvc pull` (`gs://pv-prospect-versioned-feature`) and promote
   (`gs://pv-prospect-versioned-model`).

### Local dev config override

```yaml
# config-local.yaml
instance_repo_url: 'file:///home/you/Projects/pv-prospect-instance'
model_bucket_name: 'pv-prospect-versioned-model'  # set to enable GCS promote
```

```bash
CONFIG_DIR=/path/to/config-dir RUNTIME_ENV=local \
  python -m pv_prospect.model_trainer bootstrap \
    --data-version 2026-05-31 \
    --output-dir /tmp/pv-prospect-models
```

## Repo-separation invariant

`pv-prospect` (this repo, public) is *machinery*; `pv-prospect-instance` (private)
owns the *data and model artifacts*. The bootstrap and promote steps clone the
instance repo to access the corpus and to commit artifacts, but nothing private is
ever committed here. Trained artifacts live under `models/` in the instance repo
(DVC-tracked) and under `promoted/` in the model GCS bucket (serving path).

## Development

```bash
poetry install
poetry run pytest tests/
poetry run ruff check .
poetry run mypy . --ignore-missing-imports --disallow-untyped-defs \
    --explicit-package-bases --namespace-packages
```
