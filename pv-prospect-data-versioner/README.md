# Data Versioning

Data versioning pipeline for PV Prospect. Snapshots prepared CSV data from the
staging bucket to a DVC-tracked versioned feature store on a weekly cadence,
producing an immutable reference each training run can pin against.

## Contents

- [Where data comes from and where it goes](#where-data-comes-from-and-where-it-goes)
- [Pipeline](#pipeline)
- [Configuration](#configuration)
- [Cloud Run Job entrypoint](#cloud-run-job-entrypoint)
- [Staging cleanup on hierarchical-namespace (HNS) buckets](#staging-cleanup-on-hierarchical-namespace-hns-buckets)
- [Coordinated redeploy with extraction / transformation](#coordinated-redeploy-with-extraction--transformation)

## Where data comes from and where it goes

The versioner sits downstream of the daily extraction and transformation
pipelines and upstream of model training:

- **Input** — the `prepared/` prefix on the staging bucket, populated by the
  transformation pipeline's assembly step. Two partitioned, content-named
  corpora:
  - `weather/weather_{start}_{end}_{gv}-{NN}.csv` — grid-point weather.
  - `pv/<system_id>/pv_<system_id>_{start}_{end}.csv` — PV power joined with
    on-site weather, one subtree per PV system.
- **Output** — a commit + annotated tag on the
  [`pv-prospect-instance`](https://github.com/rjferrier/pv-prospect-instance)
  repository, plus the underlying CSV bytes pushed to the `feature` DVC remote
  (the `pv-prospect-versioned-feature` GCS bucket). The tag is named
  `data-v<YYYY-MM-DD>` so downstream consumers can `dvc checkout` a specific
  weekly snapshot by tag.

The instance repo holds only `.dvc` pointer files (and the `.gitignore` entries
DVC generates next to them); the actual CSV payloads live on the DVC remote.

## Pipeline

`version_data` in `core.py` runs seven steps in order. The whole sequence
happens inside a single `TemporaryDirectory`, so a failed run leaves no
half-written clone behind.

### 1. Readiness verification

`verify_readiness` (in `readiness.py`) is the gate: it refuses to version if
the prepared data isn't in the shape model training expects.

- Every CSV under `weather/` and `pv/` must match its content-named
  partition shape (see the input layout above) — a stray or old-format file
  blocks versioning rather than being snapshotted silently.
- At least one partition file must be present to version. The versioner is a
  pure snapshotter, so it does not require any particular corpus to be
  populated — a cycle that produced only PV partitions is still ready.
- `prepared-batches/` must be empty — any leftover batch CSV indicates the
  transformation pipeline's assembly step did not complete, and versioning a
  partial state would baseline the next training run against incomplete data.

All failing conditions accumulate into a single `ReadinessError` so an
operator sees every problem in one pass, not just the first.

### 2. SSH setup

`setup_ssh` writes the deploy key (injected from Secret Manager into the
Cloud Run Job via `GITHUB_DEPLOY_KEY`) to a `0400` file inside the temp dir
and returns a `GIT_SSH_COMMAND` env var that pins git to that key with
`StrictHostKeyChecking=no`. The key never touches a path that outlives the run.

### 3. Clone the instance repo

`clone_instance_repo` performs a shallow (`depth=1`) clone of the configured
branch. Shallow is fine because the versioner only ever appends a new commit
on top of `main`; it never needs prior history.

### 4. Download prepared CSVs into the clone

Each file listed by `verify_readiness` is read from the staging bucket and
written to `<clone>/<prepared_data_dir>/<rel_path>` (default
`<clone>/data/prepared/<rel_path>`), creating intermediate directories as
needed.

### 5. `dvc add` and `dvc push`

`dvc_add_files` (in `dvc_ops.py`) runs `dvc add` against each downloaded CSV,
producing a sibling `.dvc` pointer file. Each pointer is then rewritten via
`inject_remote` to embed `remote: feature`, so downstream consumers can
`dvc pull` without passing `-r feature` explicitly (see commit `ab3fd32`).

`dvc_push` uploads the underlying CSV bytes to the named remote.

### 6. Git commit, tag, push

`git_commit_and_tag` stages the `.dvc` files plus any `.gitignore` files DVC
generated alongside them (DVC writes `.gitignore` entries so the real CSVs
aren't accidentally committed to git), commits with the message
`Version prepared data <YYYY-MM-DD>`, and creates an annotated tag
`data-v<YYYY-MM-DD>`. `git_push` then pushes the branch and tags to origin
under the deploy-key SSH env.

`set_commit_identity` configures `user.name` / `user.email` on the local repo
before commit, because Cloud Run Jobs have no global git identity. Defaults
are `pv-prospect data-versioner` and `data-versioner@pv-prospect.invalid`,
overridable via config.

### 7. Staging cleanup

`_clean_staging` deletes every file under the `cleaned/` and `prepared/`
prefixes on the staging bucket and then `rmdir`s the residual folder
hierarchy. See [HNS cleanup](#staging-cleanup-on-hierarchical-namespace-hns-buckets)
below for why the explicit rmdir is necessary.

The next weekly run therefore begins against an empty staging tree, so each
cycle versions exactly that week's freshly assembled partition files — the
clean slate is what lets content-named partitions accumulate as disjoint
`.dvc` entries across tags rather than overwriting one another.

## Configuration

`DataVersionerConfig` (in `config.py`) is loaded by `get_config` with two
config dirs merged in order: `pv-prospect-etl/.../resources` (which supplies
the bucket-prefix storage configs) then `pv-prospect-data-versioner/.../resources`
(which supplies versioner-specific keys).

Storage backends — bucket name and prefix come from
`pv-prospect-etl/config-default.yaml`:

- `staged_prepared_data_storage`
- `staged_cleaned_data_storage`
- `staged_prepared_batches_data_storage`

Versioner-specific keys (defaults):

- `instance_repo_url` — `git@github.com:rjferrier/pv-prospect-instance.git`
- `instance_repo_branch` — `main`
- `dvc_remote_name` — `feature`
- `prepared_data_dir` — `data/prepared`
- `commit_author_name` — `pv-prospect data-versioner`
- `commit_author_email` — `data-versioner@pv-prospect.invalid`

`config-local.yaml` overrides `instance_repo_url` to `../../pv-prospect-instance`
so a local run targets a sibling checkout instead of GitHub.

## Cloud Run Job entrypoint

`entrypoint.py` is the container's `CMD`. It reads two env vars:

- `VERSION_DATE` (optional) — ISO date string. Defaults to today (UTC) if
  unset. Used for both the commit message and the tag name.
- `GITHUB_DEPLOY_KEY` (required) — SSH private key for pushing to the
  instance repo. Injected from Secret Manager by the Cloud Run Job
  definition. The entrypoint refuses to start if it is empty.

The wrapping Cloud Workflow
(`terraform/modules/version/workflow/main.tf`) is a single-step workflow that
runs the Cloud Run Job with `VERSION_DATE` defaulted to today's date in the
workflow trigger context. Cloud Scheduler triggers it weekly.

## Staging cleanup on hierarchical-namespace (HNS) buckets

After uploading prepared CSVs to the versioned feature store, `version_data`
clears the `cleaned/` and `prepared/` prefixes from the staging bucket so the
next weekly run can verify readiness against a fresh write.

On the hierarchical-namespace staging bucket, folders are first-class entities
that persist after their last child object is removed. Cleanup therefore deletes
files and then `rmdir`s every residual ancestor directory deepest-first (e.g.
`weather/` and each `pv/<site>/`, plus any subtree under `data/cleaned/`).
The root prefixes themselves
(`data/prepared/`, `data/cleaned/`) are left in place; transformation re-uses
them on its next run.

`iter_ancestor_dirs` in `core.py` is the pure helper that derives the
deepest-first directory list from a set of file paths; it is unit-tested in
isolation so the rmdir ordering does not need a live bucket to validate.

## Coordinated redeploy with extraction / transformation

The versioner reads its bucket prefixes from `pv-prospect-etl`'s
`config-default.yaml`. Whenever a change reshapes the staging layout —
e.g. commit `46ac7c9` moved `staging/{raw,cleaned,prepared,prepared-batches}`
to `data/{...}` — the versioner image must be rebuilt and redeployed alongside
the extraction and transformation images. A versioner image built against an
old layout will look for prepared data at a prefix the new extract/transform
images no longer write to, and the readiness check will fail.
