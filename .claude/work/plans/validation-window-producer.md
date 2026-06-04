# Plan — Validation window producer

Detailed design for [briefs/validation-window-producer.md](../briefs/validation-window-producer.md),
the first task of the Validation API roadmap. This task produces the durable serving
artifact; tasks 2–4 (store, endpoint, wiring) consume it.

## 1. Goal & context

The Validation API needs, per known PV site, the last **90 days** of prepared PV
rows — the PV model's inputs (`temperature`, `plane_of_array_irradiance`) plus the
real `power` to compare against. The app loads this wholesale into memory at startup
(the `InMemoryData` node) and refreshes it lazily.

Two constraints fix the shape of the solution:

- **`staging/prepared` is ephemeral.** The data-versioner's `_clean_staging` wipes
  `cleaned/` + `prepared/` every weekly run, so that prefix only holds the current
  week's increment. A stable 90-day window cannot live there.
- **The historical tail lives in DVC.** The transformer only ever sees the forward
  increment; the older ~83 days of a 90-day window are in the versioned feature store,
  which neither the app nor the daily transformer reads.

So the artifact is a **durable, regenerable serving cache** at a GCS served prefix,
**seeded once** from history and **maintained forward** daily by the transformer.
Settled decisions: GCS object (not a DB, not loose CSVs); seed once from the feature
store; window = 90 days; producer = transformer (not versioner/trainer — only the
transformer runs daily and owns prepared data).

## 2. Artifact format (the contract for tasks 2–4)

Location: `gs://<bucket_prefix>-staging/data/served/validation-window/` — the staging
bucket, which the pipeline SA already owns (`objectAdmin`) and which the weekly clean
never touches (it clears only the `cleaned/` + `prepared/` prefixes, each through its
own scoped filesystem; `data/served/` is a separate prefix).

```
data/served/validation-window/
    window.csv       # all sites, last 90 days
    manifest.json    # bounds + freshness signal
```

`window.csv` columns = `['system_id'] + PV_COLUMNS`, i.e.

| Column | Source |
|---|---|
| `system_id` | `pv_partition_path` parse — added on assembly (PV_COLUMNS omits it) |
| `time` | prepared PV |
| `temperature` | prepared PV |
| `plane_of_array_irradiance` | prepared PV |
| `power` | prepared PV — actual output |
| `power_max` | prepared PV — clip flag for the display |

`manifest.json`:

```json
{
  "updated_at": "2026-06-04T23:14:00Z",
  "window_days": 90,
  "window_start": "2026-03-06",
  "window_end": "2026-06-04",
  "row_counts": {"89665": 2160, "12345": 2088, ...}
}
```

Static per-site attributes (capacities, install date) are **not** duplicated — the
app reads them from `pv_sites.csv` (already synced to `staging/resources/`). Freshness
check (task 2) is one metadata GET on `window.csv`'s `generation`; `manifest.json` is
supplementary (drives `/validate/sites` and debugging). CSV chosen for parity with the
prepared corpus and zero new deps; Parquet is a later optimisation, not now.

## 3. Component A — daily maintenance step (transformer)

**Hook.** `entrypoint.main()` dispatches on `job_type` (`plan_transform`,
`run_transform_backfill`, `consolidate_logs` = the daily end-of-run step). Add a new
`job_type == 'maintain_validation_window'`, parallel to `consolidate_logs`, invoked by
the transform workflow as a final step **after a barrier on the entire prepare phase**
— all fan-out tasks complete, not just some, or the step can capture and persist a
*partial* day (it reads *all* current prepared partitions). Self-healing recovers a
partial only on the next run, and only if the keep-policy above is correct and staging
still holds the day — cheaper to enforce the barrier. Keep it separate from
`consolidate_logs` so a window failure doesn't fail log consolidation and vice-versa.

**Algorithm** (pure core, in a new `processing/validation_window.py`):

```
prev = read_window(window_fs)                 # existing artifact (seeded)
incr = read_prepared_pv(prepared_fs)          # all current staging/prepared/pv/*,
                                              #   system_id added from the path
merged = merge_prepared_frames([prev, incr],  # ORDER IS LOAD-BEARING (see below)
                               keys=['system_id', 'time'])
window = trim_window(merged, days=90)          # keep time >= max(time) - 90d
write_window(window_fs, window)                # window.csv + manifest.json
```

- **Idempotent / self-healing.** Re-running re-merges the same rows (dedup absorbs
  them); a skipped day is recovered on the next run as long as staging still holds it
  (≤ 1 week, until the weekly clean). Reading *all* current `prepared/pv` each run
  (rather than just this run's output) is what buys the self-healing; the read is
  bounded by one week of staging, so it's cheap.
- 
- **Collision keep-policy (load-bearing).** `merge_prepared_frames` de-duplicates with
  `drop_duplicates(keep='last')` — "freshly prepared rows win over the existing file"
  (its docstring). A day extracted *partial* on Monday and re-extracted *complete* on
  Tuesday collides on `(system_id, time)` while both are still in the un-cleaned week.
  Passing `[prev, incr]` in that order (existing window first, increment last) makes
  the *corrected* row win. Reverse the order and the stale row shadows the fix —
  silently and permanently. This must be asserted by a dedicated test (§7); the plain
  idempotence test won't catch it because a clean re-run's colliding values are equal.
- **Trim cutoff** = `max(time)` across the merged frame − 90 days (data-relative, not
  wall-clock), which protects a site against *short* extraction lags. A site whose
  latest row lags by more than the window does drop out entirely — arguably correct
  (no recent data → nothing to validate), but note it's not protection against
  arbitrary lag.
- **Listing** prepared PV partitions reuses the storage-abstraction listing the
  versioner already uses for `verify_readiness`.
- **Fail mode.** If `window.csv` is absent (not yet seeded), the step **fails closed
  with a clear "run the seed first" error** rather than silently writing a 1-day
  window — the brief's seeding decision is "full window from launch".

**FS wiring.** `_build_runtime` builds FS from config storages via `get_filesystem`.
Add `validation_window_fs = get_filesystem(config.validation_window_storage)`; the step
also takes the existing `prepared_fs`.

## 4. Component B — one-time seed (bootstrap)

The operator's launch setup checks out the instance repo and pulls the DVC-tracked
data (instance-repo CLAUDE.md), so the prepared PV corpus is present locally under
`data/prepared/pv/<system_id>/pv_<system_id>_<start>_<end>.csv`. The seed needs **no
programmatic clone, no DVC, and no `pv-prospect-versioning` dependency** — it:

1. reads the local partition files matching `pv/*/pv_*.csv` (NOT the stray per-site
   `pv/<system_id>.csv` aggregates, which carry a different, `power_max`-less schema),
   keeping the last 90 days per site,
2. builds the artifact via the **same** `assemble_window` / `write_window` helpers as
   the daily step (guarantees byte-identical schema), and
3. uploads `window.csv` + `manifest.json` to the served prefix.

Ship it as a small one-shot — `scripts/seed_validation_window.py` (or a
`seed_validation_window` console entry) in `pv-prospect-data-transformation` — taking
`--prepared-dir` and `--window-storage`. Run once at launch; not wired into the daily
image's hot path, so DVC stays off it. The seed reads **whatever partitions are
present** and is correct under a full pull (the trim drops the rest), so DVC selection
is a decoupled, optional optimisation on the *pull* step — not part of its contract.

### 4.1 Targeted DVC pull (instance-repo, optional)

A full `dvc pull` fetches the entire multi-year PV history **plus** the ~150-partition
weather corpus, raw, and models — none of which the seed needs. DVC lives wholly in
the instance repo (`.dvc/config`, remotes, the `data/` tree), so this is an
**instance-repo, operator-side, seed-time-only** concern; the submodule seed and the
daily step (which reads `staging/prepared`, no DVC) are untouched.

- **Path scope — the main win.** Pull only `data/prepared/pv/`. Prepared-PV rows
  already carry `temperature` / `plane_of_array_irradiance`, so weather, raw, and
  models are irrelevant to the validation window.
- **Date scope — refinement.** Each partition's `.dvc` filename encodes its range
  (`pv_<id>_<start>_<end>.csv`, `end` exclusive), and the committed `.dvc` files are
  present after checkout **before any data is pulled** — so targets are computed with
  zero fetch: take the **global** `max_end` across all `pv/*/pv_*.csv.dvc`, then
  `dvc pull` only partitions with `end >= max_end − 90d`. Global (not per-site)
  `max_end` keeps the pulled set a superset of the seed's `max(time) − 90d` trim — it
  over-pulls at most one boundary partition per site, which the trim discards. On these
  tiny daily CSVs the date filter saves little beyond the path scope; it earns its keep
  mainly for a lean / CI / container seed, less so for a full local checkout that pulls
  the corpus for other reasons anyway.

Land it as a thin instance-repo wrapper — compute the targets, `dvc pull <targets>`,
then invoke the submodule's DVC-free seed — documented in the instance-repo CLAUDE.md
"Initial Setup". The submodule keeps no DVC knowledge.

*(Fallback if you ever want the pull unattended inside the seed: reuse
`clone_instance_repo` + `dvc_pull` from `pv-prospect-versioning` behind an optional
extra. Rejected for launch — it reintroduces the DVC coupling this design removes.)*

## 5. Config & infra changes

- **Transform config** (`DataTransformationConfig`): add
  `validation_window_storage: AnyStorageConfig` and `validation_window_days: int = 90`.
  Wire into `config-default.yaml` (the `data/served/validation-window/` prefix on
  staging) and `config-local.yaml` (a local dir).
- **Workflow** (`modules/transform/workflow`): add the
  `maintain_validation_window` step gated on the full prepare-phase barrier (after all
  prepare fan-out tasks complete, alongside `consolidate_logs`).
- **IAM:** producer needs **none** (pipeline SA already `objectAdmin` on staging). The
  app-SA *read* grant on `data/served/validation-window/` is part of task 4
  ([validation-serving-docs.md](../briefs/validation-serving-docs.md)), not here.

## 6. Code placement & reuse

- New `pv_prospect/data_transformation/processing/validation_window.py`:
  `read_prepared_pv(prepared_fs)`, `assemble_window`, `trim_window`,
  `read_window`/`write_window`, `run_maintain_validation_window(...)`. Pure where it
  matters (assemble/trim are pure → unit-tested directly); the `run_*` orchestrator
  takes FS at the entrypoint level (no patches).
- Reuse `core.merge_prepared_frames`, `core.read_csv` / `core.write_csv`,
  `core.pv_partition_path` (to parse `system_id`), `core.PV_COLUMNS`,
  `etl.get_filesystem`.
- Per the package-import rule, export the entrypoint-facing
  `run_maintain_validation_window` through the `processing` package `__init__`
  (entrypoint imports it from the package, mirroring `run_consolidate_logs`); keep the
  pure helpers module-private (tests import them by module path).

## 7. Testing

- **Unit (pure):** `trim_window` (cutoff boundary, offline-site retention, empty
  frame); `assemble_window` (system_id injection, column order, dedup on
  `['system_id','time']`); `manifest` contents. New test package
  `tests/unit/validation_window/` with one module per member, plain `def test_*`.
- **Integration:** maintenance over a `LocalStorage` fixture — seed a small window,
  drop a day's prepared partitions into a fake `prepared/pv`, run the step, assert the
  window advanced and trimmed; re-run and assert idempotence (no duplication).
- **Collision keep-policy (the bug with teeth):** `prev` holds a *stale/partial* row at
  `(system_id, time)`; `incr` holds the *complete* row at the same key → assert the
  complete row survives in the written window. Distinct from idempotence (an
  equal-value re-run cannot detect a wrong keep-policy / argument order).
- **Seed:** a `tmp_path` prepared tree → run seed → assert `window.csv` schema matches
  the daily step's output exactly (shared helper guarantees this; the test guards it).

## 8. Decisions resolved / residual choices

Resolved: storage = GCS object on the staging `data/served/` prefix; window in staging
(no producer IAM); seed from the local pulled corpus (no DVC dep), with an optional
instance-repo targeted pull scoping to recent `data/prepared/pv/` partitions; trim
cutoff data-relative; read-all-current-staging for self-healing; fail-closed if
unseeded; separate `job_type`.

Residual (low-stakes, decide at implementation): CSV vs Parquet (default CSV);
exact `manifest.json` field names; whether the seed is a script or a console entry.

## 9. Implementation order

1. `validation_window.py` pure helpers (`assemble_window`, `trim_window`,
   `read_window`/`write_window`) + unit tests.
2. `run_maintain_validation_window` orchestrator + `processing/__init__` export +
   entrypoint `job_type` branch.
3. Config fields + YAML; integration test over `LocalStorage`.
4. `seed_validation_window` one-shot + its test.
5. Workflow step (terraform `modules/transform/workflow`).
6. Manual: do the targeted `dvc pull` (instance-repo wrapper, §4.1), run the seed once
   against the real `data/served/` prefix, and confirm the next daily run advances the
   window. (App-side load is task 2.)
