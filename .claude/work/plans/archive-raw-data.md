# Plan: Archive raw extracted data (was: version raw data)

## Pivot: archive, not version

The originating brief (now `briefs/archive-raw-data.md`, formerly `version-raw-data`)
and retrospective rec 2 originally called for extending the data-versioner to
DVC-snapshot `staging/raw/` alongside
the prepared corpus. **That approach is dropped.** Investigation shows
DVC-versioning buys essentially nothing for raw, at real cost:

- **Nothing consumes versioned raw.** The model-trainer pulls only the feature
  (prepared) corpus; no automated consumer does `dvc pull` on raw. The one
  downstream reason this task exists — making `data-derived-transform-planning`'s
  "delete prepared and re-run" recovery safe — needs raw to be **durable**, not
  git-tag-reconstructable.
- **Raw is append-only + non-refetchable.** The only "version" it could ever have
  is an in-place overwrite from a re-fetch, and GCS object versioning captures
  exactly that for free. Historical point-in-time reconstruction is near-valueless
  for an append-only archive (today's corpus is a superset of every past state;
  file paths already encode their date-range coverage).
- **The machinery is a heavy misfit.** `data/raw/` is **701,626 objects / ~6 GB**
  today and growing (weather-grid densification, `openmeteo-2016-floor`, will grow
  it further). DVC must materialise and re-hash the *whole* tree every run to
  recompute its directory manifest — an unbounded weekly download-and-hash on the
  2 Gi / 30-min versioner job (which already has a hang-on-exit bug,
  `briefs/versioner-hang.md`). Per-file DVC is impossible at 700k files, and the
  pipeline SA lacks write IAM on the versioned-raw bucket.

**Goal (from the user): minimise cost.** Raw wants a durable, cheap archive, not
versioning. GCS storage-class tiering (Coldline) delivers that natively.

## Key facts established

| Fact | Value / source |
|---|---|
| Raw corpus size | 701,626 objects, ~5.96 GB under `gs://pv-prospect-staging/data/raw/` |
| Raw layout | `data/raw/timeseries/{pvoutput/<site>/…daily, openmeteo/historical/{pv-sites/<site>,weather-grid/<cell>}/…window}` + `-meta.json` sidecars |
| Raw mutation | Append-only (extraction only adds new windows/days; nothing prunes) |
| Raw I/O config | Both extraction (write) and transform (read) go through the **single** shared config key `staged_raw_data_storage` — see `data_extraction/util/upload_file.py`, `data_transformation/processing/{runner,entrypoint}.py` |
| `versioned-raw` bucket | Exists (`gs://pv-prospect-versioned-raw`), **empty**, HNS enabled, no versioning/retention blocks |
| DVC-raw scaffolding | `raw` remote in instance `.dvc/config`, `versioned_raw_data_storage` in ETL config, `dvc_sa` objectCreator IAM on the bucket — **all confirmed unused** (no `.py` refs; no raw `.dvc` files exist) |
| Versioner ↔ raw | The versioner **never touched raw**; raw-versioning was only ever a planned extension. Nothing to remove from `version_data`. |
| `pv-prospect-data` bucket | Moribund legacy bucket (656 MB): old DVC cache (`files/md5/…`) + old `staging/…` raw, last written 2026-03-06 (~4 months stale), orphaned in Terraform, not referenced by current `.dvc/config`. To be deleted. |
| `versioned-feature` / `versioned-model` sizes | 296 MB / 127 KB — both small, stable, and read-frequently (see "Coldline elsewhere?" below) |

## Recommended design

**A single dedicated raw bucket with in-place Coldline lifecycle tiering. No move
step, no DVC, no versioner change, no application code.**

The user's cost intuition (Standard while transform reads it; Coldline for the
cold tail) is correct — but GCS gives it *without* a cross-bucket move:

- A raw object is **Standard for its first N days** — the window transform
  consumes it in (no Coldline retrieval fees on the hot read).
- A lifecycle rule flips it to **Coldline after N days** (~30), **in place, at the
  same path, at zero operation cost** — long after transform is done.

GCS lifecycle **cannot** move objects between buckets, so a "move consumed raw to
an archive bucket" step (the sketch) would need Storage Transfer Service or a
custom job — ongoing machinery + per-object op costs — to reach the *same billing
outcome* the lifecycle rule gives for free. The move is rejected: pure
cost/complexity, no benefit.

Because raw never leaves the bucket (only re-tiers), it stays a single listable
corpus — so the future `data-derived-transform-planning` planner sees all of raw
in one place and "delete prepared and re-run" stays correct (a re-run over old
data just pays a small one-off Coldline retrieval fee).

### Flow

```
extraction ──writes Standard──▶  gs://<raw-bucket>/timeseries/…   (hot, ≤N days)
                                        │
transform  ──reads Standard───────────┘   (consumes within days)
                                        │
                          lifecycle SetStorageClass Coldline @ age N days (in place)
                                        ▼
                                 Coldline cold tail (kept forever, cheap)
```

## Changes required

All work is **Terraform + one config value + a one-time data move + docs**. No
application logic changes.

1. **Terraform — new raw bucket + decommission dead buckets**
   (`terraform/modules/storage/main.tf`):
   - **Create** `google_storage_bucket.raw` named `pv-prospect-raw`, HNS enabled,
     default storage class Standard, with a lifecycle rule: `action { type =
     "SetStorageClass"; storage_class = "COLDLINE" }`, `condition { age = 8 }`.
     Leave GCS soft-delete at its default; **no** retention lock (threat isn't a
     concern; a lock would block any future correction). Add an output for the name.
   - **Remove** `google_storage_bucket.versioned_raw` (+ its `outputs.tf` entry and
     the `dvc_sa_versioned_raw` IAM member) — empty, and the DVC-versioning role it
     existed for is gone.
   - **Remove** `google_storage_bucket.pv_prospect_data` — moribund (facts table).
     It holds 656 MB, so empty it first (`gcloud storage rm --recursive
     gs://pv-prospect-data/`, or set `force_destroy = true` before the destroy),
     then drop the resource. Caveat to accept: it is a *legacy* DVC cache, so any
     `data-v` tag old enough to predate the bucket split may no longer `dvc pull` —
     acceptable given archival reproducibility of raw is explicitly deprioritised.

2. **Config — redirect raw I/O** (`pv-prospect-etl/.../resources/config-default.yaml`):
   - Point `staged_raw_data_storage` at the raw bucket (`bucket_name:
     <raw-bucket>`, `prefix:` its top-level, e.g. `timeseries` or empty). Extraction
     and transform both follow automatically.
   - Check `config-local.yaml` / any env overrides for a `staged_raw_data_storage`
     override that would need matching (local dev likely stays on the local FS).

3. **Terraform — IAM**: grant the **pipeline** SA `roles/storage.objectAdmin` on
   the raw bucket (extraction writes; transform reads; the one-time migration needs
   delete on the staging source, which pipeline already has). Mirror the existing
   `pipeline_versioned_feature`/`_model` grants in `main.tf`.

4. **One-time migration** of existing raw out of staging:
   - `gcloud storage mv gs://pv-prospect-staging/data/raw/** gs://<raw-bucket>/…`
     (or an STS one-off). 701k objects — expect this to take a while; run it as a
     deliberate operational step, not from the pipeline.
   - The existing raw is all old/already-transformed, so it should end up Coldline.
     Simplest: copy as Standard and let the lifecycle rule tier it on its next
     daily pass (≤1 day at Standard, negligible). Or set `--storage-class COLDLINE`
     on the move.
   - **Sequencing gate:** quiesce extraction/transform (or run during the daily
     gap) so nothing writes to the old path mid-move; flip the config (step 2) and
     redeploy *before* re-enabling, so new writes land in the new bucket.

5. **Decommission unused DVC-raw scaffolding:**
   - Remove the `['remote "raw"']` block from the instance repo `.dvc/config`.
   - Remove `versioned_raw_data_storage` from the ETL config (confirmed no `.py`
     consumers).
   - Update instance-repo `CLAUDE.md` (drop "raw datasets use `remote: raw`").

6. **Docs & diagram:**
   - `doc/architecture.puml`: relabel `VersionedRaw` and the V1/V2 interactions —
     raw is now an archive fed directly by extraction with lifecycle tiering, not a
     DVC store written by the versioner.
   - Top-level `README.md` + `terraform/modules/storage/main.tf` header comment:
     staging no longer holds raw; document the raw archive bucket and the
     Standard→Coldline tiering. (Cross-package, external storage implication →
     top-level README per `documenting.md`.)
   - Brief already renamed to `briefs/archive-raw-data.md` and reframed (TODO +
     dependent briefs updated). At completion, write a short `reports/` entry —
     the corpus relocation is an external-storage change worth a durable record
     (new bucket, tiering policy, the one-time move, decommissioned buckets) — and
     finalise per the lifecycle (fold brief content into permanent docs, delete the
     task + brief + plan; retain the report).

## Decisions (resolved with the user)

- **Bucket:** fresh `pv-prospect-raw`; decommission the empty `versioned-raw` and
  delete the moribund `pv-prospect-data` (step 1).
- **N (Standard→Coldline age): 8 days.** Transform consumes raw within ~1–2 days;
  8 gives a week of Standard margin covering short pipeline outages/catch-up before
  raw tiers cold. (The original "weekly versioning" rationale is moot — versioning
  no longer reads raw — but ~a week of margin is still right.) Coldline's 90-day
  minimum-storage-duration never bites: raw is kept forever, never early-deleted.
- **Cold-tier class: Coldline** (not Archive) — occasional re-transform/recovery
  re-reads old raw, and Archive's 365-day minimum + higher retrieval fit worse.
- **Feature/model buckets: stay Standard** (see below).

## Coldline elsewhere? (feature/model buckets) — recommend against

Coldline pays only for **write-once / read-rarely** data. The versioned buckets
are the opposite — small, stable, and read-frequently — so tiering them is a net
loss:

- **`versioned-feature` (296 MB).** The model-trainer `dvc_pull`s the *entire*
  corpus on every weekly run (cold-cache clone, `model_trainer/bootstrap.py`).
  Coldline retrieval on ~52 full pulls/yr ≈ 52 × 0.30 GB × $0.02 ≈ **$0.31/yr**,
  versus a storage saving of only ≈ (0.020 − 0.004)/GB·mo × 0.30 × 12 ≈
  **$0.06/yr** — a **net loss**, before higher per-op read charges. Nearline loses
  too (weekly reads beat its 30-day break-even). **Leave Standard.**
- **`versioned-model` (127 KB).** Negligible at any class, *and* its plain
  `promoted/{pv,weather}/` serving path is read **per-inference** by
  `pv-prospect-app` — the worst pattern for Coldline retrieval/latency. **Leave
  Standard.**

Why raw is different: raw is read once by transform then goes dormant, *and* it is
the one corpus that grows without bound (weather-grid densification,
`openmeteo-2016-floor` backfill), so the tiering saving scales with it. At today's
sizes every figure here is pennies/yr; raw is worth tiering because it is the part
that grows.

## Interaction with `data-derived-transform-planning` (sequenced next)

That task's planner computes *raw-present − prepared-present* and its recovery
move is "delete prepared partitions and re-run." This design keeps that correct:

- Raw stays one listable corpus in the raw bucket (in-place tiering, no move-out),
  so the planner's `raw-present` listing is unaffected by archival.
- A re-run over old (Coldline) data reads it back — a small, rare retrieval fee;
  acceptable and the whole point of keeping raw durable.
- **Confirm** the planner lists the new bucket (it will, via `staged_raw_data_storage`).

Listing cost note: `raw-present` grows with the corpus, echoing the
`ledger-scan-cost` pattern — but at 700k objects a full daily list is ~$0.004,
trivial. Not a driver here.

## Risks & verification

- **Mid-move races.** Extraction/transform writing/reading the old path during the
  move. Mitigation: config flip + redeploy gated around a quiesced window (step 4).
- **Config drift across envs.** A stray `staged_raw_data_storage` override
  (`config-local.yaml`, Cloud Run env) pointing at the old path. Verify all envs.
- **Verify end-to-end** after the flip: run one extraction (`docker compose run …
  openmeteo/hourly <site> -d <date>`) and confirm the object lands in the raw
  bucket; run a transform slice and confirm it reads from there; confirm the
  lifecycle rule is active (`gcloud storage buckets describe`).
- **Coldline retrieval surprise.** If N is set too low, transform re-reads (e.g.
  daily-transform reprocessing a recent window) could hit Coldline. N = 30 avoids
  this.

## Effort & sequencing

Small: a new bucket + lifecycle rule + IAM member + one config value + a one-time
bulk move + two dead-bucket decommissions + docs. No Python, no versioner change,
no new service. Unblocks `data-derived-transform-planning` (raw is now durable).
Do the DVC-scaffolding cleanup (step 5) in the same change since it's the direct
counterpart of the pivot.

---

## Phase A — landed (reversible repo work)

Committed and inert until a deliberate deploy/apply:

- Submodule `5a2524e`: `terraform/` (new `raw` bucket + lifecycle + pipeline IAM +
  `raw_bucket_name` output; removed `versioned_raw` and `pv_prospect_data`
  resources/IAM/output) and `pv-prospect-etl` `config-default.yaml`
  (`staged_raw_data_storage` → `pv-prospect-raw`, no prefix; dropped
  `versioned_raw_data_storage`).
- Instance repo `5263d45`: `.dvc/config` (removed `raw` remote), `CLAUDE.md` (DVC
  section rewrite).

Confirmed at write time: `versioned-raw` empty (0 B), `pv-prospect-data` 656 MB,
`staging/data/raw` 5.96 GB / 701k objects (`gcloud storage du`); no runtime env
override touches the raw bucket (`apply_prefix_overrides` rewrites only
prepared/cursors/ledger *prefixes*, never `staged_raw` or any `bucket_name`);
`terraform validate` + `fmt` clean; transformation config/override tests green.

Deliberately **not** in Phase A (they must track live reality / real measurements):
narrative docs (`README.md`, `doc/architecture.puml`), the `reports/` entry, and
task finalisation (delete brief + plan, TODO). These ride with Phase B execution.

## Phase B — execution runbook (gated on go-ahead)

Outward-facing, partly irreversible; run as one deliberate operational sequence.
Env: project `semiotic-effort-474309-t7`, region/location `europe-west2`. New
bucket layout: **no prefix** — objects live at bucket root (`timeseries/…`), so the
migration must map `data/raw/<X>` → `<X>`.

1. **Create the new bucket + IAM only** (targeted apply — does *not* touch the
   removed buckets yet):
   ```
   cd terraform
   terraform apply \
     -target=module.storage.google_storage_bucket.raw \
     -target=google_storage_bucket_iam_member.pipeline_raw
   gcloud storage buckets describe gs://pv-prospect-raw \
     --format='value(lifecycle_config)'   # confirm SetStorageClass→COLDLINE age 8
   ```

2. **Quiesce** the pipeline (daily schedulers leave `paused` unmanaged, so
   out-of-band pause is safe and Terraform won't fight it):
   ```
   for J in pv-prospect-daily-extract \
            pv-prospect-daily-extract-pv-sites-backfill \
            pv-prospect-daily-extract-weather-grid-backfill \
            pv-prospect-daily-transform \
            pv-prospect-daily-transform-pv-sites-backfill \
            pv-prospect-daily-transform-weather-grid-backfill; do
     gcloud scheduler jobs pause "$J" --location=europe-west2
   done
   ```
   Let any in-flight execution drain before proceeding.

3. **Migrate** existing raw out of staging, straight to Coldline (already-cold,
   already-transformed data — skip the 8-day Standard tail). 701k objects: expect
   this to run long; use a resumable session (tmux/`nohup`) or Storage Transfer:
   ```
   gcloud storage mv gs://pv-prospect-staging/data/raw/* gs://pv-prospect-raw/ \
     --recursive --storage-class=COLDLINE
   # spot-check the path mapping is data/raw/<X> → <X>:
   gcloud storage ls gs://pv-prospect-raw/timeseries/pvoutput/ | head
   gcloud storage ls gs://pv-prospect-staging/data/raw/   # should be empty
   ```

4. **Deploy the redirected config** via the normal image build/deploy path: rebuild
   + push the `data-extraction` and `data-transformation` images (they bundle the
   `pv-prospect-etl` config), bump `extractor_image_tag` / `transformer_image_tag`
   in `terraform.tfvars`, and apply. New writes/reads now use `pv-prospect-raw`.

5. **Verify end-to-end** (still quiesced): trigger one extract execution for a
   single system/day and confirm the object lands at
   `gs://pv-prospect-raw/timeseries/…`; run one transform slice and confirm it
   reads from there. Re-confirm the lifecycle rule (step 1).

6. **Re-enable** the schedulers (`gcloud scheduler jobs resume …` for the six jobs).

7. **Destroy the dead buckets.** `versioned-raw` is empty (clean destroy);
   `pv-prospect-data` (656 MB) must be emptied first or Terraform refuses:
   ```
   gcloud storage rm --recursive gs://pv-prospect-data/**
   cd terraform && terraform apply    # untargeted: destroys the two removed buckets
   ```

8. **Finalise** (per the task lifecycle): flip `README.md` (raw lives in
   `pv-prospect-raw`, Standard→Coldline; staging no longer holds raw) and
   `doc/architecture.puml` (drop the `VersionedRaw` node + V1/V2 versioner→raw
   arrows; raw is an archive fed by extraction, read by transform); write
   `reports/archive-raw-data.md` with the actual figures (objects moved, buckets
   destroyed, tiering confirmed live); remove the task from `TODO.md`; delete this
   plan and `briefs/archive-raw-data.md` (retain the report). Push the submodule,
   bump the instance-repo pointer, push the instance repo.
