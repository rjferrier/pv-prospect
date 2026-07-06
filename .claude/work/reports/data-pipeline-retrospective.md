# Report: Data-Pipeline Retrospective — Accepted Patterns, Alternatives, and the State-Model Critique

> Written 2026-07-06. An architectural review — findings and recommendations only;
> no code or corpus was modified.

## Summary

The pipeline's **data-modelling side is sound and conventional by convergence**: the
`raw → cleaned → prepared` progression is medallion layering; the pure-function,
idempotent, immutable-partition discipline is functional data engineering; batch over
streaming (Kafka added in `87bc67e`, later deleted) was the right call; and the
dominant industry stack (Fivetran/dbt/warehouse) is genuinely inapplicable because
the transforms are scientific Python, not SQL.

The commonly-suggested infrastructure alternatives are **rejected on examination**:
open table formats (Iceberg/Delta) solve the wrong half of what the ledger does and
are mis-scaled for micro-batch CSVs, with time travel already covered by DVC at the
training boundary; data-aware orchestrators (Dagster) carry more standing operational
complexity than they remove for a single-author, scale-to-zero project — and the
worst-fitting workload (the transform backfill) has already been migrated off Cloud
Workflows into a single Cloud Run Job.

The one critique that survives contact with the evidence is **architectural, not
tooling**: the pipeline treats side-channel tracking files (cursors, consumed-through
markers, checkpoints, the task-outcome ledger) as the *authority* on what work exists
and what is done, with nothing reconciling those claims against actual bucket
contents. The system's known production failures and its most fragile runbooks all
trace to this. The recommendation (§7) is to move the **transformation** planners to
data-derived planning — plan from the set difference between raw partitions present
and prepared partitions present — demoting the ledger to audit trail. This is
adoptable incrementally within the current zero-ops stack, and it dissolves or
shrinks several operational items already in the backlog. The extraction side keeps
its ledger and cursors: deliberate failure holes cannot be recovered from raw-file
absence alone.

## Contents

1\. Introduction\
2\. Where the architecture matches accepted practice\
3\. Alternatives assessed — and why each is rejected or deferred\
4\. The substantive critique: side-channel state as authority\
5\. Backlog interactions\
6\. Conclusions\
7\. Recommendations

References

## 1. Introduction

The purpose of this review is to examine PV Prospect's existing data-pipeline design
in retrospect, assessing what works well and what is maladaptive, and to determine
whether we could do better. In particular, we ask how the architecture sits relative
to accepted data-engineering wisdom, and whether established principles, patterns,
or off-the-shelf tools are applicable — either as replacements for machinery the
project has built by hand, or as sources of ideas worth adopting in place.

The assessment was derived from the repository itself: `doc/orchestration.md`, the
`pv_prospect.etl` package (orchestration, backfill, ledger storage),
`terraform/modules/`, the runbooks under `pv-prospect-data-transformation/doc/runbooks/`,
the git history, and the work backlog in `.claude/work/TODO.md` with its briefs. The
review's scope is the pipeline — extraction, transformation, orchestration, and data
versioning — not the model or its features.

## 2. Where the architecture matches accepted practice

Three convergences, all arrived at from the problem rather than from fashion — which
is the good direction to arrive from:

- **Medallion layering.** `raw → cleaned → prepared` with distinct GCS prefixes is
  the bronze/silver/gold pattern; `prepared/` is a textbook feature layer feeding
  the DVC-versioned corpus.
- **Functional data engineering.** Pure per-slice transforms, deterministic task
  hashing, idempotent re-runs over immutable date-partitioned outputs, and
  plan-commit cursor promotion match the canonical essay (Beauchemin) nearly
  point-by-point. `filter_remaining_tasks` / `completed_task_hashes()` resumption is
  the idempotency discipline done properly.
- **Batch over streaming.** The data is daily PV readings and historical weather
  forecasts; there is no latency requirement. A Kafka cluster was provisioned
  (`87bc67e`) and deleted — resisting streaming absent a streaming problem is itself
  accepted wisdom.

One further alignment deserves credit because it happened *after* a wrong turn: the
transform backfill briefly ran as a Cloud Workflow and hit that runtime's ceilings
(2 MiB step responses, 256 MiB execution memory, 100 K steps — see
`doc/orchestration.md` §"Why no Workflow?"). It was migrated to a single Cloud Run
Job execution with an in-process thread pool and in-memory collectors. That is the
correct serverless pattern for GCS-bound fan-out work, and it means the standard
"Cloud Workflows is a generic engine doing a data-aware orchestrator's job" critique
is already half-resolved in this system.

## 3. Alternatives assessed — and why each is rejected or deferred

### 3.1 Open table formats (Iceberg / Delta / Hudi) — rejected

The superficial mapping is attractive: the JSONL ledger with plan-commit semantics
looks like a bespoke transaction log, which table formats provide natively (atomic
commits, snapshot isolation, time travel, incremental reads). It fails on three
counts:

1. **The ledger is an *attempt* record, not a commit log.** It records failures with
   error payloads, keyed by task identity, and drives both audit and resumption. A
   table format's log records successful data commits only; adopting one would still
   require an outcome ledger, now alongside a metadata layer to operate.
2. **Time travel already exists where it matters.** The data-versioner snapshots the
   prepared corpus into DVC (`data-v<date>` tags on the `feature` remote). For an ML
   project the reproducibility boundary is *training input*, and that is exactly
   where the snapshot sits. Iceberg time travel over staging CSVs duplicates this.
3. **Wrong scale.** ~10 PV sites, ~1,330 grid points, daily micro-batch CSVs, one
   writer on a cron. Per-commit metadata overhead would rival the data committed.
   Table formats earn their keep with warehouse-scale concurrent writers.

### 3.2 Data-aware orchestrators (Dagster; Airflow/Prefect) — deferred

Dagster's partitioned software-defined assets are the best off-the-shelf match for
this system's shape: "prepared features for site X, window Y" *is* an asset with an
upstream dependency on extracted raw data, and Dagster's stale-partition detection
plus backfill UI replicates the plan/filter/commit apparatus as first-class features.
Two things stop this being a recommendation today:

- **Operational cost dominates at this scale.** The current stack (Scheduler →
  Workflows/Cloud Run Jobs → GCS) is serverless, scales to zero, and costs
  near-nothing idle. A Dagster deployment (or managed Dagster+/Prefect Cloud
  subscription) is more standing complexity than the bespoke orchestration it
  replaces, for a single-author project. "Don't run infrastructure you don't have
  to" is equally part of the canon.
- **The valuable idea is extractable without the tool.** Dagster's core move —
  derive work-to-do from observed materialisation state of the assets themselves —
  can be adopted inside the current stack (§4, §7). Dagster becomes the natural
  landing spot only if the project grows contributors or asset count.

A point in Cloud Workflows' favour that generic critiques miss: **workflow sleeps
are free; container wall-time is not.** The weather-grid extraction backfill spends
~3.4 h mostly sleeping (720 s between batches) to respect OpenMeteo's 5,000 req/h
cap. On Workflows that idle time costs nothing; in a Cloud Run Job it would bill
~3.5 container-hours daily, and in Dagster/Airflow it would hold a worker slot.
Rate-limit choreography (PVOutput 300/h driving the 28-day window; staggered
schedules; in-container backoff) is the extraction side's real complexity driver and
no orchestrator absorbs it — that complexity survives any migration, so the
"re-invention tax" is smaller than it looks.

### 3.3 The modern SQL stack (Fivetran/Airbyte → warehouse → dbt) — inapplicable

The transforms are bilinear spatial interpolation, plane-of-array irradiance
physics, and PyTorch training — scientific Python, not relational SQL. The
most-promoted stack in the field simply does not fit this problem, which partly
explains (and justifies) the custom build.

## 4. The substantive critique: side-channel state as authority

The pipeline maintains four kinds of tracking state under `tracking/` — extraction
**cursors**, transformation **consumed-through markers**, workflow **position
checkpoints**, and the task-outcome **ledger** — and treats them as the source of
truth for what work exists and what is done. Nothing reconciles these claims against
what the `data/` prefixes actually contain. (The write-audit logs record writes but
are not consulted by any planner.)

The evidence that this is the system's real weak point, rather than a theoretical
concern, is already in the project's own record:

- **The documented production incident.** A path-prefix mismatch between the
  chunk-dispatcher's env vars and the container's storage backend caused the cursor
  to **silently advance over un-run work** (`doc/orchestration.md` §"Why no
  Workflow?"). The commit step trusted the tracking file; the data was never
  consulted. Under data-derived planning this class of bug produces redundant
  recomputation (idempotent, harmless) instead of silent gaps.
- **Recovery requires fighting the ledger.** Marker rewind alone cannot replay a
  window, because the resume filter pools `completed` hashes across *every* prior
  consolidated ledger; consolidated ledgers must be manually moved to
  `superseded-ledgers/` so they stop masking re-runs (the replay-window runbook; the
  2026-05-23 Open-Meteo outage recovery; the 2026-06-12 corpus re-transform, which
  hit the same wall — see `reports/pv-train-on-served-poa.md` §2). Every one of
  these is toil created by ledger-as-authority.
- **The convention-change hazard.** Task hashes are computed over env vars, not over
  output content or code version. A transform-logic change that alters outputs
  without altering env (exactly the 2026-06-12 case) is invisible to the ledger:
  old `completed` entries no-op the re-run unless manually superseded.

The accepted-wisdom name for the alternative varies — Dagster calls it asset
materialisations, Hadoop practice called it partition discovery, operations people
call it reconciliation — but the principle is one sentence: **derive work-to-do from
observed data state; demote bookkeeping to audit trail.**

**Where it applies cleanly:** the transformation side. A transform slice's
completion is directly observable — the prepared partition file exists or it
doesn't. The backfill planner (`plan_slices`) currently plans from the extraction
ledger bounded by a consumed-through marker, then filters against pooled transform
ledgers; it could instead plan from the set difference *raw partitions present −
prepared partitions present*. Markers disappear; transform ledgers become pure
audit; replay becomes "delete the prepared partitions and re-run."

**Where it does not:** the extraction side. Deliberate failure holes (the
"predictable cadence" trade-off — failed windows are *not* revisited) mean raw-file
absence cannot distinguish "never planned" from "attempted and failed"; the
attempt record is load-bearing there, and the cursors legitimately track a moving
window through time. The extraction ledger and cursors stay.

**One caveat to resolve at design time:** set-difference planning needs
partition-shaped outputs. The reconciliation surface is `data/prepared-batches/`
(per-slice micro-batch files), not the cumulative `data/prepared/` CSVs, which are
derived assemblies. The exact partition-naming contract (does the batch filename
deterministically encode scope, site/location, and window?) must be pinned down
before implementation; if it does not currently encode enough to reconstruct slice
identity, that naming change is part of the work.

## 5. Backlog interactions

The operational cluster in `.claude/work/TODO.md` reads largely as downstream
symptoms of ledger-as-authority, which corroborates the diagnosis:

| Backlog item | Interaction |
|---|---|
| `outage-recovery` | **Strongest evidence for the change.** Its hardest step — moving transform ledgers to `superseded-ledgers/` to unmask re-runs — exists only because the planner trusts the ledger. Data-derived planning collapses transform recovery to "delete affected prepared partitions"; the brief's remaining extract-side steps could fold into a much shorter runbook. |
| `failure-carry-over` | Confirms the recommendation's *boundary*: extraction holes are deliberate and only the ledger records them, so the ledger stays authoritative there. The proposed failed-row query tool remains useful for extraction; its transform analogue becomes unnecessary. |
| `version-raw-data` | **Synergistic; priority raised.** "Delete prepared and re-run" as the standard recovery move is only comfortably safe if raw — non-refetchable (PVOutput history limits) — is versioned. Also completes the DVC-covers-reproducibility argument against table formats (§3.1). |
| `tracking-restructure` | Shrinks and re-motivates. Half its content (cursor-rewind procedure, `superseded-ledgers/` mechanics, recovery-driven layout arguments) describes duties the tracking files would no longer hold; transform markers are deleted outright. **Sequence after the state-model change** to avoid renaming files headed for deletion. |
| `end-date-semantics` | Survives (extraction cursors keep exclusive-end), but blast radius shrinks — fewer serialized artifacts to migrate. Same ordering note. |
| `manifest-env-footprint` | Likely already stale: its motivating case (transform-backfill manifest vs the 2 MiB limit) predates the migration off Workflows — transform backfills write no manifests. If the daily transform later adopts the same single-process shape (a natural extension), transform manifests cease to exist entirely. Verify daily-transform manifest sizes, then delete or re-scope the brief. |
| `csv-write-logs` | Independent but same principle one generation earlier: decommission tracking artifacts duplicated by better-grounded state. Do regardless. |

The ML-side items (Gate A confirmation, yield-overestimate riders, the 61272/79336
outliers, elevation API, versioner hang, renames, util consolidation) are
orthogonal.

## 6. Conclusions

1. The pipeline's data-modelling architecture (layering, functional discipline,
   batch, DVC-versioned corpus) is on the right side of accepted practice and got
   there by convergence from the problem — no change warranted.
2. The two commonly-prescribed infrastructure migrations do not survive scrutiny at
   this project's scale and shape: table formats solve the wrong half of the
   ledger's job and duplicate DVC; a data-aware orchestrator costs more to stand and
   operate than the bespoke machinery it replaces, and its key idea is extractable
   without it. Cloud Workflows, post-migration of the transform backfill, fits its
   remaining (sleep-heavy, rate-limited) workloads well.
3. The genuine gap versus accepted wisdom is the **state model**: tracking files are
   authoritative and unreconciled against the data. This is not theoretical — the
   silent cursor-advance incident, the superseded-ledgers recovery dance, and the
   convention-change no-op hazard are all instances, and several backlog items are
   its downstream toil.
4. The fix is bounded and incremental: data-derived planning for transformation
   (where completion is observable in the bucket), ledger demoted to audit;
   extraction keeps ledger and cursors (where the attempt record is load-bearing).
   No new infrastructure.

## 7. Recommendations

In priority order:

1. **Adopt data-derived planning for the transformation backfills** (and assess the
   daily transform for the same treatment). Plan from *raw-present − prepared-present*
   over `prepared-batches/`; delete the consumed-through markers; keep writing the
   transform ledger as audit only. Precondition: pin down (or introduce) a
   partition-naming contract on `prepared-batches/` sufficient to reconstruct slice
   identity from filenames. Slot into **Later** ahead of `tracking-restructure` and
   `end-date-semantics`; fold the transform half of `outage-recovery` into it.
2. **Raise the priority of `version-raw-data`** — it is the safety precondition that
   makes "delete prepared and re-run" a routine operation rather than a gamble on a
   re-fetchable API.
3. **Re-scope the dependent briefs** once (1) lands: shrink `outage-recovery` to an
   extract-side runbook; re-motivate `tracking-restructure` as pure
   legibility/archival; verify daily-transform manifest sizes and then delete or
   re-scope `manifest-env-footprint`.
4. **Do not adopt** Iceberg/Delta or a self-hosted orchestrator at current scale.
   Revisit Dagster (managed) only on a concrete trigger: additional contributors, a
   material increase in asset/partition count, or the extraction side outgrowing
   Cloud Workflows' fit.
5. **Proceed with `csv-write-logs`** independently — same principle, no dependency.

## References

Internal:

- `doc/orchestration.md` — the manifest/ledger/cursor design; §"Why no Workflow?"
  documents both the Cloud Workflows ceilings and the silent cursor-advance incident.
- `pv-prospect-data-transformation/doc/runbooks/replay-window.md` — the
  marker-rewind + superseded-ledgers replay procedure discussed in §4.
- `.claude/work/reports/pv-train-on-served-poa.md` §2 — the 2026-06-12 corpus
  re-transform's encounter with ledger-based dedup.
- `.claude/work/TODO.md` and `.claude/work/briefs/` — the backlog items assessed in §5.
- Commit `87bc67e` — the abandoned Kafka cluster.

External:

- Maxime Beauchemin, *Functional Data Engineering — a modern paradigm for batch data
  processing* (2018) — the idempotent/immutable-partition discipline of §2.
- Databricks medallion architecture (bronze/silver/gold layering) — the pattern of §2.
- Dagster's software-defined assets and partitioned-asset model — the
  materialisation-as-state idea assessed in §3.2 and adopted in spirit in §7.
