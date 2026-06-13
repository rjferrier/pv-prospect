# Report: Train the PV model on the served (24 h-mean) POA basis

> Companion to `reports/weather-pv-vintage-alignment.md` (the diagnosis). The design
> it implemented is in submodule commit `c59f018`; the re-base mechanics it pioneered
> are generalised in
> `pv-prospect-data-transformation/doc/runbooks/re-base-corpus.md`. This report records the
> **outcome** — most of which is a change to the **data corpus** (`data-v2026-06-12`)
> and a measurement, neither of which the code diff reveals. Executed 2026-06-12.

> **⚠ Correction (2026-06-12, later the same day).** §5–6's original conclusion — that the
> retrain "hit a structural **Jensen ceiling**" — is **WITHDRAWN**. A follow-up measurement
> decomposed the residual properly (vs the *correctly DVC-pulled* corpus + true GCS actuals):
> the corpus **target is correct** (`corpus/true 1.01`), Jensen is only ~1.04 and POA
> reconstruction ~1.02, and the +51 % residual is dominated by the **`age_years` train/serve
> convention** (served `age=0` vs the aged value used in training, **×1.33**) — *not* Jensen and
> *not* a serve-side POA lever. That age effect is itself unvalidated (likely a partial 10-site
> fixed-effect, not real degradation). See the revised §5–6 and memory
> `pv-retrain-residual-age-convention`. *(An earlier pass briefly mis-concluded "corpus inflated
> 1.57×" by reading stale pre-rebuild `.csv` leftovers whose md5 ≠ their `.dvc`; always confirm
> `dvc status`/md5 before trusting on-disk DVC outputs.)*

## Contents

1. Summary
2. The data-pipeline reality (corrected model — the load-bearing learning)
3. What was executed (the 6-phase runbook)
4. The new corpus — `data-v2026-06-12`
5. Retrain + Gate A result
6. Conclusions & open items
- Appendix: operational specifics (backups, reproduction)

## 1. Summary

The code change was small and self-documented: submodule commit **`c59f018`** made
`prepare_pv` compute POA/temperature over the full 24 h weather day and use an
energy-based CF target, so the PV model's **training** basis matches its **serving**
basis (24 h-mean POA). The large, non-self-documented work was everything downstream:

- **Re-transformed the entire PV corpus** on the new basis and re-versioned it as a
  clean tag **`data-v2026-06-12`** (commit `ea07ed7`, pushed to `main` + the DVC
  `feature` remote). Contiguous 2024-03-22 → 2026-06-11; purely new-basis; the 10
  vestigial pre-partition masters removed. A consumer's `git checkout data-v2026-06-12
  && dvc pull` now yields new-basis data **and only** new-basis data.
- **Retrained the PV model** on the new corpus and measured **Gate A**. The yield
  overestimate **halved** (mean predicted/actual **2.011 → 1.515**) but did **not**
  reach ≈ 1. *(Original claim: a structural Jensen ceiling. **Withdrawn** — see the Correction
  banner. The residual is the `age_years` train/serve convention; the corpus and retrain are
  sound.)*
- **Did not promote** the new model (it still overestimates ~51 % *as measured against aged
  sites at served age 0*; also the user's explicit instruction). Artifact retained at
  `models/gate-a-store-2026-06-12/`.

Bottom line: the corpus fix is fully delivered and durable, and the corpus **target is correct**
(`corpus/true 1.01`). The remaining Gate A overestimate is **not** a PV-curve / serve-side-POA
problem — it is dominated by the **age feature** (served `age=0` vs aged sites), whose validity
is itself the real open question (§6).

## 2. The data-pipeline reality (corrected model — the load-bearing learning)

The original runbook assumed *staging = the corpus*. It is not. Verifying the true
lifecycle was the bulk of the effort, and it dictates how any future corpus
re-transform must be done:

- **The corpus is NOT in staging.** Training data is the set of per-partition `.dvc`
  files in the DVC **`feature` remote** (`gs://pv-prospect-versioned-feature`), pinned
  by a `data-v<date>` git tag. Staging `data/prepared/pv/` holds only the latest
  **un-versioned** backfill output — it is a *drain*, not the store.
- **The versioner drains + accumulates.** `data_versioner/core.py` reads staging,
  `dvc add`s it onto the existing corpus (never removes), then **wipes** cleaned +
  prepared staging. So it can only *add/overwrite by path* — it cannot purge.
- **Two producers feed the corpus.** The pv-sites **backfill** Cloud Run Job
  (`JOB_TYPE=run_transform_backfill`) runs **backward** in time, one uniform 28-day
  window per scheduler cycle (2024-03-22 → 2026-05-15 here). The **daily-transform**
  Cloud Workflow (`pv-prospect-transform`, single-date arg) produces the recent rolling
  *tail*. The corpus is the union; both must be regenerated for a full re-transform.
- **Slice dedup is ledger-based.** Re-runs dedupe against `completed` task-hash entries
  in `tracking/ledger/`; the convention change touches none of the hash fields, so old
  ledgers must be **superseded** (moved out of the ledger prefix) or the re-transform
  no-ops. The backfill is additionally bounded by a `ConsumedMarker` cursor that must be
  rewound to `""` to re-plan the whole history.
- **The writer is merge-not-skip** (`slice_producer.py` → `merge_prepared_frames`,
  `keep='last'`). New rows win, but a now-missing raw day leaves a stale old-basis row,
  so the safe move is to **delete the old partitions before** re-transforming.
- **A clean tag therefore needs a manual rebuild.** Because the versioner is
  accumulate-only, an all-new-basis tag cannot be produced by running it (it would leave
  any un-regenerated window — and the vestigial `{site}.csv` masters — old-basis).
  Instead: in a clone, `dvc rm` the whole pv tree, drop in the new partitions,
  `dvc add` → `dvc push` → commit → tag. `dvc pull` at a tag serves *exactly* that
  tag's `.dvc` files, so the rebuild makes any miss **absent and detectable** rather
  than silently old.

## 3. What was executed (the 6-phase runbook)

The schedulers were paused throughout so nothing interleaved; the live image was
already `c59f018` (verified: `:latest` is an OCI index whose amd64 child was the digest
the morning runs executed).

| Phase | Action | Gate / result |
|---|---|---|
| 0 Quiesce | Pause all 7 schedulers | pipeline frozen |
| 1 Reset | Back up cursors; supersede 22 backfill ledgers; delete the 70 mixed staging partitions; rewind marker → `""` | — |
| 2 Backfill | One-shot `run_transform_backfill` (`MAX_EXTRACT_RUNS=40`) | **280** partitions (28 windows × 10 sites) — exact `==` gate; POA/power dropped by the daylight fraction (~0.69 for midsummer windows), confirming the 24 h basis |
| 3 Tail | Supersede 30 daily ledgers; **sequential** per-day loop of `pv-prospect-transform` over 2026-05-15 → 2026-06-11 | tail abuts the backfill seam, no gap/overlap; +50 partitions |
| 4 Rebuild | Clone on `main`; `dvc rm` old (290 partitions + 10 masters); add 330 new; `dvc push` + commit + tag `data-v2026-06-12` + push | client clone-at-tag `dvc pull` = new-basis only, no masters, contiguous |
| 5 Retrain + Gate A | Train PV **locally** (CPU torch, defaults = production parity); assemble store (new PV + incumbent weather); run `measure_yield.py` | 2.011 → **1.515**; **not** promoted |
| 6 Resume | Wipe pv staging (already versioned); re-enable all 7 schedulers | pipeline live |

Notes that matter for repetition: the tail loop **must be sequential** (same-ISO-week
days race the rolling weekly partition via `merge_prepared_frames`); the rebuild **must**
land on `main` (the versioner clones `instance_repo_branch: 'main'`, so committing
elsewhere would let the next weekly-version regress the corpus to old basis); the Cloud
`model-trainer` job was **not** used because it auto-promotes (no skip flag).

## 4. The new corpus — `data-v2026-06-12`

- **330 partitions** (33 per site) = 280 backfill 28-day windows (2024-03-22 → 2026-05-15)
  + 50 daily tail partitions (→ 2026-06-11). The 10 vestigial `{site}.csv` masters are
  gone. Weather corpus unchanged (the fix is PV-only).
- **More complete than `data-v2026-06-07`**, which only spanned 2024-08-09 → 2026-06-07
  (the backward backfill hadn't yet versioned the five earliest 2024-03 → 2024-08
  windows).
- **Contiguity**: 7 of 10 sites are gap-free over all 812 days. Three have **genuine
  site-outage days** — `25724` (5), `61272` (4), `56874` (starts 2024-03-29 + ~12
  interior) — where raw PVOutput returned header-only files (no readings). These are
  unfixable from data and were equally absent in `data-v2026-06-07`.
- **Rollback**: `data-v2026-06-07` remains fully pullable as the old-basis corpus (its
  tag is immutable and the old blobs were not garbage-collected).

## 5. Retrain + Gate A result

New model: test CF R² **0.852** (incumbent `model-v2026-06-10` was 0.861 — within the
trainer's 2 pp promote tolerance). Gate A replicates the original 1.826/2.011 baseline
exactly (store `gs://pv-prospect-versioned-model`, actuals
`raw/timeseries/pvoutput/`, window **2025-06-09 → 2026-06-08**), swapping **only** the
PV model:

| site | baseline pred/act | new pred/act | new PV-intrinsic* |
|---:|---:|---:|---:|
| 4708 | 1.928 | 1.514 | 1.47 |
| 24667 | 2.124 | 1.596 | 1.42 |
| 25724 | 2.076 | 1.535 | 1.35 |
| 36019 | 2.022 | 1.531 | 1.42 |
| 42248 | 1.877 | 1.412 | 1.29 |
| 56874 | 1.654 | 1.257 | 1.22 |
| 61272 | 3.055 | 2.260 | 1.99 |
| 79336 | 1.653 | 1.245 | 1.19 |
| 82517 | 2.256 | 1.646 | 1.53 |
| 89665 | 1.462 | 1.150 | 1.10 |
| **mean** | **2.011** | **1.515** | **1.40** |

\*new pred/act ÷ that site's Gate B weather-path factor; isolates the PV model. **This column
is SUPERSEDED** — see the corrected decomposition below.

**Interpretation — CORRECTED (the original "Jensen ceiling" reading was wrong).** Overestimate
halved (+101 % → +51.5 %), but a proper decomposition — against the **correctly DVC-pulled**
corpus and true GCS actuals — shows the residual is **not** Jensen and **not** in the PV/POA
path. Gate A `chain/true 1.51` factors multiplicatively (geomean, 10 sites; `diag_decompose.py` — diagnostic script, not preserved):

| corpus/true | model fit | **age_years** | temperature | POA-recon + Jensen |
|---:|---:|---:|---:|---:|
| 1.01 | 1.04 | **1.33** | 1.01 | 1.05 |

The corpus **target is correct** (`corpus/true 1.01` vs true generation); Jensen is ~1.04 and POA
reconstruction ~1.02 (together ~5 %, not the cause). The dominant term is the **`age_years`
train/serve convention**: training derives age from each site's real `installation_date`
(panels 4–14 yr old), but the serving chain and `measure_yield.py` pass **`age=0`** (a new
install). Restoring each site's real age collapses pred/actual **1.49 → ~1.0** for all 8 sites
that have an install date (`diag_realage.py` — diagnostic script, not preserved). 61272 (no install date) is a genuine model-
*fit* outlier (B/A = 1.89), not an age effect.

**Caveat — don't over-read the age result.** With only 10 sites `age_years` is near-collinear
with site identity, so the MLP may be using it as a per-site intercept (memorising each site's
baseline level), not learning degradation. The implied rate is ~1.4–2.2 %/yr in-distribution and
*non-monotonic* at low POA (vs physical ~0.5–1 %/yr), and `age=0` is an extrapolation below the
youngest training site (4 yr). So the "real age → 1.0" check is **near-circular** and does not by
itself establish the model for new-install yield.

The model was **not promoted** — correct on the merits and per instruction. Artifact + assembled
Gate A store retained at `models/gate-a-store-2026-06-12/`.

## 6. Conclusions & open items

- **Delivered & durable**: the corpus is re-based and re-versioned; consumers get a
  clean, contiguous, new-basis corpus on `dvc pull` at `data-v2026-06-12`. The target is
  **correct** (`corpus/true 1.01`) — the energy-basis fix landed.
- **The primary route worked at the corpus level.** It removed the train/serve POA-basis
  mismatch and the daytime-weighting in the target. Jensen (~1.04) and POA reconstruction
  (~1.02) are minor; the earlier "daily→monthly Jensen residual" framing is **withdrawn**.
- **The remaining Gate A overestimate is the `age_years` train/serve convention** (served
  `age=0` vs aged sites, ×1.33) — *not* a serve-side POA lever. **Recalibrating the low-POA
  CF curve or changing POA aggregation would each chase ~5 % and mask the real cause; don't.**
- **Real next lever: validate/fix the age feature** — degradation law vs 10-site fixed-effect.
  This decides whether the model can predict *new-install* yield at all; likely needs more
  sites or a constrained ~0.5–1 %/yr degradation prior / explicit site effects. For the
  **demo** specifically, serving the aged demo sites' real ages → pred/actual ≈ 1.0
  (legitimate but cosmetic).
- **Promotion** stays deferred.
- **`61272`** is a genuine model-*fit* outlier (B/A 1.89; no install date, so age doesn't
  explain it); `79336` also lacks an install date. Both worth a separate look.
- Diagnostics + deeper analysis: memory `pv-retrain-residual-age-convention`.
  Supporting scripts (`diag_{jensen,recon_full,chain_vs_corpus,corpus_true,decompose,realage,agesweep}.py`)
  were written to `/tmp` during investigation and not preserved; if needed, reconstruct
  them under `pv-prospect-instance/data-exploration/irradiance/poa_attribution/`.

## Appendix — operational specifics

- **Reproduce the measurement**: `cd pv-prospect-app && PYTHONWARNINGS=ignore poetry run
  python scripts/measure_yield.py --store-dir models/gate-a-store-2026-06-12
  --pv-sites-csv data/static/pv_sites.csv --actuals-gcs-prefix
  gs://pv-prospect-staging/data/raw/timeseries/pvoutput/ --start 2025-06-09 --end
  2026-06-08`. Local training: `cd pv-prospect-model && poetry run python -m
  pv_prospect.model.entrypoint train-pv --data-root <pulled tag>/data/prepared
  --pv-sites-csv … --output-dir <store>/promoted/pv` (CPU torch, all defaults).
- **Backups retained** (in `gs://pv-prospect-staging/tracking/`): cursors at
  `cursors-backup-retransform-2026-06-12/`; superseded ledgers at
  `superseded-ledgers/2026-06-12/` (22 backfill) and `…/2026-06-12-daily/` (30 daily).
- **Left intentionally**: 45 unversioned 2017–18 weather-grid staging partitions (the
  weather pipeline's backfill frontier, unrelated to this task) — left for the normal
  weekly-version, not wiped.
- Deeper operational mechanics live in the assistant memory
  `pv-retransform-served-poa-mechanics`.
