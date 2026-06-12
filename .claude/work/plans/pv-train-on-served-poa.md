# Plan: Train the PV model on the served (24 h-mean) POA basis

> Companion to `briefs/pv-train-on-served-poa.md`. The diagnosis (why this is the
> primary fix) lives in `plans/pv-yield-overestimate.md` — Gate A measured the ~2×
> overestimate; Gate B attributed ~83 % of it to the PV model at the true POA. This
> plan specifies the fix.

## Goal

Eliminate the dominant ~83 % of the +100 % yield overestimate (Gate B PV-intrinsic
geomean 1.826) by aligning the PV model's **training** POA basis with its **serving**
basis (24 h-mean), so inference no longer evaluates the model in an under-trained,
over-predicting low-POA region.

## Why this route (vs the alternatives)

Three routes reach the same end (see the umbrella plan's *Recommendations*): train on
the served basis (this plan), serve on the trained basis, or recalibrate the low-POA
curve. Training on the served basis is the most **principled** — it fixes the corpus
so the model is in-distribution at inference, and removes the temperature-feature
mismatch in the same change — at the cost of a corpus re-transform + retrain. (The
serve-side route is cheaper to trial and is the fallback if retraining underperforms.)

## Design

### 1. `prepare_pv` convention change — `pv-prospect-data-transformation`

- **Today:** `prepare_pv` inner-joins PV power onto weather hours; PVOutput omits
  night rows, so each daily POA / temperature / target is restricted to **daytime**
  hours → daytime-weighted (89665/06-09: POA 270 vs the 24 h mean 214, +26 %), worst
  on the first day of each 2-day window.
- **Change:** compute POA and temperature over the **full 24 h** weather day
  (decoupled from PV-power coverage), and use an **energy-based** CF target so the
  daily CF sits on the same 24 h basis as the served POA. This also removes the
  edge/interior window inconsistency and the temperature train/serve mismatch.
- Keep clipped/censored rows handled as today; the CF–POA relation stays ~linear
  through the origin on the new basis.

### 2. Re-transform + retrain — operational runbook (corpus-complete variant)

Re-prepare the **entire** PV corpus under the new convention, then retrain. Weather
corpus + weather model untouched. Prior art (marker rewind + supersede ledgers +
trigger): `util/pv_prospect/util/backfill_prepared_corpora_20260524.py`.

**Ground truth that shapes this runbook** (verified 2026-06-12; supersedes the naive
"rewind a marker + supersede 21 ledgers" sketch):

- **Deploy is live.** `data-transformation:latest` (`sha256:81ab2e…`, OCI index; amd64
  child `7b9ef2d…` = the digest this morning's runs executed) is built from c59f018.
  No redeploy needed.
- **The corpus is NOT in staging.** Training data = ~290 per-partition `.dvc` files in
  the DVC `feature` remote (`data-v2026-06-07`, **old basis**, spanning **2024-08-09 →
  2026-06-07** — the backfill, going backward, had only reached 2024-08-09 by then).
  Staging `data/prepared/pv/` holds only the latest un-versioned, *mixed-basis* backfill
  output (70 partitions, incl. the 5 earliest 2024-03→2024-08 windows) — which we
  **discard** and regenerate. So `data-v2026-06-12` ends up *more* complete than
  `data-v2026-06-07` (full 2024-03-22 → today).
- **Feasibility verified.** The 29 extract `pv-sites-backfill` ledgers form a **gapless**
  cover: 28 distinct 28-day windows, 2024-03-22 → 2026-05-15, no gaps. ⇒ Phase 2 must
  regenerate exactly **280** partitions (28 × 10 sites); seam **E = 2026-05-15**.
- **The versioner drains + accumulates.** `data_versioner/core.py` wipes cleaned+
  prepared staging after each version and only ever `dvc add`s (never removes). So a
  clean all-new-basis tag must be built by an explicit **rebuild** (`dvc rm` then add),
  not by the production versioner.
- **Two producers.** The pv-sites *backfill* job emits uniform 28-day windows
  (2024-03 → **2026-05-15**, the last full window); the *daily-transform* workflow emits
  the recent **tail** (2026-05-15 → present) as rolling partitions. The corpus is the
  union. We regenerate **both** so the demo corpus is contiguous to today.
- **Writer is merge-not-skip** (`slice_producer.py` → `merge_prepared_frames`,
  `keep='last'`); a now-missing raw day would leave a stale old-basis row, so we delete
  staging partitions before re-transform rather than rely on overwrite.
- **Raw is fully present** (weather window files + 812 pvoutput per-day files from
  2024-03-22 to today), so the full re-transform will find every input.

#### Phase 0 — Quiesce
Pause all 7 schedulers so nothing interleaves (a stray daily-transform / backfill /
versioner mid-op = mixed-basis corpus + wiped staging):
`pv-prospect-weekly-version`, `pv-prospect-daily-transform`,
`pv-prospect-daily-transform-pv-sites-backfill`,
`pv-prospect-daily-transform-weather-grid-backfill`, `pv-prospect-daily-extract`,
`pv-prospect-daily-extract-pv-sites-backfill`,
`pv-prospect-daily-extract-weather-grid-backfill`
(`gcloud scheduler jobs pause <name> --location europe-west2`). Re-enabled in Phase 6.

#### Phase 1 — Reset transform state (full-history re-plan)
- Back up cursors: `gsutil -m cp -r gs://pv-prospect-staging/tracking/cursors gs://pv-prospect-staging/tracking/cursors-backup-retransform-2026-06-12`.
- Supersede the **22** `*-pv-prospect-transform-pv-sites-backfill.jsonl` ledgers →
  `tracking/superseded-ledgers/2026-06-12/` (discover via `gsutil ls`; else the re-plan
  dedups against completed slice hashes).
- Discard the 70 mixed staging pv partitions: `gsutil -m rm 'gs://pv-prospect-staging/data/prepared/pv/**'`.
- Rewind the marker: write `{"consumed_through": ""}` to
  `tracking/cursors/pv-prospect-transform-pv-sites-backfill.json`.

#### Phase 2 — Backfill re-transform (28-day history, one shot)
`gcloud run jobs execute data-transformation --region europe-west2 --wait --update-env-vars JOB_TYPE=run_transform_backfill,BACKFILL_SCOPE=pv_sites,MAX_EXTRACT_RUNS=40`
(40 > 29 extract ledgers ⇒ whole history in one execution; task timeout 14400s ≫ ~30 min).
**Validate:** partition count **== 280** (28 windows × 10 sites — hard equality, not
≈; a shortfall = a holey corpus); spot-check POA/power dropped ~half vs the old-basis
baseline taken from `data-v2026-06-07` (e.g. 24667 `2024-06-14_2024-07-12` POA 245.1 /
power 370.8 → expect ~half). Re-derive E and the Phase-3 date list from *this* output,
not the old corpus.

#### Phase 3 — Tail re-transform (daily path, seam → today)
- Derive seam `E` = backfill's last 28-day window end (currently **2026-05-15**, uniform
  across sites; re-derive at run time).
- Supersede the daily-transform ledgers for dates ≥ E (the `*-pv-prospect-transform.jsonl`
  from 2026-05-15 on) → superseded-ledgers (else dedup).
- Loop the daily workflow per day (no range arg; starts at E so it abuts, not overlaps).
  **Keep it sequential** — same-ISO-week days read-modify-write the same rolling weekly
  partition via `merge_prepared_frames`, so concurrent runs race and lose writes
  (`gcloud workflows run` blocks, so a plain `for` is correct; do not parallelize):
  `for d in <E .. today>; do gcloud workflows run pv-prospect-transform --location europe-west2 --data='{"pv_system_ids":"all","date":"'"$d"'"}'; done`
- **Validate:** tail partitions present for all 10 sites, new basis, and the seam is
  contiguous — no gap and **no duplicate day** at E-1 / E.

#### Phase 4 — Clean rebuild → `data-v2026-06-12` (one-off; NOT the production versioner)
In a fresh clone of the instance repo, **on branch `main`** — the versioner clones
`instance_repo_branch: 'main'` and accumulates onto it, so committing the rebuild
anywhere else means the first post-resume weekly-version regresses the corpus to old
basis. (`feature` remote configured.)
- `dvc rm` the entire pv tree — all 290 partition `.dvc` **and** the 10 vestigial
  `{site}.csv.dvc` masters — so nothing old-basis can survive.
- Download new-basis staging partitions (backfill + tail) into `data/prepared/pv/<site>/`,
  `dvc add` them, `dvc push -r feature`.
- `git add -A && git commit && git tag data-v2026-06-12 && git push origin <branch> --tags`.
- **Why rebuild not accumulate:** `dvc pull` at a tag serves exactly that tag's `.dvc`
  files; accumulate-overwrite would silently leave any un-regenerated window (or the
  masters) old-basis. Rebuild makes a miss *absent and detectable*. `data-v2026-06-07`
  stays pullable as old basis (immutable tag; blobs not gc'd).
- **Validate:** in a scratch dir, `git checkout data-v2026-06-12 && dvc pull` → only
  new-basis `pv_*.csv`, no masters, contiguous 2024-03 → today.

#### Phase 5 — Retrain + Gate A
- `gcloud run jobs execute model-trainer --region europe-west2 --wait --update-env-vars DATA_VERSION=2026-06-12`
  (trainer reads `DATA_VERSION`, clones the instance repo at `data-v2026-06-12`, pulls).
- Run Gate A — `scripts/measure_yield.py`, same window/store as the original — expect
  pred/actual ≈ 1 across sites (trust the cross-site aggregate).
- The trainer's *internal* promotion gate (`gate.py`, ≤2 pp below incumbent) is separate
  from Gate A; if it blocks promotion, evaluate separately — Gate A is the acceptance.

#### Phase 6 — Resume
Re-enable the 7 schedulers — **unconditionally, even if an earlier phase aborts** (don't
leave the production pipeline paused). Going forward the daily/weekly pipeline accumulates
new-basis partitions onto later `data-v` tags, extending the corpus.

**Rollback:** staging is discardable (we're dropping this week's data anyway); the live
corpus is untouched until Phase 4 pushes the new tag. If Phase 4/5 fail, don't push (or
delete) the tag — nothing consumes `data-v2026-06-12` until the retrain points at it, and
`data-v2026-06-07` remains the old-basis recovery point.

**Open risks:** (a) seam alignment — validate no duplicate days where backfill meets tail;
(b) weather raw presence for tail dates (likely fine; verify in Phase 3); (c) the daily
tail's rolling-partition boundaries differ from the originals (fine for a rebuild).

- If Option A (umbrella) is also adopted, fold its grid-weather sourcing into the same
  re-transform + retrain to avoid a second corpus rebuild.

### 3. Caveat / doc cleanup (on landing)

- Fix the now-false "< 1 % self-consistent" note in `app/poa.py`.
- Per the umbrella's *Cleanup + unblock W1*: once the smoke-test MAPE < 15 %, correct
  the `/predict` caveat (`main.py`) and the legacy ~30 % text in `app/poa.py` and
  `pv-prospect-app/README.md`, flip the TODO note, and unblock W1.

## Acceptance

Re-run Gate A (`scripts/measure_yield.py`, same window/store as the original
measurement) — predicted/actual annual kWh returns to ≈ 1 across sites (trust the
cross-site aggregate; single-year actuals are weather-noisy).

## Risks / open questions

- Retraining with night-zero / low-POA samples reshapes the loss landscape; verify the
  model still fits the daytime CF–POA slope rather than merely learning "low POA → low
  CF" at the expense of mid-range accuracy.
- The Jensen gap (pushing a monthly-mean POA through a nonlinear MLP at inference) also
  feeds the ~83 %; an in-distribution low-POA fit should shrink but may not fully
  remove it — re-measure, don't assume.
- Testing (per `coding-general.md`): carry `prepare_pv` unit tests for the new 24 h
  convention and an energy-target regression alongside the functional change.