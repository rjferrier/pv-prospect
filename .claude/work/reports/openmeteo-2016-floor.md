# Report: Open-Meteo Historical-Forecast 2016 Floor — Backfill Consequences Survey

> Written 2026-07-06. A survey of the consequences of Open-Meteo's fixed
> 2016-01-01 archive floor on the two backward-marching extraction backfills.
> Findings and remedy proposals only — no code or corpus was modified.

## Summary

Since **2026-06-19** the weather-grid extraction backfill has logged a steady
stream of `400 Bad Request` errors from the Open-Meteo historical-forecast API.
The API rejects any request whose `start_date` is before **2016-01-01**:

> `reason "Parameter 'start_date' is out of allowed range from 2016-01-01 to 2026-07-21"`

The root cause is a mismatch between a **fixed** lower bound in the data source
and an **unbounded** cursor in our code. Open-Meteo's historical-forecast
archive begins on a fixed per-model start date (2016-01-01 is the earliest any
model reaches); it is *not* a sliding window. Our two backfill cursors, by
contrast, have **no floor** — they march backward one window per run forever
(`backfill.py: build_backfill_plan` simply computes `start = end − window`, with
nothing clamping it). Both will eventually cross 2016 and fail indefinitely.

The good news, and the direct answer to the question that prompted this survey:
**the training corpus cannot be corrupted by this.** The feared scenario — PV
output data covering a period that weather data does not, leaving orphaned PV
rows in the training set — is structurally impossible. The transform step
double-guards it: `run_prepare_pv` skips any day whose cleaned weather file is
absent, and `prepare_pv` inner-joins PV onto weather. The prepared PV corpus
therefore self-floors at 2016 by construction; a day with PV but no weather
produces **zero** prepared rows.

What *has* happened is operational waste and a data gap, not corruption:

- The **weather-grid backfill (WGB)** crossed 2016 on ~2026-06-19 and is now at
  `2010-05-21`. On 2026-07-06 it made **8,528 failed** pre-2016 weather fetches
  against **1,065 useful** ones — its entire historical march now accomplishes
  nothing, every day, and it leaves a **permanent hole in the grid-weather
  corpus below 2016** that no future run can fill.
- The **PV-sites backfill** is at `2022-05-20` — still healthy, entirely
  ≥ 2016, **not yet across the floor**. Projected to reach 2016 around
  **late September 2026** (~83 daily runs). When it does, it will orphan raw
  PVOutput data (wasted, non-refetchable-quota API calls) but *still* write no
  bad training rows.

The remedy centres on a shared `MIN_ARCHIVE_DATE = 2016-01-01` floor, but the two
scopes use it differently (§6). The PV-sites backfill simply **halts** there
(R1a) — it has no spatial axis. The weather grid, rather than halting, gets a
**2-D cursor** so that flooring the *time* axis frees it to keep building
*spatial* density over [2016, today] — the "staircase" the original 2026-04-03
design intended but never implemented — bounded by a `target_density` cap (R1b,
the chosen direction). No corpus repair is required; the pre-2016 gap is not
fillable; cleanup of the (future) orphaned raw PV is separable.

## Contents

1\. Introduction\
2\. Root cause: a fixed archive floor meets a floor-less cursor\
3\. Current state of each backfill (empirical)\
4\. Why the training corpus is protected\
5\. The actual repercussions\
6\. Remedies\
7\. Conclusions

References

## 1. Introduction

Since 2026-06-19 the pipeline has emitted recurring errors of the form:

```
requests.exceptions.HTTPError: 400 Client Error: Bad Request for url:
https://historical-forecast-api.open-meteo.com/v1/forecast?...&start_date=2010-05-21&end_date=2010-06-03&...&models=best_match,ukmo_seamless
```

Querying an offending URL directly returns the API's own explanation:

> `reason "Parameter 'start_date' is out of allowed range from 2016-01-01 to 2026-07-21"`

The originating question was: we do not need weather/PV history earlier than
2016, so that limit is acceptable in itself — **but have there been
repercussions?** In particular, if PV output data now covers a wider date range
than weather data, the training data would be incomplete. This report surveys
the blast radius empirically (live cursors, ledgers, and raw-bucket contents in
`gs://pv-prospect-staging`) and proposes remedies. Its scope is diagnosis and
recommendation only; nothing was changed.

## 2. Root cause: a fixed archive floor meets a floor-less cursor

**The data source has a fixed floor.** Open-Meteo's historical-forecast archive
starts on a per-model date that is a *permanent* archive start, not a rolling
window. 2016-01-01 is the earliest any model reaches (the JMA models); other
requested models start later. The API validates a request against the *union*
floor of 2016-01-01 and rejects the **whole** request — with a 400 — if
`start_date` falls before it. Because the floor is fixed, there is no urgency to
"catch" pre-2016 data before it ages out: it never existed in this archive.

**Our cursors have no floor.** Both extraction backfills share the primitives in
`pv-prospect-etl/.../backfill.py`. `build_backfill_plan` computes each window as
`start = end − window_days` and sets the next cursor to `start`, unconditionally
— there is no lower clamp anywhere in `backfill.py` or `slice_schedule.py`. The
march therefore continues below 2016 forever.

**The failure does not stop the march.** In the Cloud Workflow
(`terraform/modules/extract/weather_grid_backfill_workflow/main.tf`), a per-site
extraction error is swallowed by the container (exit 0, recorded as a `failed`
ledger entry); the task still "succeeds" (`succeededCount == taskCount`), so the
workflow reaches its `commit` step and **advances the cursor anyway**. Only a
container-level exit code 2 (`WorkflowTerminatingError`) skips the commit. A
pre-2016 400 is neither — it is a per-site failure — so the cursor sails past
2016 and keeps going. The backfill is thus never *stuck*; it just never does
useful work again.

**Only the backward-marching backfills are affected.** The daily forward extract
always requests recent dates (≥ today), so it is unaffected. The blast radius is
exactly the two historical backfills.

## 3. Current state of each backfill (empirical)

Read live from `gs://pv-prospect-staging` on 2026-07-06.

**Cursors** (`tracking/cursors/`):

| Backfill | `next_end_date` | Position vs 2016 floor |
|---|---|---|
| `pv-prospect-extract-weather-grid-backfill` | **2010-05-21** | ~5.6 yr **below** — failing since ~2026-06-19 |
| `pv-prospect-extract-pv-sites-backfill` | **2022-05-20** | ~6.4 yr **above** — healthy, not yet crossed |

**WGB is burning almost entirely on failures.** Its consolidated ledger for
2026-07-06 records **1,065 `completed`** and **8,529 `failed`** task outcomes; of
the failures, **8,528 have a pre-2016 `start_date`** (min `2010-05-21`) and just
one is an unrelated recent transient. The ~1,065 completed are the single
"Step 2" recent-window sample; the entire "Step 3" historical march (the bulk of
the run, paced by ~12-minute inter-batch sleeps) now fails wholesale.

**The grid-weather raw floor is 2016, with the boundary window lost too.** For a
sample grid point, the earliest raw window present is `2016-01-08 → 2016-01-22`
and there are **no** pre-2016 files at all. The window that straddled the floor
(`2015-12-25 → 2016-01-08`) was rejected in its entirety because `start_date`
< floor, so its *valid* 2016-01-01…08 days were collateral loss — the corpus
self-floors at the first **full** window ≥ 2016, not at 2016-01-01 exactly.

**The PV-sites side is clean.** The earliest paired weather raw file for a
sample system is `..._20220520_20220617.csv` (2022-05-20), exactly matching the
PV-sites cursor. Directly listing raw PVOutput for three systems (89665, 4708,
24667) gives the same earliest date — `2022-05-20` — for all of them. There is
**no orphaned pre-2016 PV** and **no pre-2016 pv-sites weather** today
(observed, not inferred).

**Projection.** At 28 days per daily run, the PV-sites cursor needs ~83 more
runs (2331 days ÷ 28) to fall from 2022-05-20 to the 2016-01-01 floor —
approximately **2026-09-27**. That is the deadline before the PV-sites backfill
starts orphaning raw PVOutput data (§5).

## 4. Why the training corpus is protected

The question behind this survey — could PV now cover dates weather does not,
polluting the training set? — resolves to **no**, by two independent guards on
the PV-sites transform path:

1. **Existence gate.** `run_prepare_pv` (data-transformation `processing/core.py`)
   iterates day by day and `continue`s past any day whose cleaned weather file is
   missing (`if not cleaned_fs.exists(weather_path): ... continue`). A pre-2016
   day whose weather fetch 400'd has no raw → no cleaned → is skipped. No batch
   row is written.
2. **Inner join.** Even reaching the join, `prepare_pv` merges PV onto weather
   with `how='inner'` (and returns empty if the weather frame is empty). PV
   without weather cannot survive.

The consequence is that the prepared PV corpus **self-floors at 2016**: it can
never contain a row whose weather is absent. When the PV-sites backfill crosses
2016 (§3 projection), the PV tasks will complete and write raw PVOutput CSVs, the
paired weather tasks will 400, and the transform will simply produce nothing for
those days — wasted extraction, but **zero** corrupt or unmatched training rows.
The grid-weather corpus is protected the same way: a failed weather fetch leaves
no raw file, `run_clean_weather` finds nothing to clean, and no prepared
partition is produced (rather than an empty or partial one).

## 5. The actual repercussions

Not corruption — waste, noise, and a bounded data gap:

1. **Daily failure/alert noise (the reported errors).** ~8.5k failed WGB fetches
   per day, indefinitely. This is the symptom that prompted the survey.
2. **Wasted workflow time and invocations.** Every WGB run spends its full paced
   duration (~12-minute inter-batch sleeps across the Step-3 batches) issuing
   requests that are certain to 400, plus the Cloud Run Job invocations for each
   batch — and the historical march yields nothing. (Whether Open-Meteo counts
   out-of-range 400s against the request quota is unconfirmed; the waste stands on
   the workflow time and invocations regardless.)
3. **A permanent pre-2016 hole in the grid-weather corpus.** The WGB feeds the
   grid-weather corpus behind the capacity-factor map and the (future) spatial
   weather model. Below 2016 that corpus will never be filled. Whether this
   matters depends on the consumer (the map uses recent/climatological means, so
   likely tolerable), but it is a real, permanent gap, plus the lost valid days
   of the boundary window (§3).
4. **Forthcoming orphaned raw PV (from ~late Sep 2026).** Once the PV-sites
   backfill crosses 2016, it will accumulate pre-2016 raw PVOutput CSVs that can
   never join to weather — wasting the scarce PVOutput budget (300 req/hr) on
   unusable data, and marching there forever with no floor. Still no bad training
   rows, but pure waste and clutter in the raw bucket.
5. **A secondary item to verify (not corruption).** Between 2016 and ~2022 the
   default `best_match` model's coverage is thinner than later years; the
   empirical corpus shows non-empty weather back to at least 2019, so this is a
   quality question at the early edge, not a completeness failure. Worth a spot
   check if the early-vintage grid weather is used, but out of scope here.

## 6. Remedies

Proposed, not implemented. Ordered by priority. A shared
`MIN_ARCHIVE_DATE = date(2016, 1, 1)` constant belongs alongside the window
config in `pv-prospect-etl/.../backfill.py` (per the consistency rule — both
backfills share these primitives), but the two scopes *use* it differently
because only the weather grid has a spatial axis (§6.1). The direction below was
chosen on 2026-07-06 after reviewing the original design intent (the 2026-04-03
"staircase" conversation): **the weather grid should densify, not merely halt.**

**R1a — PV-sites backfill: halt at the floor.** The PV-sites backfill has no
spatial axis — 10 fixed systems, one paired PV+weather window per run — so it
simply terminates at the floor. Clamp the final window to
`[MIN_ARCHIVE_DATE, end)` and **no-op once `cursor.next_end_date <=
MIN_ARCHIVE_DATE`** (the guard is essential: a cursor already below the floor
would otherwise plan an *inverted* window). This stops the future PV orphaning
(§5.4) before it starts (~late Sep 2026). The daily forward extract is untouched.

**R1b — Weather-grid backfill: make the cursor 2-D and densify with a cap (the
chosen redesign).** Today the WGB cursor is *one-dimensional*: the sample file is
a side effect of how far back in time it has marched (`sample_index =
(anchor − sample_offset) % 32`, `sample_offset` ever-growing, now 417). So it
runs off the bottom of the archive instead of building spatial density, and the
historical grid sits permanently at ~1 sample file per 14-day window (≈ 3% of the
full 32-sample grid) — the low-density trade the 2026-04-03 design accepted, with
the density-increasing "triangle of blocks" *explicitly deferred and never
built*. The redesign realises it, specialised to a fixed floor:

  - Split the cursor into `(time_position, density_pass)`. **Time is bounded to
    `[MIN_ARCHIVE_DATE, today]`** (the fast, inner axis); `density_pass` is the
    slow, outer axis.
  - When a pass reaches the floor, increment `density_pass`, reset time to today,
    and **shift every window's sample assignment by +1** so pass *k* gives window
    *w* sample `(base + k + w) % 32`. After *k* passes each window holds *k*
    distinct sample files — clean, predictable densification (make Step-3 sample
    choice a pure function of `(pass, window)`, not the daily `anchor`).
  - **Stop starting new passes at a target density** — a "max relative spatial
    density" `f ∈ (0, 1]`, i.e. `target_passes = ceil(f × 32)` (f = 1 → full
    grid; f = 0.25 → 8 passes → quarter density). After the cap, Step 3 goes
    quiescent and only Step 2's rolling window continues. The cap is not
    optional: one full pass over [2016, today] ≈ 274 windows ÷ 8/day ≈ **34 daily
    runs**, so full density ≈ **~3 years** of running; the target should be
    chosen from what the grid-weather consumers actually need.

  This flows through the rest of the pipeline unchanged: each `(sample_index,
  window)` writes its own prepared partition (`weather_{start}_{end}_{ver}-{NN}.csv`),
  so extra passes *accumulate* files rather than colliding, and the transform
  backfill plans them from new `completed` descriptors automatically. **Migration:**
  reinterpret the live cursor (`2010-05-21`, offset 417) as "one partial pass laid
  over [2016, today]" — reset it to `(time_position=today, density_pass=1)` to
  begin the first *densifying* pass, which also immediately stops the daily 400
  storm. `target_density` is the one open parameter (see §7).

**R2 — No corpus repair is required.** The prepared corpora are already correct
(§4). The pre-2016 grid-weather gap is not fillable (the data does not exist) and
needs no action beyond acknowledging it.

**R3 — Optional: pre-empt / clean up orphaned raw PV.** If R1 lands before ~late
September 2026, no orphaned PV is ever created and there is nothing to clean. If
it slips, a one-off deletion of pre-2016 raw PVOutput files under
`data/raw/timeseries/pvoutput/**` (and any pre-2016 pv-sites weather) tidies the
raw bucket. Coordinate with the pending *version-raw-data* work, since raw PV is
otherwise non-refetchable.

**R4 — Document the as-built design and the density decision.** The staircase
design is currently under-documented: `doc/orchestration.md` covers the Step-2 /
Step-3 mechanics and the cursor, but not (i) the deliberate density trade-off
(historical grid is low-density by design), (ii) the "staircase in time and
space" intent, or (iii) that the density-increasing "triangle of blocks" was
deferred and never built. The 2026-04-03 conversation must **not** be documented
as current — the triangle it sketches does not exist in the code. Once R1b lands,
document the as-built Step-2/Step-3 scan, the 2-D densifying cursor, and the
chosen `target_density` in the extraction package README and/or
`doc/orchestration.md`.

**R5 — Optional: alerting.** Consider distinguishing "expected floor/target
reached" from genuine transient failures in the WGB run, so reaching a clean
terminal state is not an error others must triage.

## 7. Conclusions

The recurring 400s are real but benign for the model: the training corpus is
protected by construction and contains no orphaned or mismatched rows, and the
specific failure mode the survey set out to check for cannot occur. The genuine
costs are operational — a weather-grid backfill that has been failing ~8.5k
fetches a day since 2026-06-19 to no effect, a permanent (and acceptable)
pre-2016 grid-weather gap, and a PV-sites backfill on course to start wasting
scarce PVOutput budget on unusable pre-2016 data around late September 2026.

The floor at 2016-01-01 is the shared fix, but the two scopes use it differently
(§6). PV-sites simply halts there (R1a). The weather grid, rather than halting,
gets a 2-D cursor so that flooring the *time* axis frees the schedule to keep
building *spatial* density over [2016, today] — the staircase the original design
intended but never implemented — bounded by a `target_density` cap (R1b). The one
remaining decision is the value of that cap: a target relative density `f`, at a
cost of ~34 daily runs per full pass (`f = 1/32`). A modest target (e.g. 4 passes
≈ 12.5% of full grid, ~4.5 months of running) is a sensible default to refine
against the grid-weather model's actual resolution needs; it can be raised later
without rework, since densification is monotonic. Everything else — the corpus
(needs no repair), the pre-2016 gap (not fillable), the raw-PV tidy-up — is
acknowledgement or optional cleanup.

## References

- Error source & API bound: `historical-forecast-api.open-meteo.com` 400
  response, `start_date` allowed range `2016-01-01 … 2026-07-21`; Open-Meteo
  historical-forecast docs (per-model fixed archive starts).
- Code: `pv-prospect-etl/.../etl/backfill.py` (floor-less `build_backfill_plan`),
  `.../etl/slice_schedule.py`; `pv-prospect-data-extraction/.../extractors/openmeteo.py`;
  `pv-prospect-data-transformation/.../processing/core.py` (`run_prepare_pv`) and
  `.../transformations/prepare_pv.py` (inner join);
  `terraform/modules/extract/weather_grid_backfill_workflow/main.tf` (commit-on-
  swallowed-failure).
- Live evidence (`gs://pv-prospect-staging`, 2026-07-06): cursors under
  `tracking/cursors/`; WGB consolidated ledger under `tracking/ledger/2026-07-06/`
  (1,065 completed / 8,529 failed, 8,528 pre-2016); raw extents under
  `data/raw/timeseries/openmeteo/historical/{weather-grid,pv-sites}/`.
