# Floor the backfill march at 2016 and densify the weather grid

> **Full backing:** `reports/openmeteo-2016-floor.md` (survey of the consequences,
> live-state evidence, and the design decision taken 2026-07-06). This brief is
> the actionable summary; the report carries the reasoning and measurements.

## Context

Open-Meteo's historical-forecast API rejects any `start_date` before a **fixed**
2016-01-01 archive floor. Both extraction backfills share the cursor primitives
in `pv-prospect-etl/.../etl/backfill.py`, which have **no lower clamp** — they
march backward one window per run forever. The weather-grid backfill (WGB)
crossed the floor ~2026-06-19 and is now at `2010-05-21`, failing ~8.5k pre-2016
weather fetches per day to no effect; the PV-sites backfill is still healthy at
`2022-05-20` but is projected to cross the floor ~**late September 2026**, after
which it will orphan non-refetchable raw PVOutput data.

The training corpus is **not** at risk — `run_prepare_pv` skips days with no
cleaned weather and `prepare_pv` inner-joins, so PV without weather yields zero
rows (report §4). This is an operational/efficiency fix, not a corpus repair.

## What is needed

A shared `MIN_ARCHIVE_DATE = date(2016, 1, 1)` constant in `backfill.py`, used
differently per scope (only the weather grid has a spatial axis):

### R1a — PV-sites backfill: halt at the floor

Clamp the final window to `[MIN_ARCHIVE_DATE, end)` and **no-op once
`cursor.next_end_date <= MIN_ARCHIVE_DATE`** (the guard is essential — a cursor
already below the floor would plan an *inverted* window). No spatial axis here;
it simply terminates. Prevents the forthcoming raw-PV orphaning. Land before
~late Sep 2026.

### R1b — Weather-grid backfill: 2-D densifying cursor with a cap (chosen)

Today the WGB cursor is one-dimensional: the sample file is a side effect of how
far back it has marched (`sample_index = (anchor − sample_offset) % 32`, offset
ever-growing), so it runs off the archive bottom instead of building spatial
density, and the historical grid sits permanently at ~1 sample file per 14-day
window (~3% of the full 32-sample grid). The "triangle of blocks" densification
sketched in the original design (`.tmp/archived-convos/2026-04-03.txt`) was
**deferred and never built**. Implement it, specialised to the fixed floor:

- Split the cursor into `(time_position, density_pass)`. **Time is bounded to
  `[MIN_ARCHIVE_DATE, today]`** (fast, inner axis); `density_pass` is the slow,
  outer axis.
- On reaching the floor: `density_pass += 1`, reset time to today, and **shift
  each window's sample assignment by +1** so pass *k* gives window *w* sample
  `(base + k + w) % 32`. After *k* passes each window holds *k* distinct sample
  files. Make Step-3 sample choice a pure function of `(pass, window)` (drop the
  daily `anchor` for Step 3; Step 2 keeps it).
- **Stop** starting passes at `target_passes = ceil(f × 32)` — a "max relative
  spatial density" `f ∈ (0, 1]`. After the cap, Step 3 goes quiescent; Step 2's
  rolling window continues.

Downstream is unchanged: each `(sample_index, window)` writes its own prepared
partition (`weather_{start}_{end}_{ver}-{NN}.csv`), so extra passes *accumulate*
files and the transform backfill plans them from new `completed` descriptors
automatically. No transform-side change expected — verify.

**Open parameter — `target_density` (`f`).** The only undetermined value. Cost is
~34 daily runs per full pass (`f = 1/32`): 4 passes ≈ 12.5% ≈ ~4.5 months; 8 ≈
25% ≈ ~9 months; 32 = 100% ≈ ~3 years. Default suggestion **4 passes (12.5%)** —
a clear step up from the current single pass; raise later without rework
(densification is monotonic). Confirm against the grid-weather model's actual
resolution needs.

**Migration.** Reinterpret the live WGB cursor (`2010-05-21`, offset 417) as "one
partial pass already laid over [2016, today]" and reset it to
`(time_position=today, density_pass=1)` to begin the first *densifying* pass. This
also immediately stops the daily 400 storm.

### R4 — Documentation

Document the as-built Step-2 / Step-3 staircase, the deliberate density trade-off,
the new 2-D densifying cursor, and the chosen `target_density` in the extraction
package README and/or `doc/orchestration.md`. Do **not** document the archived
"triangle of blocks" as current — it never existed in the code. `orchestration.md`
today covers the mechanics but omits the density story entirely.

## Sequencing / urgency

- **Interim:** until R1b ships the WGB burns ~8.5k failed fetches/day. Resetting
  the live WGB cursor to `(today, pass=1)` stops it immediately — a production
  state change, do only with the owner's go-ahead.
- **R1a deadline:** land before ~late Sep 2026 to avoid orphaned raw PV. Raw now
  lives durably in `pv-prospect-raw` (see `reports/archive-raw-data.md`), so an
  orphan here is a wasted-quota/efficiency issue only, not a data-loss risk.
- **R3 (optional cleanup):** if R1a slips and orphaned pre-2016 raw PV appears,
  delete it under `gs://pv-prospect-raw/timeseries/pvoutput/**` (report §6 R3).

## Tests

Unit-test the schedule as a pure function: floor clamp + inverted-window guard
(R1a); the 2-D `(time_position, density_pass)` advance, the floor→next-pass reset,
the `(pass, window)` sample assignment giving distinct files per pass, and the
`target_passes` terminal (R1b). Cursor serialisation round-trips the new 2-D shape.
