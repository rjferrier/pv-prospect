# Plan: Align OpenMeteo vintage between prepared-weather and prepared-PV corpora

> Companion to `briefs/weather-pv-vintage-alignment.md`. The brief states the
> symptom (MAPE 32.7 % end-to-end, ~30 % yield underestimate); this plan pins
> the root cause and sequences the fix. **Hard prerequisite for the website's
> W1 public launch** (decision: fix-first).

## Update — yield-space evidence (revises the diagnosis and Phase 0)

A model-free investigation with real PV-side data for 89665 (on-site cleaned
weather + PVOutput, 2026-06-09; scripts in
`pv-prospect-instance/data-exploration/irradiance/poa_attribution/`) adds two
facts the Hop-1/Hop-2 framing below does not capture:

1. **A third component the split misses — daytime-vs-24h aggregation.** PVOutput
   has no night rows, so `prepare_pv`'s inner join restricts the daily
   POA/temperature/power to *daytime* hours. The corpus daily POA is therefore
   **daytime-weighted** (89665/06-09: 270 vs a 24h mean of 214 W/m², +26 %),
   worst on the **first day of each 2-day window**; `reconstruct_daily_mean_poa`
   is 24h. So much of "Hop 2" (corpus-PV-POA vs weather-corpus POA) is this
   aggregation, **not** cross-vintage drift — and the Phase-0 POA-space MAPE
   below cannot separate the two.
2. **It cancels in yield.** POA and the CF target inflate by the same daily
   factor, so a ~linear CF–POA model learns the right slope and `energy =
   power × 24` restores the truth (shown: true daily energy 35.52 kWh = perfect-
   model API 35.52 kWh). So the POA-space MAPE **overstates** the yield error,
   and the documented ~30 % was never measured as predicted-vs-actual annual kWh.

**Revised gate — measure in YIELD space, not POA hops.** Run
`pv-prospect-app/scripts/measure_yield.py` (`/predict` end-to-end vs each site's
*true* annual generation) **first**:
- predict ≈ actual → no material yield bias; the ~30 % was a POA-space artifact;
  Option A re-backfill will not move yield (justify it, if at all, on the
  train/serve-consistency argument below — not vintage).
- predict materially low → it is weather-model irradiance (Hop 1), which Option A
  does **not** fix — exactly the plan's "Hop 1 dominant" branch.

The Phase-0 POA-hop measurement is still useful for *attribution*, but it is
confounded by (1); treat it as diagnostic colour, with yield as the decision
metric. **Option A's train/serve-consistency rationale stands on its own**
(a spatial-source argument, independent of (1)/(2)) — gate it on yield too.

**Separate hygiene item (yield-neutral) — the 24h convention.** Decouple
`prepare_pv`'s POA/temperature from PV-power coverage (compute over the full 24h
weather day; energy-based target). Removes the daytime-weighting footgun, the
edge/interior corpus inconsistency, and the temperature-feature train/serve
mismatch (PV trains on daytime-warm temp, serves ~24h). Needs a PV-corpus
re-transform + PV-model retrain (weather untouched) — **fold into any Option-A
re-backfill** rather than doing standalone. It will *not* move yields (it
cancels); do it for correctness, not the ~30 %. Also fix the now-false "<1 %
self-consistent" note in `app/poa.py`.

## ⏳ Open decision (pending — do not start Phase 1 until resolved)

The fix *mechanism* is not yet chosen. Options, in recommended order:

1. **Phase 0 first, lean A** *(recommended)* — run the Hop-1/Hop-2 measurement
   below, then proceed with Option A (unify the weather source) **if Hop 2
   dominates**. De-risks the re-backfill; matches train/serve.
2. **Commit to A now** — skip the measurement gate, go straight to unifying the
   source + re-backfill + retrain. Faster to start, but risks a re-backfill that
   never clears the smoke test if Hop 1 dominates.
3. **Option B instead** — keep on-site weather; stamp OpenMeteo `generation_time`
   and reject vintage-mismatched days. Preserves exact-site POA but perpetuates
   train/serve skew and can leave gaps.

**Also undecided:** whether to start Phase 0 now (needs prepared-corpus access —
DVC pull / GCS / local sample) or leave this plan as a record for later.

Until this is resolved the plan stands as analysis only; no code/infra changes
have been made. The documented ~30 % caveat (poa.py / main.py) remains in place
and W1's public launch stays blocked.

## Root cause (confirmed in code)

Two *separate* OpenMeteo extractions feed the two corpora, and they never share
a vintage:

| Corpus | Built by | Weather coords | Schedule / cursor |
|---|---|---|---|
| `prepared/weather/` | `produce_weather_slice` (`slice_producer.py:87`) | 0.2° **grid points** | weather-grid extraction backfill, own cursor (~03:20 UTC) |
| `prepared/pv/` POA | `produce_pv_slice` (`slice_producer.py:187`) / `run_prepare_pv` (`core.py:253`) | each site's **exact** coords | PV-sites extraction backfill, separate cursor (`_weather_task_env`, `pv_backfill.py:66`) |

`produce_pv_slice` reads the on-site weather raw file and computes POA from it;
`produce_weather_slice` reads grid raw files. For any historical date D the two
cursors reach D on *different calendar days*, so OpenMeteo returns a *different
reanalysis vintage* for the same date/area. OpenMeteo continuously re-assimilates
recent history, so the PV corpus's POA drifts from the weather corpus's
irradiance for the same date.

**The brief's literal prescription is structurally impossible.** "Ensure
`prepare_pv` reads the weather from the *same OpenMeteo call* as the weather
partitions" cannot hold while the two are *different coordinates from different
jobs*. The only ways to genuinely share a vintage are to **unify the weather
source** (Option A) or to keep two sources but **stamp + gate** the vintage
(Option B).

## Phase 0 — decompose the gap before paying for a re-backfill (decision gate)

The end-to-end error is the sum of two independent hops:

- **Hop 1 — weather-model fit**: weather-model DNI/DHI vs the weather corpus it
  trained on. `pv-prospect-model/README.md` already flags DNI/DHI as "near the
  noise floor … does not beat IDW". Potentially large and **independent of
  vintage**.
- **Hop 2 — cross-vintage/spatial drift**: weather corpus vs PV corpus. This is
  the only hop the vintage fix removes.

Option A drives **Hop 2 → ~0** but does **nothing for Hop 1**. If Hop 1 alone
exceeds the ~15 % smoke-test threshold, the test still fails and W1 stays blocked
— *after* a multi-day re-backfill + retrain. So measure first; it is cheap and
uses only existing corpus data:

1. For site 89665's nearest grid point, compute `POA_wxcorpus` by pushing the
   **weather-corpus** DNI/DHI through the same POA math (`prepare_pv`'s
   `_calculate_poa_irradiance`; the corpus carries
   `direct_normal_irradiance`/`diffuse_radiation` per `WEATHER_COLUMNS`).
2. **Hop 2** = MAPE(`POA_wxcorpus` vs corpus PV POA), per month (Feb/Oct/May).
3. **Hop 1** = MAPE(reconstructed POA vs `POA_wxcorpus`).
4. Check Hop 1 ⊕ Hop 2 reconcile to the table's 32.7 %.

**Decision:**
- Hop 2 dominant → Option A is justified; proceed.
- Hop 1 dominant → Option A is still correct but **not sufficient**; say so
  plainly and re-sequence (weather-model improvement, or keep the documented
  caveat) ahead of W1 rather than spending the re-backfill first.

(The supporting weather-model MAPE that the now-deleted `productionise-models`
plan once held is *not* in README/doc — Phase 0 regenerates it.)

## Phase 1 — Primary fix, Option A: one weather source feeds both corpora (recommended)

Make the PV corpus's POA derive from the **grid weather corpus** (nearest grid
point), eliminating the separate on-site weather extraction.

**Why A over B:**
- Same vintage **by construction** — one weather source of truth.
- **Train/serve consistency**: at inference the PV model only ever sees
  grid-resolution, *model-predicted* weather (the weather net interpolates the
  grid climatology to the site — `chain.py:108`). Training its POA from grid
  weather matches that; training from exact-site weather is precisely the
  train/serve skew that B preserves.
- Removes a whole extraction job, its OpenMeteo budget, and a cursor.
- One modeling cost: grid-approximated POA paired with power generated under
  exact-site irradiance (mild label noise). The right trade, given the above —
  0.2° ≈ ≤ 11 km, far below the vintage effect Phase 0 is measuring.

**Cross-component scope** (flagged up front per the consistency rule):
1. **Transformation** — `produce_pv_slice` / `run_prepare_pv` source weather from
   the grid partitions (or grid raw) at the site's nearest grid point, not the
   on-site file. Add a nearest-grid-point lookup (grid is 0.2°; sample files
   enumerate points — `etl/sample_file.py`).
2. **Extraction** — drop the per-slice weather task from the PV-sites backfill
   (`pv_backfill.py` `_weather_task_env` and phase 1 of `build_phases`); the
   daily path likewise stops extracting on-site weather.
3. **Orchestration** — new dependency: PV prepare for date D needs grid weather
   for D present. Today the PV-sites transform and weather-grid transform advance
   on *independent markers*; either gate PV prepare on grid coverage or read grid
   weather from the accumulated prepared corpus.
4. **Re-backfill + retrain** — re-prepare the existing PV corpus from grid
   weather to repair historical drift, then retrain. Prior art:
   `util/pv_prospect/util/backfill_prepared_corpora_20260524.py` (marker rewind +
   supersede old ledgers + trigger workflows).

## Phase 2 — Secondary fix, Option C: trainer validation gate (complementary)

After training, before promotion: compute the weather model's DNI/DHI MAPE vs
the corpus on held-out (location, month) cells; log it as a metric; **block
promotion above a configured threshold** (e.g. 20 %). This does not fix Hop 1 but
makes it *visible* — a silently-degraded weather model fails the gate instead of
shipping. Do this regardless of the A/B choice.

## Phase 3 — Cleanup + unblock W1

Once the smoke-test MAPE < 15 %:
- Remove the ~30 % caveat in `pv-prospect-app/.../poa.py` docstring and the
  `/predict` response caveat in `main.py:35`.
- Flip the TODO note and unblock the website's W1 public launch.

## Alternative — Option B (fallback, only if exact-site fidelity proves material)

Keep on-site weather but force same vintage: capture OpenMeteo `generation_time`
(not currently captured — add to `_metadata_from_response`, `openmeteo.py:325`),
stamp it into the raw metadata sidecar, and have `prepare_pv` reject/skip a day
whose on-site-weather vintage differs from the grid-weather vintage for that date
by more than one week. **Partial mitigation**: keeps two jobs, can leave gaps
where vintages cannot be reconciled, preserves exact-site POA but *perpetuates*
the train/serve skew A removes. Pick only if Phase 0 shows A's grid approximation
is itself material (unlikely).

## Sequencing

```
Phase 0 (measure, ~hours) ──▶ decision
   ├─ Hop 2 dominant ──▶ Phase 1 (A: transform + extraction + orchestration,
   │                              re-backfill, retrain)  +  Phase 2 (C: gate)
   │                          └─▶ Phase 3 (cleanup, unblock W1)
   └─ Hop 1 dominant ──▶ A still correct but insufficient; sequence weather-model
                          work / keep caveat before W1
```
