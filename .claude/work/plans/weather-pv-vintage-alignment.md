# Plan: Align OpenMeteo vintage between prepared-weather and prepared-PV corpora

> Companion to `briefs/weather-pv-vintage-alignment.md`. The brief states the
> original symptom (a POA-space MAPE of 32.7 %, asserted to be a ~30 % yield
> underestimate). **That yield figure has never actually been measured** — this
> plan resequences the work around measuring it first. It touches the website's
> W1 public-launch gate (currently "fix-first"), but whether W1 is really blocked
> is itself one of the things Gate A decides.

## Where to start — the gated sequence (read this first)

The decision-critical unknown is **not** which fix (Option A/B/C) to apply. It
is whether the deployed `/predict` chain actually mis-estimates real annual
generation — and in which direction. Every fix below is a candidate whose
justification depends on that answer, so the work is **two gates, not a menu**.

### Gate A — yield truth (do first; decides whether there is a problem at all)

Run `pv-prospect-app/scripts/measure_yield.py`: the real `/predict` chain
end-to-end (weather model → POA → PV model → energy) vs each site's **true**
annual generation. This single number has never been measured; the documented
"~30 %" was a POA-space MAPE *asserted* to propagate to yield, not a
predicted-vs-actual kWh.

**The outcome is genuinely unknown, including its sign — do not assume it.**
The +26 % daytime-vs-24h aggregation (see *Background* below) provably cancels
for a *linear* CF–POA chain — with a perfect model, `energy = power × 24`
recovers the truth (demonstrated on 89665/06-09: 35.52 kWh = 35.52 kWh). But the
real PV model is a **nonlinear, clipped MLP**, and the **temperature feature
does not cancel** (the daytime-mean temperature the corpus trains on is not the
24h-mean temperature, and it does not scale by the POA factor). So the real
residual could be ≈0, an under-estimate, *or an over-estimate*. The whole point
of Gate A is that the cancellation argument cannot predict it — measure it.

- **predict ≈ actual** → no material yield bias. The ~30 % was a POA-space
  artifact; correct the caveat and unblock W1 (see *Cleanup*). Option A/B
  re-backfill is then **not** justified on yield grounds — only, if at all, on
  the train/serve-consistency argument, which stands on its own.
- **predict materially off** → a real bias exists; go to Gate B to attribute it.

**Gate A is not runnable as-is — fixing that is the literal next action** (see
*Running Gate A*).

### Gate B — attribution (only if Gate A shows a real bias)

The POA-hop decomposition (this was the old "Phase 0"; full method below). Split
the measured gap into its two independent hops:
- **Hop 1 — weather-model irradiance error** (predicted DNI/DHI vs the corpus it
  trained on). Option A does **not** fix this.
- **Hop 2 — cross-vintage / spatial drift** (PV corpus vs weather corpus). The
  only hop the vintage fix removes.

Runnable now on already-pulled prepared data, **but confounded by the +26 %
aggregation** — read it as attribution colour, not a clean gate.
- Hop 1 dominant → weather-model work / keep caveat; re-sequence before W1.
- Hop 2 dominant → Option A (fold the 24h-convention hygiene in) + Option C gate.

### Contingent — do not action until the gates resolve

The "~30 % underestimate / fix-first / W1-blocked" language in `TODO.md`,
`app/poa.py`, and `main.py:35` is **downstream of Gate A**; leave it until
measured. The 24h-convention hygiene item is yield-neutral by construction —
fold it into any Option-A re-backfill; don't do it standalone for the ~30 %.

## Running Gate A (prerequisites + the next action)

Gate A cannot run today, and `measure_yield.py` had a correctness gap that the
prep must close:

1. **Model store** — `models/` is empty. Pull the trained artifacts
   (`data-v2026-05-31`, named in `main.py`'s caveat) from
   `gs://pv-prospect-versioned-model`, or point `--store-dir` at them.
2. **True actuals — the long pole.** True generation lives **only in raw
   PVOutput's `energy` column** (cumulative Wh within a day; the day's last
   reading is its total — e.g. 89665/06-09 ends at 35519 Wh = 35.5 kWh).
   Cleaned/prepared corpora **drop night rows and strip `energy`** (cleaned is
   just `time,power`), so they cannot yield true kWh. `measure_yield.py` now
   reads actuals from raw `energy` (daily max → annual sum); it previously
   integrated cleaned power, which cannot see the truth. A year of raw PVOutput
   across the validation sites is needed (`gs://pv-prospect-versioned-raw`); the
   user hand-fetched a single site-day into `.tmp`, so acquiring this corpus is
   the gating step.
3. **Run** — `cd pv-prospect-app && poetry run python scripts/measure_yield.py
   --store-dir <store> --pv-sites-csv resources/pv_sites.csv
   --actuals-dir <raw-pvoutput-dir> --start <D0> --end <D1>`. Single-year
   actuals are weather-noisy; trust the cross-site aggregate, not any one site.

**Open practical question for the user:** how to source a year of raw PVOutput
actuals across sites — `dvc pull` of `versioned-raw`, a fresh extraction run, or
PVOutput's own daily-energy export? This choice gates Gate A.

## Background — the aggregation that confounds POA-space

Model-free investigation with real 89665 data (2026-06-09; scripts in
`pv-prospect-instance/data-exploration/irradiance/poa_attribution/`). PVOutput
has no night rows, so `prepare_pv`'s inner join restricts each daily
POA/temperature/power to **daytime** hours, while `prepare_weather` (and
`reconstruct_daily_mean_poa`) average over the full 24 h. The corpus daily POA
is therefore **daytime-weighted and inflated** (89665/06-09: 270 vs a 24 h mean
of 214 W/m², +26 %), worst on the **first day of each 2-day window**. This is a
*third* component the Hop-1/Hop-2 split does not separate: much of "Hop 2" is
this aggregation, not cross-vintage drift — which is exactly why Gate B's
POA-space numbers are confounded, and why the decision metric is Gate A's yield,
not a POA hop.

## Root cause of the vintage drift (confirmed in code)

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

## Gate B method — decompose the gap (POA-hop; only if Gate A shows a bias)

The end-to-end error is the sum of two independent hops:

- **Hop 1 — weather-model fit**: weather-model DNI/DHI vs the weather corpus it
  trained on. `pv-prospect-model/README.md` already flags DNI/DHI as "near the
  noise floor … does not beat IDW". Potentially large and **independent of
  vintage**.
- **Hop 2 — cross-vintage/spatial drift**: weather corpus vs PV corpus. This is
  the only hop the vintage fix removes.

Option A drives **Hop 2 → ~0** but does **nothing for Hop 1**. Method (uses only
already-pulled corpus data — runnable now, but read with the +26 % aggregation
confound from *Background* in mind):

1. For site 89665's nearest grid point, compute `POA_wxcorpus` by pushing the
   **weather-corpus** DNI/DHI through the same POA math (`prepare_pv`'s
   `_calculate_poa_irradiance`; the corpus carries
   `direct_normal_irradiance`/`diffuse_radiation` per `WEATHER_COLUMNS`).
2. **Hop 2** = MAPE(`POA_wxcorpus` vs corpus PV POA), per month (Feb/Oct/May).
3. **Hop 1** = MAPE(reconstructed POA vs `POA_wxcorpus`).
4. Check Hop 1 ⊕ Hop 2 reconcile to the brief table's 32.7 %.

**Attribution → fix:**
- Hop 2 dominant → Option A is justified; proceed to the fix menu.
- Hop 1 dominant → Option A is **not sufficient**; say so plainly and re-sequence
  (weather-model improvement, or keep the documented caveat) ahead of W1 rather
  than spending the re-backfill first.

(The supporting weather-model MAPE that the now-deleted `productionise-models`
plan once held is *not* in README/doc — Gate B regenerates it.)

## Fix menu (contingent on the gates)

### If Gate B → Hop 2: Option A — one weather source feeds both corpora (recommended)

Make the PV corpus's POA derive from the **grid weather corpus** (nearest grid
point), eliminating the separate on-site weather extraction.

**Why A over B:**
- Same vintage **by construction** — one weather source of truth.
- **Train/serve consistency**: at inference the PV model only ever sees
  grid-resolution, *model-predicted* weather (the weather net interpolates the
  grid climatology to the site — `chain.py:108`). Training its POA from grid
  weather matches that; training from exact-site weather is precisely the
  train/serve skew that B preserves. **This rationale is independent of the
  yield/vintage question** — it holds even if Gate A shows no bias.
- Removes a whole extraction job, its OpenMeteo budget, and a cursor.
- One modeling cost: grid-approximated POA paired with power generated under
  exact-site irradiance (mild label noise). The right trade, given the above —
  0.2° ≈ ≤ 11 km, far below the vintage effect Gate B is measuring.

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

### Regardless of A/B: Option C — trainer validation gate (complementary)

After training, before promotion: compute the weather model's DNI/DHI MAPE vs
the corpus on held-out (location, month) cells; log it as a metric; **block
promotion above a configured threshold** (e.g. 20 %). This does not fix Hop 1 but
makes it *visible* — a silently-degraded weather model fails the gate instead of
shipping. Do this regardless of the A/B choice.

### 24h-convention hygiene (yield-neutral; fold into any re-backfill)

Decouple `prepare_pv`'s POA/temperature from PV-power coverage (compute over the
full 24h weather day; energy-based target). Removes the daytime-weighting
footgun, the edge/interior corpus inconsistency, and the temperature-feature
train/serve mismatch (PV trains on daytime-warm temp, serves ~24h). Needs a
PV-corpus re-transform + PV-model retrain (weather untouched). It will **not**
move yields where the chain is linear (it cancels) — but note that the
temperature non-cancellation (Gate A, above) is a real reason to do it for
correctness, not just tidiness. Also fix the now-false "<1 % self-consistent"
note in `app/poa.py`.

### Cleanup + unblock W1 (contingent on Gate A)

Once Gate A is resolved and (if a fix was needed) the smoke-test MAPE < 15 %:
- Correct/remove the ~30 % caveat in `pv-prospect-app/.../poa.py` docstring and
  the `/predict` response caveat in `main.py:35`.
- Flip the TODO note and unblock the website's W1 public launch.

### Alternative — Option B (fallback, only if exact-site fidelity proves material)

Keep on-site weather but force same vintage: capture OpenMeteo `generation_time`
(not currently captured — add to `_metadata_from_response`, `openmeteo.py:325`),
stamp it into the raw metadata sidecar, and have `prepare_pv` reject/skip a day
whose on-site-weather vintage differs from the grid-weather vintage for that date
by more than one week. **Partial mitigation**: keeps two jobs, can leave gaps
where vintages cannot be reconciled, preserves exact-site POA but *perpetuates*
the train/serve skew A removes. Pick only if Gate B shows A's grid approximation
is itself material (unlikely).

## Sequencing

```
Gate A — measure_yield.py (prereq: store + raw actuals) ──▶ decision
   ├─ predict ≈ actual ──▶ no yield bias: correct caveat, unblock W1.
   │                        Option A only on the train/serve argument, if at all.
   └─ predict materially off ──▶ Gate B (POA-hop attribution, confound-aware)
         ├─ Hop 2 dominant ──▶ Option A (+ fold 24h hygiene) + Option C gate
         │                        └─▶ Cleanup, unblock W1
         └─ Hop 1 dominant ──▶ A insufficient; weather-model work / keep caveat
                                before W1
```
