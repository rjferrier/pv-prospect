# Report: Diagnose the PV yield overestimate (formerly: align OpenMeteo vintage)

> This task began as *"Align OpenMeteo vintage between prepared-weather and prepared-PV
> corpora"* — the instinct that a weather/PV corpus vintage mismatch caused a ~30 %
> yield **under**-estimate. Measuring it end-to-end overturned both the sign and the
> cause. The work was investigation, not implementation; this report records what was
> found. The fix space is in the follow-up task: **`briefs/pv-yield-overestimate.md`**
> / **`plans/pv-yield-overestimate.md`**.

## Contents

1. Context & background
2. Hypotheses
3. Method
4. Results
5. Conclusions
- Appendix A — superseded POA-hop attribution (historical)

---

## 1. Context & background

### The original symptom

A POA-space comparison at site 89665 (Feb–May 2025/2026) showed model-reconstructed
plane-of-array irradiance running 20–38 % below the prepared-PV corpus POA
(MAPE 32.7 %). The brief *asserted* this propagated to a ~30 % **under**-estimate of
annual yield. **That yield figure had never actually been measured** — measuring it
is what this investigation set out to do.

### The vintage drift is real (confirmed in code)

Two *separate* OpenMeteo extractions feed the two corpora, and they never share a
vintage:

| Corpus | Built by | Weather coords | Schedule / cursor |
|---|---|---|---|
| `prepared/weather/` | `produce_weather_slice` (`slice_producer.py:87`) | 0.2° **grid points** | weather-grid backfill, own cursor (~03:20 UTC) |
| `prepared/pv/` POA | `produce_pv_slice` (`slice_producer.py:187`) / `run_prepare_pv` (`core.py:253`) | each site's **exact** coords | PV-sites backfill, separate cursor (`_weather_task_env`, `pv_backfill.py:66`) |

For any historical date D the two cursors reach D on *different calendar days*, so
OpenMeteo (which continuously re-assimilates recent history) returns a *different
reanalysis vintage* for the same date/area. The brief's literal prescription —
"ensure `prepare_pv` reads the weather from the *same OpenMeteo call* as the weather
partitions" — is therefore **structurally impossible** while the two are different
coordinates from different jobs. The only genuine ways to share a vintage are to
**unify the weather source** (Option A, plan §fix-space) or to **stamp + gate** the
vintage (Option B). The drift is real; the question Gate B answers is *how much it
costs in yield* — which turns out to be little.

### The confound that forces yield-space measurement

POA-space MAPE cannot be read as yield error, because of a **daytime-vs-24h
aggregation** artifact. PVOutput has no night rows, so `prepare_pv`'s inner join
restricts each daily POA/temperature/power to **daytime** hours, while
`prepare_weather` and `reconstruct_daily_mean_poa` average over the full 24 h. The
corpus daily POA is therefore daytime-weighted and inflated (89665/06-09: 270 vs a
24 h mean of 214 W/m², **+26 %**), worst on the first day of each 2-day window. As
Hypothesis (a) shows, this *cancels* in yield — so the decision metric must be
**predicted-vs-actual annual kWh**, not a POA hop.

## 2. Hypotheses

Three candidate causes for an end-to-end yield error:

- **(a) Daytime-vs-24h aggregation** — the +26 % daytime-weighting above leaks into
  yield.
- **(b) Cross-vintage / spatial drift** (the *weather path*) — the off-site grid
  weather and the different-vintage corpus mis-state the irradiance the PV model is
  fed. This is what the brief's Option A would fix.
- **(c) PV-model low-POA miscalibration** (the *PV-intrinsic path*) — the PV model
  is *served* a POA basis (24 h-mean) different from the one it *trained* on
  (daytime-mean), landing it in a sparsely-trained region.

These are not a menu to pick from a priori; they are discriminated by **two gates**:

- **Gate A — is there a yield bias at all, and in which direction?** The sign is
  genuinely unknown going in: (a) provably *cancels* for a linear chain, but the real
  PV model is a nonlinear clipped MLP and the temperature feature does not cancel, so
  the residual could be ≈ 0, negative, or positive.
- **Gate B — if a bias exists, whose is it?** Attribute the measured ratio between
  the weather path (b) and the PV-model intrinsic (c).

## 3. Method

### Gate A — yield truth

`pv-prospect-app/scripts/measure_yield.py` runs the real `/predict` chain
end-to-end (weather model → reconstruct POA → PV model → clamp → energy) for each
site and compares to its **true** annual generation. Two prerequisites had to be
closed first:

1. **Model store** — loaded from `gs://pv-prospect-versioned-model` via `--store-dir`.
2. **True actuals** — true generation lives **only in raw PVOutput's `energy`
   column** (cumulative Wh/day; the day's max is its total — e.g. 89665/06-09 ends at
   35519 Wh = 35.5 kWh). Cleaned/prepared corpora drop night rows and strip `energy`,
   so they cannot yield true kWh. The script reads actuals straight from the raw
   per-day files via `--actuals-gcs-prefix` (daily `energy` max → annual sum).
   Coverage was complete: 365/365 in-window daily files per site checked.

Command (exact flags):

```bash
cd pv-prospect-app && poetry run python scripts/measure_yield.py \
  --store-dir gs://pv-prospect-versioned-model \
  --pv-sites-csv <pv_sites.csv> \
  --actuals-gcs-prefix gs://pv-prospect-staging/data/raw/timeseries/pvoutput/ \
  --start 2025-06-09 --end 2026-06-08
```

Caveats: single-year actuals are weather-noisy — trust the cross-site aggregate, not
any one site. Multi-panel sites use a split-and-sum POA approximation that diverges
slightly from the corpus's area-weighted-POA-then-model path for a nonlinear MLP (a
minor per-site magnitude caveat, not an aggregate one).

### Gate B — energy-space attribution

Gate B was run as an **energy-space decomposition**, *not* the POA-hop sketch the
plan originally carried (Appendix A), because the POA-hop is confounded by the +26 %
aggregation. Script:
`data-exploration/irradiance/poa_attribution/gate_b_decompose.py`, same window and
store as Gate A. The measured ratio is split multiplicatively:

```
E_pred / E_true  =  (E_pred / E_truePOA)  ×  (E_truePOA / E_true)
                       weather-path            PV-intrinsic
```

- **E_pred** — the real `/predict` chain; == Gate A's predicted.
- **E_truePOA** — the *same* PV/clamp/×24 path, but fed the **true monthly-mean
  24 h POA & temperature** (raw per-site `best_match` weather pushed through the same
  `pv_prospect.physics` POA math the corpus uses) instead of the
  weather-model+reconstruct inputs. So it differs from E_pred *only* in the
  POA/temperature inputs.
- **E_true** — raw PVOutput actual annual kWh; == Gate A's actual.

A **correctness gate** is built in: the replicated served path is checked against the
real `predict_yield` per site (must agree < 0.5 %), which validates that E_truePOA
isolates exactly the POA/temperature swap.

### Weather-path de-confounding

`gate_b_wxcheck.py` (June, the peak-energy month; 8/10 sites — 2 skipped on a flaky
elevation API) probes the weather path directly — served vs true DNI/DHI/POA/
temperature — to separate an irradiance effect from a temperature effect within the
~8 % weather rider.

## 4. Results

### Gate A — a real bias, and it is an OVERESTIMATE

Against `model-v2026-06-10` (R²=0.86) vs raw PVOutput actuals, all 10 sites,
2025-06-09 → 2026-06-08:

| site  | predicted kWh | actual kWh | pred/actual |
|------:|------:|------:|------:|
| 4708  |  6256 |  3245 | 1.928 |
| 24667 |  3469 |  1633 | 2.124 |
| 25724 |  7599 |  3660 | 2.076 |
| 36019 |  8864 |  4383 | 2.022 |
| 42248 |  7627 |  4063 | 1.877 |
| 56874 |  8346 |  5047 | 1.654 |
| 61272 |  7543 |  2469 | 3.055 |
| 79336 |  7579 |  4586 | 1.653 |
| 82517 | 17863 |  7918 | 2.256 |
| 89665 | 11907 |  8144 | 1.462 |
| **mean** | | | **2.011** |
| **median** | | | **1.975** |

**Outcome: predict ≠ actual — a +101 % OVERESTIMATE, opposite in sign to the
documented "~30 % underestimate".** The ~30 % claim (brief, and `_VINTAGE_CAVEAT` in
`main.py`) was wrong in both magnitude and direction; both have been updated.

### Hypothesis (a) — aggregation — TESTED AND RULED OUT

`prepare_pv` inner-joins PV power onto weather hours, so corpus POA and corpus power
are averaged over the *same* daytime rows. The daytime inflation sits in both the
numerator and denominator of CF = power/capacity, so the model learns the true
*hourly* CF–POA slope. At serving, `reconstruct_daily_mean_poa` feeds the 24 h-mean
POA and the chain multiplies power by 24; for a through-origin linear model that
recovers true daily energy exactly (demonstrated on 89665/06-09: 35.52 kWh =
35.52 kWh). So the aggregation convention **cannot** produce a 2× bias on its own.
The temperature-feature non-cancellation is real but second-order — it cannot account
for a factor of two. *This negative result is why the investigation is yield-space,
not POA-space.*

### Root cause indicated — the PV model over-predicts CF where it is served

Re-probing the PV model's CF-vs-POA response (fixed day-of-year/temperature, sweeping
POA) shows a **concave, miscalibrated** curve — not a simple intercept:

| POA (W/m²) | CF_model / CF_physics (≈ POA/1000) |
|---:|:---|
| 0       | intercept ≈ **0.045–0.056** (should be 0) |
| 25–150  | **1.5–2.8× too high** |
| 300–400 | ≈ 1.0 (about right) |
| 600–800 | 0.6–0.9 (too low) |

Mechanism = **train/serve POA-basis mismatch**: the model trains on daytime-mean POA
(rarely below ~100 W/m²) but is served the 24 h-mean POA (~80–150 for the UK; night
zeros pull it down), so serving evaluates it in its sparsely-trained,
badly-extrapolated low-POA region, where it over-predicts CF by 1.4–2.8×. Plugging
realistic UK 24 h-mean POA into the measured curve reproduces `E_pred/E_true ≈
1.3–2.8` from the **curve shape alone**. Independently, the model predicts CF ≈
30–40 % above corpus CF even given exact corpus features (POA ≈ 200) — consistent
with the curve being ~1.4× at mid-POA — so the bias is in the model, not only in the
inference-time POA hop.

### Gate B — the overestimate is ~89 % PV-model, ~11 % weather-path

| site | E_pred | E_truePOA | E_true | weather-path | PV-intrinsic | product |
|------:|------:|------:|------:|------:|------:|------:|
| 4708  |  6256 |  6073 |  3245 | 1.030 | 1.871 | 1.928 |
| 24667 |  3469 |  3081 |  1633 | 1.126 | 1.887 | 2.124 |
| 25724 |  7599 |  6690 |  3660 | 1.136 | 1.828 | 2.076 |
| 36019 |  8864 |  8205 |  4383 | 1.080 | 1.872 | 2.022 |
| 42248 |  7627 |  6968 |  4063 | 1.095 | 1.715 | 1.877 |
| 56874 |  8346 |  8117 |  5047 | 1.028 | 1.608 | 1.654 |
| 61272 |  7543 |  6631 |  2469 | 1.137 | 2.686 | 3.055 |
| 79336 |  7579 |  7220 |  4586 | 1.050 | 1.574 | 1.653 |
| 82517 | 17863 | 16552 |  7918 | 1.079 | 2.091 | 2.256 |
| 89665 | 11907 | 11400 |  8144 | 1.044 | 1.400 | 1.462 |
| **geomean** | | | | **1.080** | **1.826** | **1.972** |

Even fed the **true** site POA, the PV model still over-predicts real generation by
~83 % (PV-intrinsic geomean **1.826**, range 1.40–2.69) — the concave low-POA
miscalibration plus the Jensen gap of pushing a monthly-mean POA through a nonlinear
MLP. The weather path is a small, same-signed ~8 % rider (geomean **1.080**, range
1.03–1.14). In log terms the split is `ln 1.826 / ln 1.972 = 0.89` → **~89 %
PV-model, ~11 % weather-path**. The clean, fundamental number is the **PV-intrinsic
1.826, measured at the true inputs**; the per-site PV-intrinsic range 1.40–2.69 *is*
the per-site overprediction.

### What the weather-path actually is (de-confounded)

It is **not** a POA over-prediction. In June the weather model serves DNI ~7 %
*low*, POA ~4 % *low* (geomean served/true 0.957), and temperature **~2.4 °C cooler**
than the actual June-2025 24 h-mean (cooler at every site). Because the served POA is
*low* in June — which on its own *lowers* yield — June's positive contribution is
**necessarily the temperature gap** (a cooler served temperature lifts the PV model's
CF), outweighing the POA deficit. June dominates annual (summer-heavy UK) yield, so
the annual +8 % is **temperature-led** — but the winter months' POA signs were
**not** separately verified, so this is a **June-anchored lean, not a
month-by-month proof**. The temperature gap is also partly year-specific (June 2025
ran warm vs the model's climatological mean), so this rider is **partly weather
noise, not pure model error**. This *corroborates* the brief's "weather
under-predicts irradiance" instinct for DNI/POA in June — it simply does not cause an
under-estimate, because both the PV over-prediction and the temperature serving gap
push yield up.

### Integrity checks

1. All 10 per-site `product` values reproduce the Gate A table exactly, and the
   correctness gate (replicated served path vs real `predict_yield`) passed for every
   site (< 0.5 %) — so the split is trustworthy, not noise.
2. The true-POA leg (which underpins *both* factors) is anchored to gold_check: the
   harness's true 24 h-mean POA is 201 W/m² for 89665 June and 188–232 across sites —
   in 24 h-mean territory (gold_check ~214) and well below the corpus daytime-weighted
   ~270, confirming E_truePOA is on the true 24 h basis.

## 5. Conclusions

- **What it is:** a PV-model calibration failure. The model is trained on
  daytime-mean POA and served 24 h-mean POA; the served operating range falls in its
  under-trained, over-predicting low-POA region. This accounts for ~83 %
  over-prediction (geomean 1.826) **even at the true site POA**, i.e. independent of
  any weather error. Root cause is the **train/serve POA basis** — which reconnects to
  this brief's alignment theme in corrected form: it is a *basis* alignment, not a
  *vintage* alignment.
- **What it isn't:** *not* the daytime-vs-24h aggregation (it cancels — ruled out);
  *not* primarily the vintage/grid drift (the weather path is only ~8 % and is
  *same-signed*, so fixing it can only shrink the gap, never have opened it); *not* a
  simple CF intercept (the intercept ~0.05 is real but small).

### What we got wrong en route (load-bearing — do not re-introduce)

Two interim drafts inferred a mechanism from the *net* and got it backwards. Both
corrections are recorded so a future reader does not reintroduce them:

1. **"Positive CF intercept."** An early draft blamed a single positive CF intercept.
   The intercept (~0.05) is real but small; the dominant effect is the **concave
   over-prediction across the low-POA range**. Zero-forcing the intercept alone would
   **not** fix it.
2. **"Weather over-predicts POA."** A draft stated the weather model serves POA *too
   high*, opposite the brief. The direct probe shows the reverse — POA is served
   *low*; the small positive weather-path comes from the **temperature** serving gap,
   not POA. The net split and the PV-model conclusion are unchanged.

The meta-lesson: **measure the components; don't infer the mechanism from the net.**

### Follow-up

The fix space is documented in **`briefs/pv-yield-overestimate.md`** /
**`plans/pv-yield-overestimate.md`**. The primary fix (retrain on the 24 h-mean POA
basis) has since been executed — see **`reports/pv-train-on-served-poa.md`** — and
reduced the overestimate from 2.011 to 1.515, hitting a structural Jensen ceiling.
The remaining bias requires the serve-side lever.

---

## Appendix A — superseded POA-hop attribution (historical)

Gate B was *originally* sketched as a POA-hop split; references to "Hop 1 / Hop 2"
(and the "If Gate B → Hop 2, Option A is recommended" conditional that previously
headed the fix menu) come from it. It was **superseded** by the energy-space
decomposition (§3) because it is confounded by the +26 % aggregation. Recorded only to
resolve those references:

- **Hop 1 — weather-model fit:** weather-model DNI/DHI vs the weather corpus it
  trained on. Independent of vintage; Option A does **not** fix it.
- **Hop 2 — cross-vintage / spatial drift:** weather corpus vs PV corpus. The only hop
  Option A removes.

The original conditional read "Hop 2 dominant → Option A recommended." Gate B did
**not** come out Hop-2-dominant — the whole weather path (Hop 1 ⊕ Hop 2 ⊕
aggregation) is only ~8 % — so that conditional never fired, and Option A is **not**
the recommended primary fix. This appendix exists because that stale "(recommended)"
label caused exactly that misreading.