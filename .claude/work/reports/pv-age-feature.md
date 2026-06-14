# Report: Validate & fix the PV `age_years` feature

> Successor to `reports/pv-train-on-served-poa.md` (which delivered the
> `data-v2026-06-12` corpus re-base and re-attributed the residual yield
> overestimate to this feature). This task is the **W1 public-launch gate** for
> the prediction product. It was specified in `plans/pv-age-feature.md` (deleted
> on finalisation — this report is the permanent home for its Phase 0 rationale)
> and `briefs/pv-age-feature.md`.

## Contents

1. Summary
2. The problem (why `age=0` is the whole game)
3. Phase 0 — is a degradation slope identifiable? (the fork)
4. Phase 1 — the bounded degradation prior, and the gate miss
5. The decision (2026-06-14)
6. Phase 2 — LOSO, the prospect uncertainty band
7. Acceptance vs outcome
8. Remaining operational step (promote + caveat cleanup) — **not yet done**
9. Durable artifacts & pointers

## 1. Summary

The PV model trained `age_years` as a free MLP feature. With only 10 aged sites
(4–14 yr), `age_years` is near-collinear with site identity, so the network used
it as a **per-site intercept**, not a degradation law — implying a non-physical,
non-monotonic ~1.4–2.2 %/yr in-sample rate and inflating the served `age=0`
(new-install) prospect prediction by **×1.33**. W1 serves prospects, so every
prospect estimate rides on that `age=0` behaviour.

The fix (Phase 1, committed): remove `age_years` from the MLP and route it into a
**bounded multiplicative degradation factor** with a **fixed** rate,
`CF = head(weather) × (1 − r · age)`, `r = 0.007/yr`. This makes the `age=0`
prediction defensible by construction (monotone, ≈ ×1.06 uplift at ~8 yr instead
of ×1.33).

The cost: the bounded prior **misses the promotion gate by −2.6 pp** (within-site
temporal-holdout power-space R² **0.844** vs incumbent **0.8707**). A probe proved
the gap is **per-site level, not degradation slope** — exactly the overfit the
fix removes. **Decision (user, 2026-06-14):** promote the bounded prior **by
hand**, do **not** build a site embedding for W1 (it cannot change an unknown
prospect's output), and instead **expose a prospect uncertainty band**. Phase 2
(LOSO) calibrated that band at **1σ ≈ ±17 %** out-of-sample.

Status: Phases 0–2 are done and committed; the docs/report (Phase 3) are done.
The **promotion deploy and the contingent caveat cleanup remain** (§8) — they are
the user's outward-facing step, deliberately not performed here.

## 2. The problem (why `age=0` is the whole game)

The predecessor task halved the `/predict` yield overestimate (Gate A 2.011 →
1.515 against aged demo sites at served `age=0`) and proved the corpus target is
correct (`corpus/true 1.01`). It attributed the residual **+51 %** to the
`age_years` train/serve convention, not to Jensen or any serve-side POA lever:

- Training derives `age_years` per row from each site's real `installation_date`
  (panels 4–14 yr). Serving a prospect — and `measure_yield.py` — pass `age=0`.
  Forcing `age=0` on the aged demo sites inflated pred/actual by **×1.33**.
- But that check is near-circular: with 10 sites `age_years` is near-collinear
  with site identity, so the MLP may be memorising each site's *level*, not
  learning degradation. The implied rate was ~1.4–2.2 %/yr, *non-monotonic* at
  low POA, and `age=0` is an extrapolation below the youngest training site.

W1 serves an arbitrary map point with no install history, so `age=0` is the
genuine serving condition. Whether the model's `age=0` behaviour is trustworthy
cannot be settled from Gate A alone, because *every* validation site is aged — age
is confounded with site identity. Resolving that confound is this task. The
corpus is already correct, so this is **model-training only** (no re-transform).

## 3. Phase 0 — is a degradation slope identifiable? (the fork)

> Source note (retained): `data-exploration/irradiance/poa_attribution/
> age_identifiability.{py,md}`. This section folds in its reasoning because the
> plan — its only other home — is deleted on finalisation.

**Question.** Should Phase 1 *fit* `r` (band-constrained) or *fix* it
(prior-only)? That hinges on whether a genuine, transferable degradation slope is
estimable *within* sites (where age moves independently of identity) at this N.

**Method (fixed a priori).** Ten **separate** per-site regressions (never pooled
— pooling re-admits the cross-site age/identity confound):

```
log(CF) ~ t_years + log(POA) + log(POA)^2 + temperature + sin/cos(doy, K=2)
```

`t_years` = years since the site's window start (= panel age up to a constant), so
its slope **is** the per-year fractional CF change; `r = −d log(CF)/dt`. POA floor
≥ 50 W/m² (condition on the exogenous regressor, not the outcome); inverter-
clipped rows censored as in training; Newey-West HAC(30) SEs. Robustness (not a
spec hunt): K=3 harmonics, POA-decile FE, POA floor ∈ {20, 100}.

**Result (primary spec).** `r > 0` = CF declining with age:

| site | n | mean age (yr) | r (%/yr) | 95% CI |
|---|---:|---:|---:|---|
| 89665 | 540 | 3.4 | +4.87 | [+1.79, +7.95] |
| 61272 | 603 | — | +3.21 | [+0.05, +6.38] |
| 25724 | 384 | 11.4 | +7.62 | [+1.18, +14.06] |
| 79336 | 392 | — | +4.54 | [+2.29, +6.80] |
| 4708 | 556 | 13.7 | +3.92 | [+0.94, +6.91] |
| 82517 | 267 | 6.3 | +5.26 | [+2.52, +8.01] |
| 36019 | 580 | 10.2 | +5.10 | [+1.77, +8.43] |
| 42248 | 603 | 9.4 | +2.23 | [−2.03, +6.50] |
| 24667 | 606 | 13.4 | +0.87 | [−3.71, +5.44] |
| 56874 | 230 | 7.5 | −1.95 | [−5.89, +2.00] |

**9/10 declining; median +4.23 %/yr; only 1/10 inside the physical 0.5–1.0 %/yr
band.** Robustness medians: K=3 +4.33, POA-decile-FE +4.02, POA-floor-20 +2.23,
POA-floor-100 +1.96 — **sign stable** (8–10/10 declining), **magnitude not** (it
swings 2–5 %/yr with the POA window).

**Interpretation — a third outcome, not either anticipated fork row.**

1. **A real, consistent within-site decline exists.** 9–10/10 negative, stable
   across harmonic order and POA-control form (so not a 2.2-yr seasonality leak).
2. **But ~2–5 %/yr is 2–5× the physical band and POA-floor-sensitive.** Clean
   multiplicative degradation is irradiance-independent, so a rate that moves with
   the POA window is **not** pure degradation. Likely contaminants, all pointing
   the same way over a short window: **soiling**, a **weather-corpus POA
   vintage/aggregation drift** 2024→2026 (errors-in-variables in the regressor we
   control on), and a few short, noisy censored series (82517/56874/25724 carry
   the widest CIs). *Reverse-ordering fingerprint:* the within-site rate (~4 %/yr)
   came out **higher** than the free-MLP cross-site rate (~1.4–2.2 %/yr). If the
   inflation were pure site-identity proxying, the *confounded* cross-site
   estimate should be the larger one — the reverse is a signature of a
   calendar/vintage artifact on the within-site time axis.
3. **Decisive consequence — a free fit would make things worse.** At face value,
   r ≈ 4 %/yr inflates the age=0 prospect by `1/(1−0.04·9) ≈ 1.56×` — *larger*
   than the ×1.33 it removes. The physical band constraint is the **entire safety
   mechanism**, not optional tuning.

**Fork taken: FIX `r`** within the physical band (prior-only). The signal is
present and directionally clear, but its magnitude is non-physical and fragile,
and a band-clamped fit would just pin to the ceiling (data wants ~4 % ≫ r_max ≡
fix at 1.0 %/yr). Fixing is preferred because it lets the value be chosen on
physical/product grounds. **Value: `r = 0.007/yr`** (0.7 %/yr, literature-central
for c-Si, mid-band). The within-site sign agreement is the *check* that the
prior's **direction** is right; physics — not this data — supplies the **rate**.

Tension recorded (not resolved by the data): the data argues for the band's upper
end, but a larger `r` makes the already-optimistic prospect (self-selected
PVOutput sites) *more* optimistic. 0.7 % is the mid-band compromise; revisit only
if Gate A lands materially off expectation (§7).

## 4. Phase 1 — the bounded degradation prior, and the gate miss

**Model change (committed).** `CapacityFactorNet.forward(x)` =
`network(x[:, :−1]) × (1 − r · x[:, −1:])` — age is the trailing column, kept as a
single tensor so the shared train loop / weather path is untouched. `r` is a
fixed float from `TrainingConfig.r_fixed` (a non-trainable buffer; `__post_init__`
asserts `r_min ≤ r ≤ r_max`, band `[0.005, 0.010]`). `age_years`/`age_known` were
removed from `CONTINUOUS/BINARY_FEATURES`; `AGE_COLUMN` is structural, unscaled.
The scaler now scales the 3 weather features (`day_of_year`, `temperature`,
`plane_of_array_irradiance`) only. This is what kills the non-monotonic low-POA
age behaviour: age is no longer a free MLP input.

**Retrain result (`data-v2026-06-12`).** The age=0 runaway is fixed (×1.33 →
bounded ×1.06) and the code is green. But the **promotion gate misses**:
within-site temporal-holdout power-space **R² 0.844** (mean of 6 seeds, sd 0.009)
vs incumbent **0.8707** — a robust **−2.6 pp**, beyond the gate's 2 pp tolerance
(5/6 seeds below the 0.8507 floor). The gate is `pv.critical_metric` =
clamped-power test R².

**Diagnosis — per-site LEVEL, not slope** (probe: `age_level_probe.py`,
`age_promotion_gate.md`). A leakage-safe per-site multiplicative intercept (fit
`mean_actual/mean_pred` per site on **train**, applied to **test** — exactly what
an embedding's `s_site` learns) **recovers R² fully**, without touching `r`:

| pooled power R² (seed 1) | value |
|---|---|
| r=0.007 base | 0.856 |
| r=0.007 **+ per-site intercept** | **0.906** (above incumbent) |
| r=0.030 (≈ Phase-0 empirical decline) | 0.870 |

Per-site R² is the tell: the intercept lifts the *weak* sites (61272
0.437→0.845, 82517 0.747→0.852) → the gap is **between-site level**. Raising `r`
to 0.03 lifts *pooled* R² only by re-spreading per-site levels through the
cross-site age range (age re-proxying identity — the exact confound this task
removes), **degrades most per-site R²**, fails to recover 61272 (age pinned at the
median), and re-arms the age=0 runaway (×1.37). So raising `r` is a false fix; the
mechanistically correct read is "the miss is per-site level."

**But the embedding does not help the W1 product.** The gate and an embedding's
per-site levels concern the **known 10 sites**. W1 serves **prospects** (unknown
site → an embedding's unknown slot → **population-default** level = the *same*
output the bounded prior already gives). An embedding would recover the gate
metric and the known-site `/validate` path, not the prospect prediction.

## 5. The decision (2026-06-14)

Per-site level for an unknown prospect is genuinely unmodellable — so quantify it
as uncertainty rather than chase it:

1. **Promote the bounded-prior model (`r = 0.007`) by hand, accepting the −2.6 pp
   gate drop, documented.** The gate compares against the *incumbent*, whose extra
   R² **is** the age-as-site-level overfit this task exists to remove — the wrong
   yardstick here. (Promotion is already a manual step; the Cloud `model-trainer`
   auto-promotes within 2 pp and has no skip flag.)
2. **Do not build the site embedding for W1.** It cannot change an unknown
   prospect's output.
3. **Expose a prospect uncertainty band** instead: `expected × (1 ± margin)`,
   from the spread of per-site levels, framed as a **floor**. Tracked as separate
   W1 product work (`briefs/prospect-uncertainty-band.md`): `/predict` returns
   `expected ± margin`; the website renders it.
4. **Embedding demoted** from "Phase-2 trip-wire, build if LOSO shows a gap" to
   "revisit only if the known-site `/validate` (W2) path later needs per-site
   level." For prospects it is resolved as *won't-build*.

## 6. Phase 2 — LOSO, the prospect uncertainty band

**Implementation (committed).** `loso_eval` (`training/loso.py`) + pure scorers
(`loso_site_metrics`, `build_loso_report` in `evaluation.py`); an optional
`EvalReport.loso` section (persistence round-trips it; pre-LOSO artifacts load
`loso=None`); the `loso-pv` CLI subcommand; and **defensive** trainer wiring
(`bootstrap._maybe_attach_loso`, `config.compute_loso=True`, provenance
`pv_loso_pooled_r2`/`pv_loso_band_1sigma`) — LOSO never gates promotion and a fold
failure can't abort the weekly job. Driver:
`data-exploration/.../loso_probe.py`.

**Method.** For each of the 10 sites: train on the other 9, predict the held-out
10th at its **real ages**. Per-site **level** = mean actual CF / mean predicted CF
**in CF space**, so the fixed-`r` factor cancels and the ratio isolates *level*,
not age. Band statistic = sample SD (ddof=1) of the per-site levels normalised to
mean 1.0 — computed identically to the in-sample probe so the two are directly
comparable.

**Result (seed 1, `data-v2026-06-12`; full table in `age_promotion_gate.md`).**
Normalised so population = 1.00:

| site | level | pw R² | | site | level | pw R² |
|---|---|---|---|---|---|---|
| 89665 | 1.28 (+28%) | 0.70 | | 56874 | 0.98 (−2%) | 0.85 |
| 79336 | 1.18 (+18%) | 0.86 | | 4708 | 0.98 (−2%) | 0.92 |
| 42248 | 1.12 (+12%) | 0.84 | | 24667 | 0.94 (−6%) | 0.81 |
| 36019 | 0.99 (−1%) | 0.91 | | 82517 | 0.88 (−12%) | 0.82 |
| 25724 | 0.98 (−2%) | 0.87 | | 61272 | 0.66 (−34%) | 0.35 |

- **Out-of-sample 1σ ≈ ±17 %** (vs in-sample ±15 %); 2σ ≈ ±34 %; range
  −34 %…+28 %. Excluding `61272` (no install date → median-imputed age; a separate
  fit-outlier, pw R² 0.35) the band is ~±13–14 %, so the outlier widens but does
  not invent the band. **This is the band to ship** as a floor.
- **Out-of-sample mean level ≈ 1.00** — a 9-site fit predicts an unseen site's
  level with no systematic over/under-statement; the LOSO ordering matches the
  in-sample probe → the spread is genuine **per-site level**
  (orientation/shading/soiling/panel-tech), not age. Age check on the 8 dated
  sites: r(age, level) = **−0.55**, but **+0.08 excluding the youngest `89665`**
  → no systematic age trend across the 7 sites at 6–14 yr; the lone signal is the
  youngest reading highest, equally explained by its premium config (dual-azimuth
  + optimisers). At most a small degradation-mismatch hint (fixed `r`=0.7 %
  under-corrects Phase 0's ~2–5 %/yr) — which only widens the floor.
- **Pooled power R² 0.839**, against the **bounded prior's own** within-site
  temporal-holdout R² **0.844** (Phase 1) — *not* the old free-age incumbent
  0.871. So the cross-site penalty is **small** (~0.005), not the 0.871→0.839 gap
  (most of which is the Phase-1 model change). Small, not zero: LOSO scores each
  site's full window, the temporal number only the hard tail.

**Why the band is a floor, not a full error bar.** The 10 are self-selected,
well-maintained PVOutput sites (an arbitrary roof can sit below the worst of
them), and the band is **mostly per-site level** — it excludes weather-model
irradiance error, single-year weather noise, and degradation uncertainty, which
widen the true band. The generalisation limitation LOSO measures but cannot fix:
the model represents a population of 10 self-selected sites, so the prospect
prediction is likely **optimistic** for an arbitrary map point. Carry this in W1
launch copy. The fix is more / younger / more-representative sites, not this task.

## 7. Acceptance vs outcome

The brief/plan acceptance, against what landed:

1. **`r` fixed in the physical band, monotone, non-monotonic low-POA behaviour
   gone** — ✅ (`r = 0.007`; age removed as MLP input).
2. **LOSO close to the within-site number** — ✅ pooled 0.839 vs 0.844 (small
   penalty). Note the §2 caveat: with a band-clamped `r`, *slope* transfer passes
   near-trivially; the meaningful LOSO read here is the *level* spread, which is
   real and is the band's calibration.
3. **Age path fixed; Gate A materially improved** — the age=0 runaway is removed
   by construction (×1.33 → ×1.06). The expected Gate A is ~1.15–1.20 at `age=0`
   (≈ 1.11 non-age residual × 1.06) and ~1.11 at real ages — **not** an absolute
   ≈ 1.0 (a clean bounded factor cannot absorb the non-age residual, unlike the
   old free-age fit which co-adapted with it). The numeric Gate A confirmation is
   **deferred** (a one-shot check, optional, not a blocker): it needs raw-PVOutput
   actuals from `gs://pv-prospect-staging/data/raw/timeseries/pvoutput/` via
   `measure_yield.py --actuals-gcs-prefix` + GCS auth. Tracked as TODO "Gate A
   confirmation". Do **not** chase the ~1.11 residual with serve-side POA
   recalibration (predecessor report §6 forbids it).
4. **Model promoted; caveats cleaned; W1 note flipped** — the gate is **not** met,
   resolved by the by-hand promotion decision (§5). The W1 *model gate* is
   resolved; W1 *launch* still depends on the band product (#5) and the website.
   Promotion + caveat cleanup remain (§8).

## 8. Remaining operational step (promote + caveat cleanup) — not yet done

These are **outward-facing** and were deliberately not performed in Phase 3 (the
served-poa precedent deferred promotion to the user; and the API caveats describe
*production-served* behaviour, so they stay true only while the old model is live).
They must be done **together, in order**:

1. **Promote the bounded-prior artifact by hand** to the production model store
   (`gs://pv-prospect-versioned-model`); the Cloud `model-trainer` will not
   auto-promote it (gate −2.6 pp, no skip flag) — hence by hand.

   **Regenerate the artifact** (authoritative recipe — `models/` is gitignored, so
   any local dir named below is a convenience, not a durable source). Fresh
   **all-sites** bounded-prior train on the current model code with
   production-parity defaults:
   `cd pv-prospect-model && poetry run python -m pv_prospect.model.entrypoint
   train-pv --data-root <pulled data-v2026-06-12>/data/prepared --pv-sites-csv
   data/static/pv_sites.csv --output-dir <store>/pv` (CPU torch, all defaults ⇒
   all sites, `r_fixed = 0.007`). Assemble the production store as **this PV
   artifact + the incumbent weather artifact** (the age fix is PV-only; weather is
   unchanged). Verify it is the bounded prior, not the free-age model:
   `training_config.json` has `r_fixed = 0.007` and `feature_spec.json` lists
   **3** continuous features (`day_of_year`, `temperature`,
   `plane_of_array_irradiance`) — **no** `age_years`.

   The local Phase-1 train (all-sites, `r_fixed = 0.007`, verified) is at
   `models/pv-age-feature-2026-06-13/pv/` — **PV only, no assembled store**. Do
   **not** promote `models/gate-a-store-2026-06-12/`: that is the predecessor's
   *free-age* store (`age_years` still a continuous feature, no `r_fixed`) — the
   exact model this task replaced.

2. **Then** clean the caveats that describe the now-superseded production model.
   **Note: the three caveats are mutually inconsistent *today*** — `poa.py` and
   the app README claim a ~30 % *under*estimate (`data-v2026-05-31`), while
   `main.py` `_VINTAGE_CAVEAT` claims a +100 % *over*estimate (`model-v2026-06-10`).
   Reconcile all three to the promoted model's actual behaviour; this is not a
   clean baseline where one line changes. The exact edits (recorded here because
   the brief/plan are deleted):
   - `pv-prospect-app/pv_prospect/app/poa.py` — module docstring: remove the
     "~30 % systematic underestimate against data-v2026-05-31 artifacts … not yet
     retrained on the 24 h-mean corpus" paragraph (lines ~11–14); the
     reconstruction is now self-consistent with the *promoted* 24 h-mean model.
   - `pv-prospect-app/pv_prospect/app/main.py` — `_VINTAGE_CAVEAT` (lines ~37–46,
     "+100% overestimate … model-v2026-06-10"): remove or rewrite — it described
     the old-basis artifact; the promoted model is on the corrected corpus.
     Rewrite the `/predict` age caveat (line ~263, "Age degradation: age_years=0
     … slight extrapolation") to reflect the bounded prior (age=0 is a bounded
     ~×1.06 uplift, not an extrapolation). Add the **uncertainty-band** wording
     when #5 ships.
   - `pv-prospect-app/README.md` — "Known limitation" section (lines ~129–135,
     the data-v2026-05-31 underestimate): update to the promoted model's state.

3. **Then finalise:** delete `briefs/pv-age-feature.md`, `plans/pv-age-feature.md`,
   and `briefs/cross-site-generalization-eval.md` (closed by Phase 2), and the
   `briefs/prospect-uncertainty-band.md` brief once #5 ships. **Keep** this report
   and the data-exploration notes (§9).

## 9. Durable artifacts & pointers

Retained (not deleted on finalisation):

- **This report** — the permanent home for the Phase 0 fork rationale and the
  decision.
- `data-exploration/irradiance/poa_attribution/age_identifiability.{py,md}` —
  Phase 0 per-site slopes + the fix-`r` reasoning.
- `data-exploration/irradiance/poa_attribution/age_level_probe.py` +
  `age_promotion_gate.md` — the gate-miss diagnosis (per-site level) and both the
  in-sample (±15 %) and Phase-2 LOSO (±17 %) band tables.
- `data-exploration/irradiance/poa_attribution/loso_probe.py` — the LOSO driver.
- Memory: `pv-age-feature-phase1-state`, `pv-retrain-residual-age-convention`,
  `poa-gap-aggregation-mismatch`.

Code (committed): `pv-prospect-model` LOSO + bounded-prior model; documented in
`pv-prospect-model/README.md` (PV Data Flow steps 4/6/7, Design Notes, PV
Cross-Site (LOSO) Evaluation).
