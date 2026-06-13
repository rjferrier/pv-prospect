# Plan — Validate & fix the PV `age_years` feature

> **Companion to `briefs/pv-age-feature.md`** (the what/why and the launch-decision
> framing A/B/C). This plan firms up the **implementation design** the brief deferred
> ("the model-structure section below may be promoted to a plan"). Upstream context:
> `reports/pv-train-on-served-poa.md` §5–6 (the residual decomposition) and memory
> `pv-retrain-residual-age-convention`. Validator: `briefs/cross-site-generalization-eval.md`
> (LOSO), in scope as Phase 2. **This is the W1 public-launch gate.**
>
> **Scope decision (2026-06-13): the site embedding is deferred** (→ *Deferred* section).
> In-scope is the bounded degradation prior (Phase 1) + the LOSO validator (Phase 2) —
> a **model-only** change with **no serving-contract churn**.

---

## 0. The one thing that decides this plan's shape

Everything in the brief's "candidate model structure" — the site embedding, embedding
dropout, within-site identification of the degradation slope `β` — exists to **extract a
transferable degradation slope from within-site temporal variation**. The brief itself
hedges that this signal is weak (~1–2 % CF per site over ~2.2 yr, against noisy daily
CF). **If the slope is not actually estimable at N = 10 × 2.2 yr, we would be building
machinery to identify something that isn't there.**

So the plan forks on a cheap, up-front measurement (**Phase 0**), and is staged so that
the part that *unblocks W1* (**Phase 1**, the bounded physical prior) does **not** depend
on that machinery. **The site embedding is deferred** (scope decision above): what stays
in scope is the LOSO validator (**Phase 2**), which runs on the Phase-1 architecture and
doubles as the **trip-wire** — the embedding becomes a *future* task only if LOSO reveals
site-shaped residual structure (see *Deferred*).

**LOSO does not require the embedding.** LOSO (train on 9 sites, predict the 10th) runs on
*any* architecture. In the cross-site brief the embedding is the *trip-wire response* to
site-shaped residuals, not a prerequisite. Deferring it keeps this task to a model-only
change plus its validator, with **no serving-contract churn** — see *Deferred* for the
cost we are choosing not to pay yet.

---

## 1. Design decisions (locked for this plan)

### 1.1 Degradation is **multiplicative**, not additive

The brief sketches `CF ≈ α_site + β·age + f(POA, temp, …)`. That additive form is wrong
on physics: degradation is a *fractional* loss, so an additive `β·age` would subtract the
same absolute CF at POA→0 (night/winter, where CF≈0) as at full sun. The correct form is
a multiplicative degradation factor on the weather-driven response. **In-scope form
(embedding deferred):**

```
CF = f_weather(POA, temp, day_of_year)  ·  (1 − r · age)
```

The eventual full form (when the deferred embedding lands) reinstates a per-site level:
`CF = f_weather · s_site · (1 − r·age)`.

- `f_weather` — shared MLP on **weather features only** (the transferable physics
  response); this is what a prospect rides on. Until the embedding lands, it also carries
  per-site level implicitly (population-mean fit across the 10 sites).
- `s_site` — per-site multiplicative level (**deferred**; absorbs orientation / shading /
  soiling baseline / panel tech). Implicitly 1.0 until built; dropped → population default
  for a prospect when it does land.
- `(1 − r·age)` — degradation factor; `r` constrained to a physical band so it is
  monotone-decreasing by construction.

**Log-space alternative (noted, not chosen as primary).** `log CF = log f_weather +
b_site − r·age` makes the decomposition additive, lets `r` read off directly, and gets
positivity for free via `exp`. It is cleaner *if* a site term is present (the deferred
embedding). Its cost: it changes the training loss from MSE-on-CF to (effectively)
relative error, which re-weights low-CF winter rows and breaks comparability of the
`test_f_space` R² the promotion gate leans on; and `log CF` is ill-defined for the CF≈0
rows the corpus contains. **Decision:** keep **MSE on CF** with the raw multiplicative
product.

### 1.2 Positivity

The degradation factor is `(1 − r·age)` with `r ∈ [0.005, 0.01]` and `age ≲ 16 yr` →
factor ∈ `[0.84, 1.0]`, strictly positive. The product's sign therefore equals the
existing head's sign — no positivity concern, and the unbounded head (and its "unbounded
converges faster" rationale in `nets/pv.py`) is preserved. Positivity only becomes a
question with the deferred embedding, when `s_site` multiplies the head (see *Deferred*).

### 1.3 The prior is load-bearing; identification only complements it

What actually kills the ×1.33 `age=0` inflation and makes the prospect prediction
*defensible by construction* is the **bounded multiplicative factor** — and that needs
**no embedding**. Fitting `r` freely-but-constrained to the physical band (vs leaving age
as a free MLP input) is the whole Phase-1 fix. Within-site identification (the deferred
embedding) is a *check* on the prior and the path to letting the data eventually stand
alone — exactly the brief's "complements B's physical prior" stance.

### 1.4 Constraining `r`

Parameterise `r = r_min + (r_max − r_min)·σ(θ)` with `[r_min, r_max] = [0.005, 0.010]`
(physical ~0.5–1 %/yr), `θ` a free scalar. `r` is then **fitted but always in-band and
≥ 0** → monotone by construction (acceptance #1). If `θ` pins `r` to a band edge, that is
informative (the data wants to leave the band — see Phase 0 / §6). A *fixed* `r` (e.g.
0.7 %/yr, no fit) is the fallback if Phase 0 shows no usable signal.

---

## 2. Acceptance — with one sharpening

Restating `briefs/pv-age-feature.md`'s acceptance, plus a distinction that must not be
lost when reading the Gate A numbers. **Phase 1 fixes only the age path.** The report's
geomean factorization of the 1.515 Gate A is `corpus 1.01 × fit 1.04 × age 1.33 ×
temp 1.01 × (POA-recon+Jensen) 1.05`; everything except `age` (**~1.11× combined**) is
**untouched** by re-routing age — it is the known, deprioritised non-age residual (report
§6: "minor, don't chase", and explicitly **not** the rejected serve-side POA lever).

> **`age=0` (prospect) should *not* land at pred/actual ≈ 1 against aged-site actuals.**
> A fresh panel genuinely out-produces the aged sites we measured. Phase 1 replaces the
> age **1.33** runaway with a bounded uplift `≈ 1/(1−r·age) ≈ 1.06` (r ~0.7 %, age ~8 yr)
> — keep this, it is physically correct. The ~1.11× non-age residual persists either way.
> Working from the report's own factors:
> - Gate A at each site's **real age** → ≈ **1.11** (age factor → ~1.0; residual persists).
> - Gate A at **`age=0`** → ≈ **1.15–1.20** (≈ 1.11 × 1.06).
>
> **Tension with the brief's acceptance #3 ("real ages → ≈ 1").** That ≈ 1.0 was the
> *old* model's empirical result (`diag_realage.py`), where the free age feature
> co-adapted with and partly absorbed the POA-recon/fit quirks — so "restoring real age"
> cancelled both age *and* some of the residual. A clean bounded factor cannot absorb the
> non-age residual, so the new model's real-age Gate A likely sits ~1.11, **above 1.0**.
> Read ~1.1 as *success* (age path fixed), **not** regression — and do **not** chase the
> residual with the report-forbidden serve-side POA recalibration.

Full acceptance:

1. Fitted `r` monotone and in the physical band; the non-monotonic low-POA age behaviour
   gone (guaranteed by removing age as a free MLP input — §3.2).
2. LOSO aggregate close to the within-site number (**caveat:** a *tightly* band-clamped
   `r` makes "transfer" near-trivially pass — the meaningful test uses the freer
   within-site estimate from Phase 0; see §6).
3. **Age path fixed and Gate A materially improved** — `age=0` Gate A down from 1.515 to
   ~1.15–1.20, the residual attributable to the known non-age factors above. *Not* an
   absolute pred/actual ≈ 1 band (see the tension note).
4. Model promoted; caveats cleaned; W1 launch note flipped.

---

## Phase 0 — Identifiability check (forks the plan)

**Goal:** decide, cheaply and before any model change, whether a transferable degradation
slope is estimable at all.

**Where:** `pv-prospect-instance/data-exploration/irradiance/poa_attribution/` (the report
notes the original `diag_*.py` lived in `/tmp`; reconstruct here — per memory
`feedback_script_location`, keep investigative scripts with future value out of `/tmp`).

**Method:** for each of the 10 sites, regress `log(CF)` on `age` (equivalently calendar
time within the site's window) with a coarse weather/seasonality control — e.g. POA
decile × day-of-year bin fixed effects, or residualise CF on `f_weather`-like bins first.
Read off the 10 within-site slopes and their CIs. (8 sites have install dates; `61272`,
`79336` do not — exclude or use calendar time.)

**Decision rule:**

| Phase 0 finding | Consequence |
|---|---|
| Slopes mostly negative, ~0.5–1 %/yr, consistent | Signal is real → fit `r` (band-constrained, §1.4); LOSO tests genuine transfer; the deferred embedding is the eventual upgrade path. |
| Slopes noisy / mixed-sign / wide CIs | Not identifiable at this N → the **prior is the entire deliverable**; `r` is **fixed**, not fitted; LOSO's job flips to "confirm we *cannot* identify, so we *impose*." Embedding stays deferred regardless. |

**Deliverable:** a short findings note (in the same dir, or folded into the eventual
report) recording the 10 slopes and the fork taken (fit vs fix `r`).

---

## Phase 1 — Bounded degradation prior (no embedding) — *unblocks W1*

The minimal change that removes the runaway and is defensible by construction. Touches
**`pv-prospect-model` only** (corpus is already correct — brief scope note).

### 3.1 `nets/pv.py` — route age into a degradation factor

`CapacityFactorNet.forward` currently takes the full feature row. Change it to take
weather features and `age` separately and return `head(weather) · (1 − r·age)`:

- Keep the existing 4-layer head, now sized to **weather features only**
  (`day_of_year, temperature, plane_of_array_irradiance`).
- Add `r` as a constrained parameter (§1.4): store `θ` as an `nn.Parameter`; expose `r`
  via the `r_min + (r_max−r_min)·σ(θ)` map.
- `forward(weather_x, age)` → `self.network(weather_x) * (1.0 - self.r * age)`.

### 3.2 `features/pv.py` + `splits.py` — age leaves the feature vector

- Remove `age_years` from `CONTINUOUS_FEATURES` and `age_known` from `BINARY_FEATURES`;
  they are **no longer MLP inputs** (this is what kills the non-monotonic low-POA
  behaviour — acceptance #1). `age_years` stays as a **column** (routed to the factor);
  `compute_age_years` is unchanged (median-impute unknown installs as today — those sites
  then carry a population-median degradation in training, which is fine).
- Decide `age_known`'s fate: simplest is to drop it from the model entirely (its only job
  was flagging imputation for the old free feature). Retain the column for diagnostics.
- The `StandardScaler` now scales the 3 weather features only.

### 3.3 `domain.py` — config + spec

- `TrainingConfig`: add `r_min: float = 0.005`, `r_max: float = 0.010` (and, if Phase 0
  says "fix r", a `r_fixed: float | None = None`).
- `FeatureSpec`: continuous features drop to the 3 weather features; add the degradation
  parameters needed to reconstruct the net (`r_min`, `r_max`, and the learned `r` /
  `θ`). The age column is structural, not a scaled feature.

### 3.4 `training/pv.py` + `training/loop.py` — fit `r` jointly

- Pass `age` tensors alongside the scaled weather tensors into the loop; the loss is
  unchanged MSE on CF. `r`'s `θ` is in `model.parameters()`, so Adam fits it jointly.
- `inference.py::_run_pv_forward` must supply `age` to `forward` (read the `age_years`
  column from the df, unscaled).

### 3.5 `persistence.py` — round-trip the new net

- `save_artifact` / `load_artifact`: persist `r_min/r_max` and the learned `r` (or `θ`);
  reconstruct `CapacityFactorNet` with the new constructor signature. Bump nothing
  externally versioned — the artifact is internal.

### 3.6 Tests

Update/extend the unit tests that already exist for this surface:
`tests/unit/features/pv/test_augment_features.py`,
`.../test_compute_age_years.py`, `tests/unit/inference/test_predict_capacity_factor.py`,
`tests/unit/persistence/test_persistence.py`, `tests/unit/training/test_run_train_loop.py`.
New assertions worth their weight: `r` stays in `[r_min, r_max]`; `forward` is monotone
non-increasing in `age`; `age=0` returns exactly the head output; round-trip preserves `r`.

### 3.7 Retrain + measure

- Retrain locally on `data-v2026-06-12` (CPU torch, production-parity defaults — the
  exact recipe is in `reports/pv-train-on-served-poa.md` Appendix).
- Run **Gate A** two ways with `measure_yield.py` (same window 2025-06-09 → 2026-06-08,
  same store layout as the report):
  - **real ages** (diagnostic) → expect ≈ 1.11 (age neutralised; non-age residual persists — §2);
  - **`age=0`** (the served prospect condition) → expect ≈ 1.15–1.20 (≈ 1.11 × 1.06).
- Compare the fitted `r` against Phase 0's free within-site estimate (sanity, not gate).

**W1 is unblockable on Phase 1 alone if:** `r` is monotone and in-band, the `age=0`
runaway is gone, and Gate A is materially improved (1.515 → ~1.15–1.20) with the residual
attributable to the known non-age factors (§2). Do **not** gate on an absolute ≈ 1.0, and
do **not** treat the persistent ~1.11 as a reason to recalibrate POA serve-side (report
§6). Phase 2 (LOSO) then runs as validator.

---

## Phase 2 — LOSO validator (closes `briefs/cross-site-generalization-eval.md`)

Runs on the **Phase-1 architecture** (no embedding). Touches `pv-prospect-model`
(training/eval: LOSO loop + `eval_report` schema) and `pv-prospect-model-trainer`
(compute + record; offline, 10 trainings per run, acceptable for a weekly job).

- For each site: train on the other 9 (reuse `train_pv`'s `system_ids` exclusion),
  predict the held-out 10th, score power-space R²/MAPE. Aggregate + per-site into a new
  `eval_report.loso` section; surface via `/version` and/or monitoring.
- **Trip-wire:** compare the LOSO aggregate to the within-site (temporal-holdout) number.
  A small gap → the population-mean model transfers to an unseen site (W1's headline claim
  holds). A large / site-shaped gap → the trip-wire for building the **deferred embedding**
  (with its unknown-site default) and/or more sites.
- **Acceptance #2 caveat (from advisor):** if `r` is tightly band-clamped, the same `r`
  applies everywhere and "transfer" passes near-trivially. The *meaningful* transfer test
  compares the held-out site's **freer within-site slope** (Phase 0 method, on the
  held-out site) against the slope the 9-site fit implies.

**Generalisation limitation LOSO measures but cannot fix.** The model represents a
population of **10 self-selected PVOutput sites** (well-sited, well-maintained reporters
self-select), so the prospect prediction is likely **optimistic** for an arbitrary map
point — this is true with or without the embedding (deferred or not, the training
distribution is those 10 sites). Record it in the report and carry it in W1 launch copy
(brief Option A's caveat survives in spirit). The fix is more / younger / more-
representative sites, not this task.

---

## Phase 3 — Finalisation (carried forward from `pv-train-on-served-poa`)

Gated on a model that passes the above (Phase 1 sufficient for the launch decision; Phase
2 LOSO informs how strong the caveat needs to be).

- **Promote** the retrained PV model. The Cloud `model-trainer` auto-promotes via
  `passes_promotion_gate` on `pv.critical_metric` (clamped-power test R²); confirm the new
  artifact's R² is within the 2 pp tolerance of the incumbent before promoting, or promote
  the locally-trained artifact by hand as the report did (the Cloud job has no skip flag).
- **Caveat cleanup** (do **not** touch until promotion — production still serves old-basis
  artifacts, so the caveats are still *true*): `app/poa.py` docstring (both notes),
  `pv-prospect-app/README.md`, and the `/predict` caveat in `main.py` (line ~263, "Age
  degradation: age_years=0 … slight extrapolation" — rewrite to reflect the bounded prior).
- **Flip the `TODO.md` W1 note** and unblock the website's W1 public launch.
- **Docs:** update `pv-prospect-model/README.md` — the PV data-flow/design notes now
  describe age as a **bounded multiplicative degradation factor** (not a free MLP feature);
  the "Site identity is not a feature" note **stands** (embedding deferred). Add the LOSO
  eval section. Per `documenting.md`, model-internal design → model README.
- **Report:** write `reports/pv-age-feature.md` (the outcome lives outside the source tree —
  a model behaviour change + Gate A / LOSO measurements). Retain it; delete the brief + this
  plan on finalisation (CLAUDE.md task lifecycle). The cross-site/LOSO brief is closed by
  Phase 2 and finalised likewise.

---

## Deferred — site embedding (trip-wire upgrade, **not** in this task)

Kept here so the design isn't lost. Build **only** if Phase 2 LOSO shows a site-shaped
generalisation gap (the trip-wire). This is the brief's full decomposition and decision
#10's "revisit with a small embedding".

**Model.** Add `s_site` as `nn.Embedding(n_sites + 1, 1)` (index `n_sites` = "unknown /
prospect" slot), applied multiplicatively (`exp(emb)` init ~0 → scale ~1). **Embedding
dropout:** during training, with prob `p_drop`, replace a row's site index with the
unknown slot — this trains the prospect default vector (cleanest of the brief's three
anti-collinearity options; residualisation-order and mixed-effects shrinkage are
fallbacks). With `s_site` free per-site, cross-site age variation is absorbed by the
embedding, so `r` is identified **only** by within-site temporal variation.

**Serving-contract blast radius (the reason it's deferred).** A site channel crosses the
model→app boundary:

| Component | Change |
|---|---|
| `model/nets/pv.py`, `domain.py`, `persistence.py` | net constructor gains `n_sites`; `FeatureSpec` gains site-index vocabulary + embedding; round-trip. |
| `model/inference.py` | `predict_capacity_factor` df must carry a site-index column; map unknown → unknown slot. |
| `app/chain.py` | builds `pv_rows` — add site-index = **unknown slot** for `/predict`. **Note:** today it sets `age_known=1` (the brief's "(age_known=False)" is inaccurate). |
| `app/main.py` | `/predict` is always a prospect → unknown slot; request schema likely unchanged. |
| `app/validation.py` | known-site validation *could* use the **real** site embedding (site is in training) — W2 scope; defer. |
| `scripts/measure_yield.py` | prospect mode → unknown slot. |

If/when built, the `pv-prospect-model/README.md` "Site identity is not a feature" design
note must be rewritten (site identity becomes a training feature with an unknown-site
default).

---

## 5. Suggested sequencing

1. **Phase 0** (½–1 day, data-exploration) — fork decision (fit vs fix `r`).
2. **Phase 1** (model-only) — retrain, Gate A both ways. **W1 unblockable here.**
3. **Phase 2** (LOSO) — validator on the Phase-1 architecture; closes the cross-site brief.
4. **Phase 3** — promote, clean caveats, flip TODO, docs, report.

Embedding (*Deferred*) is a separate future task, built only if Phase 2's trip-wire fires.
Brief options A and C ride alongside: A (launch caveat copy) is one line either way;
C (hold for full validation) is not taken — LOSO runs as B's validator, not a blocking gate.

## 6. Risks / open decisions

- **Phase 0 says "no signal."** Then `r` is fixed (not fitted) and the launch leans harder
  on the caveat; LOSO confirms "impose, don't identify." Embedding stays deferred. Plan
  already forks for this.
- **`r` pins to a band edge in Phase 1.** Informative, not fatal — report it; consider
  widening the band slightly or fixing `r` at the Phase 0 central estimate.
- **Promotion-gate metric comparability.** Keeping MSE-on-CF (vs log-space) preserves the
  `test_*_space` R² the gate compares; do not switch the loss without re-checking the gate.
- **`age_known` repurposing** vs the live `chain.py` (`age_known=1`) — reconcile in Phase 1
  so serving and training agree. (No site channel is added — embedding deferred — so this
  is the only serving-side touch.)
- **Trip-wire decision (resolved for now):** the site embedding — including wiring a real
  site embedding into the known-site Validation API — is **deferred**, revisited only if
  Phase 2 LOSO shows a site-shaped gap.
