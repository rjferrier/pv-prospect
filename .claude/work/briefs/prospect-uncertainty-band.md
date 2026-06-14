# Expose a prospect yield uncertainty band

> Spun out of **`briefs/pv-age-feature.md`** / **`plans/pv-age-feature.md` §3.8**
> (resolution 2026-06-14). W1 **product** work — distinct from, and downstream of, the
> model change (Phase 1, done). Calibrated by the LOSO eval in
> **`briefs/cross-site-generalization-eval.md`**.

## Why

W1 predicts speculative yield for a site we have never seen. The PV model deliberately
does **not** model per-site "level" — the residual after weather (orientation, shading,
soiling baseline, panel quality, inverter sizing) — because for an unknown prospect that
level is unknowable; the bounded-prior model falls back to the population-mean response.
Phase 1 measured that this level varies widely across the 10 training sites (in-sample:
**1σ ≈ ±15 %, range −30 % … +23 %**). **Phase 2 LOSO** then gave the honest out-of-sample
version — predict each site from the other nine, the genuine prospect scenario:
**1σ ≈ ±17 %, range −34 % … +28 %** (and ~±13–14 % excluding the imputed-age fit-outlier
`61272`); out-of-sample mean level ≈ 1.00, i.e. no systematic transfer bias. Both numbers in
`data-exploration/irradiance/poa_attribution/age_promotion_gate.md`. Shipping a bare point
estimate hides that real uncertainty; the honest move is to expose it.

This is also why the site embedding was **not** built for W1 (plan §3.8): per-site level
can't be recovered for an unknown prospect, so we quantify it as a band rather than model it.

## What

`/predict` returns expected yield **plus a margin**; the website renders *expected ± band*.
Calibrate the margin from the **Phase 2 LOSO out-of-sample level spread** (`loso_probe.py` /
`training/loso.py`): per-site level = mean actual CF / mean model-predicted CF when the site
is held out of training, **1σ ≈ ±17 %** (the in-sample ±15 % from `age_level_probe.py` is the
floor-of-the-floor; LOSO is the honest, slightly wider number actually to ship). Use ±17 % 1σ
as the band, or expose 2σ (≈ ±34 %) if the product wants a wider interval.

Frame it as a **floor**, not a full error bar: the 10 are self-selected, well-maintained
PVOutput sites (an arbitrary roof can sit below the worst of them), and it is **mostly
per-site level** — it excludes weather-model irradiance error and single-year weather noise,
which widen the true band. (It is not *purely* level: the LOSO age check found no systematic
age trend across the 7 sites at 6–14 yr — r ≈ +0.08 excluding the youngest — but the youngest
site reads highest, a small possible degradation-mismatch component from the fixed `r`=0.7 %
under-correcting the real ~2–5 %/yr decline. That can only widen the floor, so the framing
holds.)

## Scope

`pv-prospect-app` (the `/predict` response gains a margin field) + the **website** (renders
the band). Out of scope: modelling per-site level (the **embedding** — demoted to the W2
`/validate` path, plan §3.8); the PV model change itself (done in Phase 1).
