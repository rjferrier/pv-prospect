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
Phase 1 measured that this level varies widely across the 10 training sites: **1σ ≈ ±15 %,
full range −30 % … +23 %** (`data-exploration/irradiance/poa_attribution/age_promotion_gate.md`).
Shipping a bare point estimate hides that real uncertainty; the honest move is to expose it.

This is also why the site embedding was **not** built for W1 (plan §3.8): per-site level
can't be recovered for an unknown prospect, so we quantify it as a band rather than model it.

## What

`/predict` returns expected yield **plus a margin**; the website renders *expected ± band*.
Compute the margin from the cross-site level spread (per-site level = mean actual CF / mean
model-predicted CF across the 10 sites; `age_level_probe.py`). **Prefer calibrating from
Phase 2 LOSO's out-of-sample per-site error** once available — more honest than the
in-sample spread, and expected to be wider.

Frame it as a **floor**, not a full error bar: the 10 are self-selected, well-maintained
PVOutput sites (an arbitrary roof can sit below the worst of them), and it is **level-only**
— it excludes weather-model irradiance error, single-year weather noise, and degradation
uncertainty, all of which widen the true band.

## Scope

`pv-prospect-app` (the `/predict` response gains a margin field) + the **website** (renders
the band). Out of scope: modelling per-site level (the **embedding** — demoted to the W2
`/validate` path, plan §3.8); the PV model change itself (done in Phase 1).
