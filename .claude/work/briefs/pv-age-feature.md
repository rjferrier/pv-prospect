# Validate & fix the PV `age_years` feature (degradation law vs. site fixed-effect)

> Successor to **`reports/pv-train-on-served-poa.md`** (which delivered the corpus
> re-base and re-attributed the residual to this feature) and the diagnosis in memory
> `pv-retrain-residual-age-convention`. Validated by the **LOSO** eval in
> **`briefs/cross-site-generalization-eval.md`** — run the two together. Umbrella
> context: **`briefs/pv-yield-overestimate.md`**.
>
> **This is the W1 public-launch gate** (re-pointed here from the completed
> `pv-train-on-served-poa` task). Specified in **`plans/pv-age-feature.md`** (Phase 0
> identifiability check → Phase 1 bounded prior → Phase 2 LOSO → Phase 3 finalisation).
>
> **Resolution (2026-06-14 — plan §3.8).** Phase 0 → **fix `r`** at 0.007/yr (the
> within-site decline is real in sign but ~2–5 %/yr, non-physical, so impose the rate).
> Phase 1 (done, committed) bounds the age=0 runaway but **misses the promotion gate by
> ~2.6 pp**; a probe proved the gap is **per-site level**, not slope. Decision: **promote
> the bounded prior by hand** (documented — the gate's yardstick is the overfit incumbent),
> **do not build the site embedding for W1** (it can't change an unknown prospect's output),
> and instead **expose a prospect uncertainty band** (≈ ±15 % 1σ — see
> **`briefs/prospect-uncertainty-band.md`**). The embedding is **demoted** to "revisit only
> for the known-site `/validate` (W2) path"; Phase 2 LOSO now also calibrates the band.
>
> **Phase 3 status (2026-06-14).** Docs + report **done** — see
> `reports/pv-age-feature.md` (the permanent record; folds in the Phase 0 fork
> rationale and the exact pending caveat diffs) and `pv-prospect-model/README.md`.
> **Two outward-facing steps remain, in order** (report §8): (1) promote the
> bounded-prior artifact **by hand** to the production model store
> (`gs://pv-prospect-versioned-model`; the Cloud job won't auto-promote it — gate
> −2.6 pp, no skip flag); (2) **then** clean the API caveats (`app/poa.py`
> docstring, `main.py` `_VINTAGE_CAVEAT` + `/predict` age caveat, app `README.md`),
> which describe *production-served* behaviour and stay true only while the old
> model is live. **This brief + the plan + the cross-site brief are deleted only
> after step 1** (finalisation is gated on the deploy).

## Problem

The corpus re-base on the served 24 h-mean POA basis (`data-v2026-06-12`) landed and is
sound — the corpus target is correct (`corpus/true 1.01`). It halved the `/predict`
yield overestimate (Gate A 2.011 → 1.515) but did **not** reach ≈ 1. The corrected
decomposition (report §5–6) attributes the residual **+51 %** to the `age_years`
train/serve convention, **not** to Jensen or any serve-side POA lever:

- Training derives `age_years` per row from each site's real `installation_date`
  (panels 4–14 yr old). Serving a prospect — and `measure_yield.py` — pass `age=0`
  (`age_known=False`). Forcing `age=0` on the aged demo sites inflates pred/actual by
  **×1.33**; restoring real ages collapses 8 of 10 sites to ≈ 1.0.
- But that check is **near-circular**: with 10 sites `age_years` is near-collinear with
  site identity, so the MLP may be using it as a **per-site intercept** (memorising each
  site's level), not learning **degradation**. The implied rate is ~1.4–2.2 %/yr,
  *non-monotonic* at low POA (vs. physical ~0.5–1 %/yr), and `age=0` is an extrapolation
  below the youngest training site (4 yr).

W1 (prospect prediction) serves an arbitrary map point with no install history, so
`age=0` is the **genuine** serving condition — every prospect estimate rides on the
model's `age=0` behaviour. Whether that behaviour is trustworthy is the open question,
and it cannot be settled from Gate A alone because **every validation site is aged**, so
age is confounded with site identity. Resolving that confound is this task.

Scope note: the corpus is already correct, so this is **model-training only**
(`pv-prospect-model` / `pv-prospect-model-trainer`) — no re-transform.

## Launch-decision framing (A / B / C — centring on B)

Three ways to get W1 to a defensible launch state:

- **A — Launch with a documented caveat (cheapest).** Promote the retrained model,
  serve `age=0`, and carry the uncertainty in UI copy ("speculative estimate, N=10
  sites, may overstate by ~X %"). Near-zero engineering; transparent for a portfolio
  demo. Risk: if the `age=0` extrapolation is materially wrong, every prospect number is
  systematically off and the caveat does the work the model should.

- **B — Constrained degradation prior (recommended; this task's deliverable).** Stop
  trusting the freely-learned age coefficient; impose the physics we already know — a
  monotone ~0.5–1 %/yr degradation — and, where the data allows, *identify* the slope
  from within-site temporal variation (below). Makes the `age=0` prediction
  **defensible by construction**, and arguably more correct than the free fit (which is
  overfitting site identity on 10 collinear sites). Launchable without more sites.

- **C — Hold for full validation (highest confidence, slowest).** Don't launch W1 until
  LOSO (and ideally more / younger sites) confirms the age effect is real degradation
  that transfers to an unseen site. This is the honest bar for the product's central
  *prospect* claim; here it is run as the **validator** for B, not as a blocking gate.

**Recommended sequence:** B unblocks W1 honestly and soon; A is one line of copy either
way; C (LOSO) rides alongside as B's validator and the eventual upgrade path.

## Candidate model structure (for B)

Decompose the learned per-site behaviour into a transferable age slope and a
non-transferable site level, instead of letting a single age feature proxy both:

```
CF ≈ α_site  +  β · age  +  f(POA, temp, …)
```

- `α_site` — a per-site intercept (fixed effect / embedding) that absorbs everything
  time-invariant about a site (orientation, shading, soiling baseline, panel tech), so
  it soaks up the site-identity signal that `age` currently proxies.
- `β · age` — forced to carry only what `α_site` can't, i.e. the genuine degradation.

**Identification.** The slope `β` is identified from **within-site variation over
time** — each site spans ~2.2 yr (2024-03 → 2026-06), so its `age` genuinely moves
within its own window. That temporal trend identifies `β` *independently of site
identity* (measured within each site, not by comparing a young site to an old one). The
current single-MLP-with-age-as-a-plain-feature can't do this: with no explicit site
channel, age is its only way to express per-site levels, so it overfits them (hence the
inflated, non-monotonic rate).

**Serving a prospect.** Drop `α_site` (use the population-mean / an "unknown-site"
default) and apply `β · age` at `age=0` — now a controlled extrapolation against a
physically-meaningful slope, not a runaway.

**Avoid the collinearity trap.** Feeding the model *both* site identity and age lets the
more expressive feature (site) starve the age channel, leaving the prospect fallback
undertrained. Force the age channel to carry the transferable part via one of:

- **Embedding dropout** — randomly mask site identity in training so the model must
  sometimes predict from age alone; this literally trains the "unknown-site default
  vector" used for prospects (cleanest).
- **Residualisation order** — fit age first, let the site effect explain only the
  residual, so age gets first claim on the shared variance.
- **Mixed-effects shrinkage** — site as a random intercept, age as a fixed slope;
  shrinkage regularises the 10 per-site levels.

This is the **site-embedding-with-unknown-default** already anticipated in the locked-in
PV-trainer decisions (#10) and in `briefs/cross-site-generalization-eval.md`.

**Honest limit.** 10 sites × ~2 yr gives a *weak* within-site degradation signal
(~1–2 % total CF change per site, against noisy daily CF). The decomposition makes
degradation **identifiable** and stops age from absorbing site identity, but it does not
manufacture data — so in practice it **complements** B's physical prior (use the
within-site estimate to *check* the prior; lean on the prior where the data is too thin)
rather than replacing it. More / younger sites is what eventually lets the data stand
alone.

## LOSO cross-link

`briefs/cross-site-generalization-eval.md` is the **validator** for this task: train the
decomposition on 9 sites, predict the held-out 10th using only its `age` + the
unknown-site default, and score power-space R²/MAPE. A small LOSO-vs-within-site gap
confirms the age slope transfers to an unseen site; a large gap is the trip-wire for the
embedding / a stronger prior. Sequence the two together — LOSO is the instrument that
tells you whether B worked.

## Carried forward from `pv-train-on-served-poa`

Deliberately left open by the completed corpus task; they land here, gated on a model
that passes the above:

- **Promotion** of the retrained PV model (deferred to date) happens here, once B + LOSO
  give a defensible `age=0` prediction.
- **Contingent caveat cleanup** — do **not** touch until promotion: production still
  serves old-basis artifacts, so the caveats are still *true*. Then fix the `app/poa.py`
  docstring (both the "< 1 % self-consistent" note and the "~30 % underestimate /
  data-v2026-05-31 / not yet retrained" caveat), `pv-prospect-app/README.md`, and the
  `/predict` caveat in `main.py`.
- **Flip the `TODO.md` W1 note** and unblock the website's W1 public launch.

Out of scope (separate **Later** task, `briefs/pv-fit-outliers-61272-79336.md`): the
`61272` / `79336` model-*fit* outliers — they lack install dates, so age does not
explain them.

## Acceptance

- The fitted degradation slope is monotone and within (or constrained to) the physical
  ~0.5–1 %/yr band; the non-monotonic low-POA age behaviour is gone.
- LOSO aggregate is close to the within-site number (the age slope transfers to an
  unseen site).
- Gate A on the aged demo sites at their **real ages** returns pred/actual ≈ 1
  (`measure_yield.py`, same window/store as the report).
- Model promoted; caveats cleaned; W1 public-launch note flipped.
