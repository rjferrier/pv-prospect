# Plan — Productionise the PV and weather models

Detailed design for [`briefs/productionise-models.md`](../briefs/productionise-models.md).
Ordered so the **Prediction API (item 2)** can be built and demoed first against a
manually-trained artifact, with automated retraining and monitoring layered on
after.

---

## 0. Target architecture (delta over today)

Today everything in GCP is **Cloud Run Jobs** fanned out by **Cloud Workflows** and
triggered by **Cloud Scheduler**. Productionisation adds:

```
                        weekly (Sun 23:00)
  Cloud Scheduler ──► version Workflow ──► data-versioner Job   (existing)
                                   │            └─ tag data-v<date>, dvc push (feature)
                                   ▼  (new 2nd step, on success)
                          model-trainer Job        (new — item 3)
                            ├─ checkout data-v<date>, dvc pull (feature)
                            ├─ train pv + weather, evaluate
                            ├─ promotion gate vs incumbent critical metric
                            ├─ if pass: dvc add+push (model remote), tag model-v<date>
                            └─ push metrics to Cloud Monitoring   (item 4)

  client ──► Cloud Run SERVICE  pv-prospect-app   (new — item 2, the key objective)
               ├─ at start-up: load promoted model artifacts from model DVC store (item 1)
               ├─ weather model → POA → PV model → clamp → integrate
               └─ /healthz, /version, /predict
```

New DVC remote `model` → `gs://<prefix>-versioned-model` (mirrors the existing
`raw`/`feature` split; per-output `remote:` field as the instance `CLAUDE.md`
requires).

---

## 1. Deploy the models in GCP (model store)

**Approach: DVC + git tag, atop a GCS remote** (decision taken). The data-versioner
already proves this pattern; reuse it.

**One bucket, two roles** (resolves the DVC-vs-thin-serving-image tension — this is
the hybrid the user agreed to, "DVC atop storing the models in a bucket"):

* **Lineage role — DVC + `model-v<date>` git tag.** Content-addressed, git-tagged,
  fully reproducible. This is the *system of record*; it is how you recreate exactly
  which model + data produced any prediction. Requires git + DVC to resolve, so it
  is used by the *trainer*, not the serving path.
* **Serving role — a plain (non-DVC) promoted-artifact path + `current.json` in the
  same bucket.** On promotion the trainer *also* writes the raw 4-file artifact to a
  stable, human-readable prefix (`gs://<bucket>/promoted/pv/...`) and updates
  `current.json`. The **Service reads only this** at start-up — no git, no DVC, thin
  image (§2.6). The trainer writes *both* roles atomically on promotion.

* New module-level DVC remote `model` in `.dvc/config` →
  `gs://<bucket_prefix>-versioned-model`.
* Trained artifact is the existing 4-file layout (`model.pt`, `feature_spec.json`,
  `training_config.json`, `eval_report.json`) per model, written under
  `models/pv/` and `models/weather/` in the instance repo, `dvc add`-ed, pushed to
  the `model` remote, and committed with an annotated tag `model-v<date>`.
* **Provenance:** extend the artifact with a `provenance.json` (or fields in
  `training_config.json`) recording: source `data-v<date>` tag, instance-repo git
  SHA, trainer image tag, UTC training timestamp, and the critical-metric value.
  This is what makes a served prediction traceable to an exact model + data.
* **A "current/promoted" pointer.** The latest tag is not necessarily the promoted
  one (promotion gate may reject a retrain). Maintain a small
  `models/current.json` (committed, not DVC-tracked) per model:
  `{"model_version": "model-v2026-06-07", "promoted_at": "...", "critical_metric": 0.82}`.
  The API reads this to know which tag to load.

**Why not Vertex AI:** inconsistent with the serverless-jobs architecture and adds
managed-service cost for no current benefit. Revisit only if managed rollout/canary
becomes a requirement.

**Terraform:** add `module "storage"` output + bucket for the versioned-model
bucket and grant the pipeline SA `objectAdmin` on it (mirrors
`pipeline_versioned_feature`).

---

## 2. Prediction API — `pv-prospect-app` (KEY OBJECTIVE)

New Poetry package `pv-prospect/pv-prospect-app`, Python 3.12, FastAPI + uvicorn,
depending on `pv-prospect-common`, `pv-prospect-etl`, `pv-prospect-model` (for the
nets + `load_artifact`/`load_weather_artifact`), and `pvlib`.

### 2.1 HTTP contract

```
POST /predict
{
  "latitude": 51.5, "longitude": -0.12,
  "start_date": "2026-01-01", "end_date": "2026-12-31",   # day-of-year range; year ignored
  "panels_capacity_w": 4000,
  "azimuth_deg": 180, "tilt_deg": 35,
  "inverter_capacity_w": 3680,        # optional; default = panels_capacity_w (no clamp benefit)
  "install_age_years": 0              # optional; default 0 (new install), age_known=1
}
→ 200
{
  "expected_annual_kwh": 3520.4,
  "monthly_kwh": [ ... 12 ... ],
  "assumptions": {"climatology": true, "elevation_m": 12.0, "model_version": "model-v2026-06-07"},
  "caveats": [...]
}

GET /healthz   → 200 once models loaded
GET /version   → loaded model tags + critical metrics + data-version provenance
```

### 2.2 Inference chain (the substance)

For the requested day-of-year range, grouped by **calendar month** (the weather
model's native resolution):

1. **Elevation lookup** for `(lat, lon)`. **Source-skew warning:** extraction reads
   `elevation` from the Open-Meteo *archive/forecast* response — the **grid-cell**
   elevation the weather model trained on — whereas the standalone `/v1/elevation`
   endpoint returns the **point** 90 m DEM. These diverge most in the Scottish
   Highlands, exactly where elevation is "load-bearing", and a skew here biases
   *temperature* (which flows into the PV model too). **Validation item (its own
   gate, separate from POA §2.3):** compare candidate inference elevations against
   the `elevation` column already present in the prepared weather corpus
   (`weather.py` confirms every partition carries it) for a handful of known
   coordinates. If `/v1/elevation` matches within tolerance, use it (cache by
   rounded coordinate); if not, source elevation the way training did (archive-API
   `elevation`) or interpolate from the corpus grid. Fail closed if unavailable.
   The §2.3 POA gate will *not* catch this skew — POA is DNI/DHI-driven — so it
   needs its own check.
2. **Domain guard.** Reject (HTTP 422) `(lat, lon)` outside the UK bounding box the
   models were trained on — the weather model is UK-only and must not silently
   extrapolate. Reuse the bounding boxes in `pv-prospect-common`.
3. **Weather model** per month: build `(lat, lon, elevation, doy_sin, doy_cos)` for
   the month-midpoint day (mirroring `downsample_to_monthly`'s month-midpoint
   convention), apply the feature scaler, run `WeatherNet`, inverse-transform with
   the target scaler → `(temperature, DNI, DHI)` monthly means.
4. **POA reconstruction** (highest-risk step — see 2.3) → daily-mean
   `plane_of_array_irradiance` for that month, for the caller's `tilt`/`azimuth`.
5. **PV model** per day in the month: features `[day_of_year, temperature(month),
   POA(month), age_years, age_known]`, apply the PV scaler, run `CapacityFactorNet`
   → `capacity_factor`.
6. **Deliver:** `power_w = min(f × panels_capacity_w, inverter_capacity_w)`;
   `daily_kwh = power_w × 24 / 1000` (power is a daily-mean watt figure — confirm
   the training cadence is daily-mean, §2.4); sum days → monthly → annual.

### 2.3 POA reconstruction (the correctness crux)

Training (`prepare_pv._calculate_poa_irradiance`) computed POA on **hourly**
DNI/DHI, then the prepare step time-averaged hourly POA to the daily cadence the PV
model trains on. At inference we only have a **monthly-mean** DNI/DHI scalar from
the weather model, so a single solar-position evaluation would *not* reproduce the
daily-mean-of-hourly-POA the model expects.

**Recommended approach:** reconstruct an intraday shape and average, mirroring
training:
* Use `pvlib` clear-sky (`Location.get_clearsky`) over the representative day to get
  the *shape* of hourly DNI/DHI.
* Scale the shape so its daytime mean equals the weather model's monthly-mean
  DNI/DHI.
* Compute hourly POA with the **shared** `get_total_irradiance(..., model='isotropic')`
  call (§2.5) using the caller's `tilt`/`azimuth`, then average to daily-mean POA —
  exactly the reduction training applied.

**Validation gate (must pass before trusting the API):** for a known training site
and period, the API's reconstructed daily-mean POA must match the prepared
`plane_of_array_irradiance` distribution to a documented tolerance. If the clear-sky
shape proves too crude, fall back to a calibrated zenith-weighting; either way the
acceptance criterion is "inference POA ≈ training POA on held-out known sites".

### 2.4 Cadence confirmation

Confirm from the prepared PV partitions that a row is a **daily mean** (features use
integer `day_of_year`; weather prepared cadence is daily). If it is sub-daily,
adjust the energy integration in step 6 accordingly. This is a 10-minute check on a
real partition before writing the integration.

### 2.5 Shared POA refactor (`system-design.md` consistency)

Extract the POA computation into a single shared function (candidate home:
`pv-prospect-common`, or a small `pv-prospect-transform`-shared module) consumed by
**both** `prepare_pv` and the API. Signature roughly
`poa_irradiance(times, dni, dhi, lat, lon, tilt, azimuth) -> Series`. Without this,
training and inference POA will drift and §2.3's validation will rot. Do this as a
separate refactor commit ahead of the API work.

### 2.6 Serving infra

* New Terraform `module "cloud_run_service"` (the first Cloud Run *Service* in the
  repo) + an `artifact_registry` repo `pv-prospect-app`.
* `min_instances = 0` (scale-to-zero, cheap), `max_instances = 2` (cost cap),
  CPU allocated only during requests, `concurrency` tuned to model latency.
* `allow_unauthenticated` Terraform variable (default `false` = IAM auth for
  private testing; `true` for a public demo). When public, an optional
  `X-API-Key` checked in-app against a Secret Manager value bounds casual abuse.
* Models pulled at start-up: the container clones the instance repo at the promoted
  `model-v<date>` tag and `dvc pull -r model` (or pulls the artifact objects
  directly from the model bucket — simpler, no git in the serving image; decide in
  implementation, leaning "direct object read" to keep the serving image thin).
* `Dockerfile` (torch CPU wheel + pvlib + fastapi/uvicorn). Keep the image CPU-only;
  these nets are tiny.
* **README** (`pv-prospect-app/README.md`) with demo `curl` examples for both the
  authenticated and public modes — explicitly requested.

---

## 3. Scheduled retraining (item 3)

**Trigger:** extend the existing **version Workflow**
(`terraform/modules/version/workflow`) with a second step that, on versioner
success, executes a new **model-trainer Cloud Run Job**, passing
`DATA_VERSION=<date>` (the tag the versioner just created). Chaining in one workflow
(rather than a separate scheduler) guarantees the trainer pins the *exact* snapshot
and runs only after a clean version.

**Trainer job** (new entrypoint; reuse `pv-prospect-model`'s training code):
1. SSH + shallow clone instance repo, `git checkout data-v<date>`.
2. `dvc pull -r feature` the prepared CSVs for that tag.
3. `train_pv` + `train_weather` (existing code) → artifacts + `eval_report`.
4. **Promotion gate** (§4): compare new critical metric to incumbent
   `models/current.json`; promote only if `new ≥ incumbent − tolerance`.
5. If promoted: `dvc add` + `dvc push -r model` artifacts, write `provenance.json`,
   update `models/current.json`, commit, tag `model-v<date>`, push.
   If rejected: do **not** update `current.json`; emit the degradation alert metric
   and log loudly (the previous model keeps serving).
6. Push metrics to Cloud Monitoring (§4).

**Shared git/dvc/ssh refactor (`system-design.md`):** the versioner's
`git_ops`/`dvc_ops`/`setup_ssh` are exactly what the trainer needs. Extract them
into `pv-prospect-etl` and have both the versioner and trainer consume the shared
module — this is the worked example in `system-design.md`, so do it rather than
copy. Separate refactor commit.

**Terraform:** `artifact_registry` repo `pv-prospect-model`, `cloud_run_job`
`model-trainer` (needs `GITHUB_DEPLOY_KEY` secret + `model` bucket `objectAdmin` +
torch-sized memory, e.g. 4Gi), and the workflow second-step wiring. The job timeout
must cover both trainings (smoke run early-stopped at epoch 11 — minutes — so 1800s
is ample).

---

## 4. Metrics & degradation alerting (item 4)

**Be honest about what is observable.** A prospect site never yields near-term
ground truth, so **live prediction accuracy cannot be monitored.** "Degradation"
here means two concrete, buildable things:

1. **Offline critical-metric trend across retrains** — caught at training time by
   the promotion gate. This is the primary degradation signal.
2. **Serving operational health** — latency, error rate, availability — from Cloud
   Run's built-in metrics.

**Critical metric:** PV **clamped-power test R²** (`test_power_space.r2`) — the
end-to-end product fidelity number. Secondary diagnostics emitted but with looser
or no alerting: PV capacity-factor R², weather temperature block-clim RMSE lift vs
IDW. **Do not alert on weather DNI/DHI** — the model does not beat IDW there by
design.

**Emission:** the trainer writes custom metrics to Cloud Monitoring, e.g.
`custom.googleapis.com/pv_prospect/pv_clamped_power_r2`,
`.../weather_temperature_rmse`, plus a `model_promoted{0,1}` gauge.

**Alert policies (Terraform / `google_monitoring_alert_policy`):**
* Critical metric below an absolute floor (e.g. R² < 0.70) **or** a drop of >X vs
  the incumbent → page/notify.
* `model_promoted == 0` on a scheduled retrain (a retrain happened but was rejected)
  → notify.
* Cloud Run Service 5xx rate / p95 latency / no-healthy-instances → notify.

The promotion gate (§3.4) is what turns "alert on degradation" into an *action*: a
degraded retrain never reaches the serving path; it raises an alert and the prior
model keeps serving.

---

## 5. Supporting work (item 5)

* **Input-domain guard** (UK bounding box) — already in §2.2; called out because it
  is a correctness guardrail, not a nicety: the models must refuse out-of-domain
  input rather than extrapolate.
* **Dockerfiles** for `pv-prospect-app` and the model-trainer job; CPU torch wheel.
* **CI image builds** — extend the existing GitHub Actions / `deploy.sh` flow to
  build & push the two new images to Artifact Registry (mirror the
  extract/transform/version image pattern; WIF is already configured).
* **Health/version endpoints** and structured logging consistent with the rest of
  the codebase.
* **End-to-end smoke test:** predict for a known PV site over a past period and
  sanity-check the annual kWh against its actual recorded output — the single most
  valuable integration test, and the acceptance gate for the whole chain.
* **Docs (finalisation, per `documenting.md`):** new package READMEs; update the
  top-level `README.md` operational-workflows table to add the trainer step and the
  Prediction API; document the `model` DVC remote in the instance `CLAUDE.md`
  (per-output `remote:` rule); architecture diagram update (`doc/architecture.puml`)
  to add the Service + trainer nodes.

---

## Suggested phasing (each phase shippable)

1. **Refactors first** (separate commits): shared POA function (§2.5) —
   **DONE**: extracted to a new `pv-prospect-physics` package
   (`compute_poa_irradiance`); `prepare_pv` now delegates to it. A new package
   (rather than `pv-prospect-common`) keeps `pvlib` out of the packages that
   don't need it. Still to do: shared git/dvc/ssh into `pv-prospect-etl` (§3).
   - §2.4 cadence question is also settled: `prepare_pv(timescale_days=1)`
     produces **daily-mean** rows, so the step-6 `× 24 / 1000` integration holds.
2. **Prediction API against a manually-trained artifact** (§2) — the key objective;
   demoable end-to-end with the §2.3 POA validation and §5 smoke test as the gate.
   Manually `train-pv`/`train-weather`, hand-place artifacts in the model store.
3. **Model store + DVC versioning** (§1) made first-class.
4. **Automated retrain chained off versioning** (§3).
5. **Metrics, promotion gate, alerting** (§4).
6. **Demo hardening + docs** (§5): public toggle, README, architecture update.

## Risks / watch-items

* **POA reconstruction fidelity (§2.3)** — the make-or-break risk; gate on it.
* **Cold start** on the Service with `min_instances=0` — first request pays model
  load + clone/pull. Acceptable for a demo; raise `min_instances` to 1 only if the
  demo needs snappy first response (small ongoing cost).
* **Versioner already hangs on exit** (see `briefs/versioner-hang.md`) — chaining a
  second workflow step after it must key off the *job's real success signal*, not
  the flapping "Completed False" badge, or the trainer will never fire.
* **Age extrapolation** — the PV model saw few age≈0 rows; `age_years=0` is a mild
  extrapolation. Note it in `caveats`; revisit if predictions look optimistic.
