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

### Repo separation invariant (governs §1–§3)

**`pv-prospect` (this repo, public) is *machinery*; `pv-prospect-instance`
(private) owns the *data and model artifacts*.** The prepared corpus and the
trained-model store both live on the instance side (its repo + private DVC
remotes / GCS buckets); nothing private is committed to `pv-prospect`. Access is
config-driven and follows the path the data-versioner already uses — clone the
instance repo, `dvc pull` the relevant remote. Concretely:

* The **corpus** is obtained by cloning `pv-prospect-instance` at a `data-v<date>`
  tag and `dvc pull -r feature` (the versioner's `DataVersionerConfig` already
  carries `instance_repo_url` / `dvc_remote_name` / `prepared_data_dir`).
* The **model store** (DVC `model` remote + `promoted/` prefix, §1) lives on the
  instance side too; the public serving app is merely *pointed at* its bucket by
  config.
* `pv-prospect` ships the **machinery and a documented bootstrap** to *produce*
  models from a corpus (§3), never the models themselves. The local `.tmp/`
  sample is dev-only scratch, not the corpus.

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
depending on `pv-prospect-common`, `pv-prospect-etl` (storage, to read the
promoted artifacts), `pv-prospect-model`, `pv-prospect-physics` (shared POA), and
`pvlib` (clear-sky reconstruction, §2.3).

> **Prerequisite — produce artifacts via the bootstrap (§3.1) before this runs.**
> The API loads a 4-file trained artifact dir per model (§2.0); the §2.3 POA gate
> + §5 smoke test need a real prepared corpus. Per the §0 separation invariant the
> corpus and artifacts are **instance-private** — not in this public repo
> (`.tmp/prepared/` is dev-only scratch, and in the wrong layout: flat
> `weather.csv` + `pv/<id>.csv` vs the builders' `weather/weather_*.csv` and
> `pv/<id>/pv_*.csv`). So **build the local bootstrap (§3.1) first**, then run it to
> clone `pv-prospect-instance` at a `data-v<date>` tag, `dvc pull -r feature`, and
> `train-pv` / `train-weather` into a local artifact dir the API points at. This
> needs instance access (repo URL + read deploy key + DVC remotes) — confirm those
> are available before starting.

### 2.0 Building blocks already in place (read before coding)

Phase 1 is complete. The inference chain is assembled from existing, tested code
— **reuse it, do not re-derive**; matching training exactly is the whole point.
All symbols below are verified present at the cited paths.

* **POA** — `from pv_prospect.physics import compute_poa_irradiance`:
  `compute_poa_irradiance(times, dni, dhi, location, panel_geometry,
  altitude=ALTITUDE)` returns `poa_global` (W/m²) for one `PanelGeometry`.
  `ALTITUDE = 0` (sea level — do **not** pass site elevation; elevation is a
  weather-model feature only). The caller supplies `times` already
  label-corrected (tz-naive → UTC). The prediction path builds its own intraday
  grid (§2.3), so it applies **no** −30 min shift (that shift is `prepare_pv`'s,
  for its right-labelled hourly data).
* **Model loading** — `from pv_prospect.model.persistence import load_artifact,
  load_weather_artifact`. `load_artifact(dir) -> ModelArtifact` (`.model`
  =`CapacityFactorNet`, `.scaler`, `.feature_spec`, `.eval_report`);
  `load_weather_artifact(dir) -> WeatherModelArtifact` (`.model` =`WeatherNet`,
  `.feature_scaler`, `.target_scaler`, `.feature_spec`). Each artifact is a 4-file
  dir: `model.pt`, `feature_spec.json`, `training_config.json`, `eval_report.json`.
* **Running a model — the exact transform training/eval use; mirror it:**
  * **Weather** (`training/weather.py:59-122`): build one row per calendar month
    with `feature_spec.feature_columns`
    = `[latitude, longitude, elevation, day_of_year_sin, day_of_year_cos]`;
    `feats = scale_features(df, artifact.feature_scaler, list(feature_columns), [])`;
    `scaled = artifact.model(torch.FloatTensor(feats.values)).detach().numpy()`;
    inverse-transform `raw = scaled * target_scaler_scale + target_scaler_mean`
    (or `artifact.target_scaler.inverse_transform(scaled)`), columns
    `= [temperature, direct_normal_irradiance, diffuse_radiation]`.
  * **PV** (`training/pv.py:65-127`): build a frame with `continuous_features`
    = `[day_of_year, temperature, plane_of_array_irradiance, age_years]` and
    `binary_features = [age_known]`;
    `feats = scale_features(df, artifact.scaler, list(continuous), list(binary))`
    (scales continuous, appends binary **unscaled**);
    `cf = artifact.model(torch.FloatTensor(feats.values)).detach().numpy().ravel()`.
    The net output **is** capacity factor (no final activation).
  * `scale_features` / `fit_scaler` live in `pv_prospect.model.splits`. **Do this
    first (directive, not optional):** add `predict_capacity_factor(artifact, df)`
    and `predict_weather(artifact, df)` to `pv-prospect-model` (a new
    `pv_prospect/model/inference.py`, re-exported from the package `__init__`),
    each implementing exactly the transform above, and have the API call them.
    Refactor training/eval to call the same helpers (same commit or an immediate
    follow-up) so one inference path serves both — this is the `system-design.md`
    consistency rule, and it is what prevents the API silently diverging from
    training.
* **Weather feature construction** — reuse from `pv_prospect.model.features.weather`:
  `add_cyclic_day_of_year(df)` (angle = 2π·day_of_year/365.25 → `_sin`/`_cos`) and
  the **month-midpoint** convention from `downsample_to_monthly`: the
  representative `time` for a month is its 1st + 14 days. So per month set
  `time = <month-1st> + 14d`, then derive the cyclic pair from it.
* **PV feature constants** — `CONTINUOUS_FEATURES`, `BINARY_FEATURES`,
  `TARGET_COLUMN` in `pv_prospect.model.features.pv`. Inference defaults:
  `age_years = install_age_years` (default 0), `age_known = 1` (a prospect install
  is known-new).
* **Clamp / delivery** — `from pv_prospect.model.evaluation import
  clamped_power_pred`: `min(cf × panel_capacity, inverter_capacity)`.
* **Critical metric** (for `/version` + the promotion gate) —
  `artifact.eval_report.test_power_space.r2`.
* **Packaging note:** none of the above is re-exported from
  `pv_prospect.model.__init__` today. Per the package-export convention, add the
  symbols the app imports to `pv-prospect-model`'s `__init__` (or the new
  inference module's), rather than importing module paths across packages.

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

1. **Elevation lookup** for `(lat, lon)` — **default recipe:** source elevation
   the same way training did — from the Open-Meteo **archive/forecast** response's
   `elevation` field (the grid-cell value; extraction reads it at
   `extractors/openmeteo.py:330`), cached by rounded coordinate. Fail closed if
   unavailable. **Do *not* default to the standalone `/v1/elevation` endpoint:** it
   returns the **point** 90 m DEM, which diverges from the grid-cell elevation the
   weather model trained on (worst in the Scottish Highlands), silently biasing
   *temperature* (which flows into the PV model too) — and the §2.3 POA gate will
   *not* catch this (POA is DNI/DHI-driven). *Optional sanity check (not a
   prerequisite branch):* for a handful of known coordinates, compare the chosen
   elevation against the `elevation` column in the prepared weather corpus (every
   partition carries it, per `features/weather.py`).
2. **Domain guard.** Reject (HTTP 422) `(lat, lon)` outside the UK bounding box the
   models were trained on — the weather model is UK-only and must not silently
   extrapolate. Reuse the bounding boxes in `pv-prospect-common`.
3. **Weather model** per month: build `(lat, lon, elevation, doy_sin, doy_cos)` for
   the month-midpoint day (mirroring `downsample_to_monthly`'s month-midpoint
   convention), apply the feature scaler, run `WeatherNet`, inverse-transform with
   the target scaler → `(temperature, DNI, DHI)` monthly means.
4. **POA reconstruction** (highest-risk step — see 2.3) → daily-mean
   `plane_of_array_irradiance` for that month, for the caller's `tilt`/`azimuth`.
5. **PV model** per day in the month: build the PV feature frame
   `[day_of_year, temperature(month), POA(month), age_years, age_known]` and run
   the PV transform (§2.0) → `capacity_factor`.
6. **Deliver:** `power_w = clamped_power_pred(cf, panels_capacity_w,
   inverter_capacity_w)` (§2.0); `daily_kwh = power_w × 24 / 1000` (power is a
   daily-mean watt figure — confirmed daily-mean, §2.4); sum days → monthly →
   annual.

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
* Compute hourly POA with the shared `compute_poa_irradiance` (§2.0) for the
  caller's `tilt`/`azimuth` (as a `PanelGeometry`), then average over the day →
  daily-mean POA — exactly the reduction training applied.

**Validation gate (must pass before trusting the API):** for a known training site
and period, the API's reconstructed daily-mean POA must match the prepared
`plane_of_array_irradiance` distribution to a documented tolerance. If the clear-sky
shape proves too crude, fall back to a calibrated zenith-weighting; either way the
acceptance criterion is "inference POA ≈ training POA on held-out known sites".
**Reference POA to compare against:** run `prepare_pv` on a known site (e.g. the
cleaned 82517 CSV in `.tmp/samples/` joined with matching cleaned weather), or use
any prepared `pv/<site>/` partition from the versioned corpus; `.tmp/prepared/
weather.csv` (10 grid points) is a sample for shape inspection.

### 2.4 Cadence confirmation — CONFIRMED (daily mean)

`prepare_pv(timescale_days=1)` (the production default) time-weighted-averages to
1-day rows, so a prepared PV row's `power` is daily-mean watts and
`capacity_factor = power / panels_capacity` is a daily mean. The step-6
`× 24 / 1000` integration is therefore correct as written; no adjustment needed.

### 2.5 Shared POA refactor (`system-design.md` consistency) — DONE

Extracted to the new `pv-prospect-physics` package as `compute_poa_irradiance`
(contract in §2.0); `prepare_pv` now delegates to it. A dedicated package (rather
than `pv-prospect-common`) keeps `pvlib` out of the packages that install `common`
but never compute POA. The API consumes the same function, so training and
inference POA cannot drift.

### 2.6 Serving infra

* New Terraform `module "cloud_run_service"` (the first Cloud Run *Service* in the
  repo) + an `artifact_registry` repo `pv-prospect-app`.
* `min_instances = 0` (scale-to-zero, cheap), `max_instances = 2` (cost cap),
  CPU allocated only during requests, `concurrency` tuned to model latency.
* `allow_unauthenticated` Terraform variable (default `false` = IAM auth for
  private testing; `true` for a public demo). When public, an optional
  `X-API-Key` checked in-app against a Secret Manager value bounds casual abuse.
* **Models loaded at start-up via direct object read (resolved in §1, not a
  choice to revisit):** the Service reads each model's promoted 4-file artifact
  from the `promoted/{pv,weather}/` layout, selected by a `current.json` pointer —
  **no git and no DVC in the serving image** (the DVC/git-tag path is the
  *trainer's* lineage mechanism, §1/§3). Use `pv-prospect-etl` storage to fetch the
  objects, then call `load_artifact`/`load_weather_artifact` (§2.0). Keeps the
  image thin and avoids pulling `dvc`/`gitpython` into the app.
  * **The store location is config-driven** (resolves the phase-ordering gap): a
    **local directory** for dev — which is exactly what the §3.1 bootstrap writes —
    or `gs://<model-bucket>/promoted/...` in prod, both holding the same
    `{pv,weather}/` artifact dirs + `current.json`. So Phase 2's bootstrap already
    yields a readable store; Phase 4 makes the GCS/DVC version first-class **without
    changing this serving contract**. The bootstrap (§3.1) must therefore write the
    `promoted/`-style layout + `current.json`, not just a loose artifact dir.
* `Dockerfile` (torch CPU wheel + pvlib + fastapi/uvicorn). Keep the image CPU-only;
  these nets are tiny.
* **README** (`pv-prospect-app/README.md`) with demo `curl` examples for both the
  authenticated and public modes — explicitly requested.

---

## 3. Model trainer — local bootstrap + scheduled retraining (item 3)

> **Status (decided):** a new `pv-prospect-model-trainer` package, with **only the
> local bootstrap mode (§3.1) implemented in Phase 2**. The scheduled-job mode +
> promotion gate + metrics (§3.2, §4) are deferred to the automation phases — the
> package is their permanent home, so they grow in later without re-homing code.

**Package & two run modes.** The trainer is **machinery**, so it lives in
`pv-prospect` as a new `pv-prospect-model-trainer` package depending on
`pv-prospect-model` (training code), `pv-prospect-versioning` (clone/dvc/git ops),
and `pv-prospect-etl`. It is **not** folded into `pv-prospect-model`: that package
is imported by the serving app and must stay free of the heavy `dvc[gs]` /
`gitpython` stack (the same dependency-isolation reasoning as `pv-prospect-physics`
and `pv-prospect-versioning`). One entrypoint, two run modes:

* **Local bootstrap (the Phase-2 enabler — build this first, §3.1).** Run on a dev
  machine to produce the initial artifacts the API needs: clone
  `pv-prospect-instance` at a `data-v<date>` tag, `dvc pull -r feature`, then
  `train-pv` / `train-weather` (§2.0), writing the `promoted/{pv,weather}/` +
  `current.json` store layout (§2.6) to a local path the API reads; optionally also
  promote to the instance bucket/DVC. No GCP scheduling required.
* **Scheduled job (automation — the rest of this section).** The same flow wrapped
  as a Cloud Run Job, chained off the version workflow, adding the promotion gate
  (§4) and metrics.

### 3.1 Bootstrap deliverable (public instructions + code)

`pv-prospect` must be self-sufficient as *machinery*: shipping a runnable,
documented path to produce models from a corpus (the invariant in §0).

* **Code:** the `pv-prospect-model-trainer` entrypoint, runnable locally (e.g.
  `python -m pv_prospect.model_trainer bootstrap --data-version <date>
  --output-dir ...`), reusing `pv-prospect-versioning` for clone/`dvc pull` and
  `pv-prospect-model` for training, and writing the `promoted/`-layout store +
  `current.json` (§2.6) to `--output-dir` so the API can read it directly.
* **Config / access (instance-side, never committed here):** instance repo URL, a
  read deploy key, and the `feature` (and `model`) DVC remote / bucket names —
  supplied via the same `config-{env}.yaml` + env-var mechanism the versioner
  uses. Document these as required inputs; **fail closed** if absent.
* **Docs:** a "Producing the models" section (top-level `README.md` and/or
  `pv-prospect-model-trainer/README.md`) giving the exact clone → `dvc pull` →
  `train-*` → (promote) sequence and stating the §0 separation invariant.

### 3.2 Scheduled retraining (automation)

**Trigger:** extend the existing **version Workflow**
(`terraform/modules/version/workflow`) with a second step that, on versioner
success, executes a new **model-trainer Cloud Run Job**, passing
`DATA_VERSION=<date>` (the tag the versioner just created). Chaining in one workflow
(rather than a separate scheduler) guarantees the trainer pins the *exact* snapshot
and runs only after a clean version.

**Trainer job** (the `pv-prospect-model-trainer` job mode — the §3.1 bootstrap flow
plus gate + promote + metrics):
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

**Shared git/dvc/ssh refactor (`system-design.md`) — DONE:** the versioner's
`git_ops`/`dvc_ops`/`setup_ssh` are exactly what the trainer needs. Extracted
into a new `pv-prospect-versioning` package (not `pv-prospect-etl`, to keep the
heavy `dvc[gs]` stack out of `etl`'s extraction/transformation consumers); the
data-versioner now consumes it and the trainer will too. The trainer will add
the `git checkout <tag>` + `dvc pull` operations it additionally needs to the
same package.

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

The promotion gate (§3.2) is what turns "alert on degradation" into an *action*: a
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
* **Docs (finalisation, per `documenting.md`):** new package READMEs, including a
  **"Producing the models"** guide for the bootstrap (§3.1); a top-level statement
  of the **repo-separation invariant** (§0 — public machinery vs instance-private
  data/artifacts); update the top-level `README.md` operational-workflows table to
  add the trainer step and the Prediction API; document the `model` DVC remote in
  the instance `CLAUDE.md` (per-output `remote:` rule); architecture diagram update
  (`doc/architecture.puml`) to add the Service + trainer nodes.

---

## Suggested phasing (each phase shippable)

1. **Refactors first** (separate commits) — **DONE**:
   - Shared POA function (§2.5): extracted to a new `pv-prospect-physics`
     package (`compute_poa_irradiance`); `prepare_pv` now delegates to it.
   - Shared git/dvc/ssh (§3): extracted to a new `pv-prospect-versioning`
     package (`setup_ssh`, `clone_instance_repo`, `set_commit_identity`,
     `git_commit_and_tag`, `git_push`, `inject_remote`, `dvc_add_files`,
     `dvc_push`); the data-versioner now consumes it. Placed in its own
     package rather than `pv-prospect-etl` (as originally sketched) because
     `dvc[gs]` is very heavy and `etl` is installed by extraction/transformation
     — the same dependency-isolation reasoning as the physics package.
   - §2.4 cadence question is also settled: `prepare_pv(timescale_days=1)`
     produces **daily-mean** rows, so the step-6 `× 24 / 1000` integration holds.
2. **Predict-helper extraction + model-trainer bootstrap** (§2.0, §3.1) — the
   Phase-2 *enabler*. Add `predict_capacity_factor` / `predict_weather` to
   `pv-prospect-model`; build the `pv-prospect-model-trainer` bootstrap (clone
   instance @ `data-v<date>` → `dvc pull -r feature` → `train-*`) and run it to
   produce the first PV + weather artifacts. Requires instance access (§0 / §3.1).
3. **Prediction API** (§2) against those artifacts — the key objective; demoable
   end-to-end, with the §2.3 POA validation and §5 smoke test as the acceptance gate.
4. **Model store + DVC versioning** (§1) made first-class (trainer promote step,
   `current.json`, `model` remote).
5. **Automated retrain chained off versioning** (§3.2): wrap the same trainer as a
   Cloud Run Job + workflow second step + promotion gate.
6. **Metrics & alerting** (§4).
7. **Demo hardening + docs** (§5): public toggle, READMEs (incl. "Producing the
   models"), architecture update.

## Risks / watch-items

* **POA reconstruction fidelity (§2.3)** — the make-or-break risk; gate on it.
* **Cold start** on the Service with `min_instances=0` — first request pays model
  load + the object fetch of the promoted artifacts from the model bucket (§2.6;
  no clone/pull — that's the trainer's path). Acceptable for a demo; raise
  `min_instances` to 1 only if the demo needs a snappy first response (small cost).
* **Versioner already hangs on exit** (see `briefs/versioner-hang.md`) — chaining a
  second workflow step after it must key off the *job's real success signal*, not
  the flapping "Completed False" badge, or the trainer will never fire.
* **Age extrapolation** — the PV model saw few age≈0 rows; `age_years=0` is a mild
  extrapolation. Note it in `caveats`; revisit if predictions look optimistic.
