# Productionise the PV and weather models

Now that `pv-prospect-model` trains both networks and writes artifacts, take them
to production: deploy them in GCP, expose a **Prediction API** that chains them
into an end-user energy-yield estimate, schedule weekly retraining off the
versioned feature store, and monitor model fidelity. The Prediction API is the
key objective; everything else exists to feed and guard it.

See the detailed design in [`plans/productionise-models.md`](../plans/productionise-models.md).

## The two models (inference contracts)

Confirmed from the code, not the prose:

* **Weather model** (`WeatherNet`) — climatology, *not* a forecast.
  * in: `(latitude, longitude, elevation, day_of_year_sin, day_of_year_cos)`
  * out: `(temperature, direct_normal_irradiance, diffuse_radiation)`
  * feature scaler in, target scaler out.
* **PV model** (`CapacityFactorNet`).
  * continuous: `[day_of_year, temperature, plane_of_array_irradiance, age_years]`
  * binary: `[age_known]`; target: `capacity_factor = power / panels_capacity`.

The product the API delivers is `min(f × panels_capacity, inverter_capacity)`
integrated over the requested period — an **expected/typical (climatological)**
yield, so the calendar year in the period is irrelevant; only the day-of-year
range matters.

## Objectives

1. **Deploy the models in GCP** — publish trained artifacts to a DVC-tracked
   model store (git tag + GCS remote), loaded by the serving container at start-up.
2. **Prediction API (key objective)** — a Cloud Run **Service** (new pattern; all
   existing GCP compute is Cloud Run *Jobs*) in a new `pv-prospect-app` package
   that runs the weather → POA → PV chain and returns an energy-yield estimate.
3. **Scheduled retraining** — chain a model-trainer Cloud Run Job off the weekly
   versioning workflow so it trains against the exact `data-v<date>` snapshot the
   versioner just produced.
4. **Metrics & degradation alerting** — emit R²/RMSE per model to Cloud
   Monitoring; critical metric = **PV clamped-power test R²**; gate promotion and
   alert on offline degradation (live accuracy is unobservable — see plan).
5. **Supporting work** — Dockerfiles, CI image builds, input-domain guards
   (UK-only), health/version endpoints, a demo README, and shared-code refactors.

## Decisions already taken

* **Model versioning: DVC + git tag now** (not deferred), atop a GCS DVC remote —
  same machinery as the data-versioner, so models are git-tag-reproducible from
  day one. Add a `model` DVC remote rather than overloading `feature`.
* **Auth: single Terraform toggle** (`allow_unauthenticated`), default
  IAM-authenticated for cheap private testing, flippable to public for a demo.
  Cost bounded by `max_instances` + request-time-only CPU (scale-to-zero); an
  optional in-app API key (Secret Manager) deters casual abuse. API Gateway /
  Cloud Armor is documented as future work, not built now.
* **Critical fidelity metric: PV clamped-power test R²**
  (`eval_report.test_power_space.r2`) — the README's "honest end-user delivery
  metric". Weather secondary = block-climatology RMSE lift vs IDW for
  *temperature* only (DNI/DHI do not beat IDW — do not alert on them).
* **New package `pv-prospect-app`** realises the architecture's "App (A)" node and
  houses the Prediction API.
* **Repo-separation invariant:** `pv-prospect` (public) is *machinery*;
  `pv-prospect-instance` (private) owns the *corpus and model artifacts*. Nothing
  private is committed here; corpus/artifact access is config-driven via the
  instance repo + its DVC remotes (the data-versioner's existing pattern).
* **Bootstrap to produce models (decided requirement):** `pv-prospect` ships
  runnable bootstrap code + a "Producing the models" guide — clone instance @
  `data-v<date>` → `dvc pull -r feature` → `train-*` → artifacts. Whatever form it
  takes must keep the heavy `dvc[gs]` stack out of `pv-prospect-model` (the serving
  app imports that package).

* **Trainer packaging (decided):** a new `pv-prospect-model-trainer` package,
  separate from `pv-prospect-model` (keeps `dvc[gs]` out of the serving app). Only
  the **local bootstrap mode** is built in Phase 2; the scheduled-job mode +
  promotion gate + metrics are deferred to the automation phases, growing into the
  same package without re-homing. See plan §3.

## Hard problems (flagged, designed in the plan)

These are the inference-chain subtleties that make item 2 more than a Cloud Run
wrapper:

* **Elevation at a prospect point.** The weather model *requires* elevation (it is
  "load-bearing"), but a caller supplies only lat/lon. Resolve via the Open-Meteo
  elevation endpoint (same DEM source extraction used), cached by rounded
  coordinate.
* **Monthly-climatology → daily POA reconstruction.** Training computed
  `plane_of_array_irradiance` as the daily mean of *hourly* POA from hourly
  DNI/DHI. The weather model only yields a *monthly-mean* DNI/DHI. The API must
  reconstruct a representative intraday profile and average it so the POA fed to
  the PV model is on the same scale the model trained on. This must be validated
  against prepared POA for a known site — it is the single biggest correctness
  risk.
* **POA code must be shared, not re-derived — DONE.** Extracted to the
  `pv-prospect-physics` package (`compute_poa_irradiance`); `prepare_pv` delegates
  to it and the API calls the same function, so they cannot drift.
* **Age for a not-yet-built panel.** Default `age_years = 0`, `age_known = 1`
  (we *do* know a prospect install is new); expose an optional override.
* **Git/DVC/SSH machinery is duplicated — DONE (for the ops the versioner has).**
  Extracted to a new `pv-prospect-versioning` package (not `pv-prospect-etl`, to
  keep the heavy `dvc[gs]` stack out of etl's extraction/transformation
  consumers); the data-versioner now consumes it. The trainer (Phase 3) will add
  the `git checkout <tag>` + `dvc pull` ops it additionally needs to the same
  package.

## Open question deferred to implementation

* Exact PV-row cadence assumption (daily mean) — confirmed from features but the
  energy integration (daily-mean power → kWh over period) should be validated
  end-to-end against a known site's actual output.
