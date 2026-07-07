# PV Prospect

A model of photovoltaic (PV) power outputs according to weather in the UK. 
The website can be accessed [here](https://pv-prospect-app-iwge6y4a5q-nw.a.run.app/).

## Contents

- [About the Model](#about-the-model)
- [System Architecture](#system-architecture)
- [App Serving](#app-serving)
- [Operational Workflows](#operational-workflows)
- [Full Documentation](#full-documentation)

## About the Model

PV Prospect estimates the annual energy yield of a UK solar installation. The
result is a **typical-year climatological estimate** — not a forecast for a
specific year, but a long-run average learned from historical data.

Two neural networks are chained with a physics step between them:

1. **Weather model** — given location and time of year, predicts climatological
   values for direct normal irradiance (DNI), diffuse horizontal irradiance (DHI),
   and ambient temperature, learned from
   [Open-Meteo](https://open-meteo.com/) historical data.
2. **Solar geometry** — standard physics converts DNI and DHI into plane-of-array
   (POA) irradiance for the given panel orientation and tilt.
3. **PV model** — predicts daily capacity factor from POA irradiance and
   temperature; a fixed degradation factor (~0.7 %/year) is applied for panel age.
   Trained on generation data from 10 real UK systems shared on
   [PVOutput](https://pvoutput.org/).

**Inputs:**

* location (latitude and longitude)
* panel rating (W) and optional inverter rating (W)
* azimuthal orientation (degrees clockwise from North)
* tilt (degrees from the horizontal)
* installation age (years, optional)

**Uncertainty:** the annual yield estimate carries a minimum ±17 % (1σ) uncertainty
band, calibrated by leave-one-site-out cross-validation across the 10 training sites. It
captures site-to-site variability but excludes weather-year fluctuations and
real-world variation in shading, soiling, and roof condition. The training corpus is
self-selected by motivated system owners, so estimates are optimistic for an
arbitrary installation.

## System Architecture

![Architecture diagram](doc/architecture.png)

The pipeline is orchestrated around the following named flows:

| Key | Description                      | When it happens    |
|-----|----------------------------------|--------------------|
| A   | Data extraction                  | Daily              |
| B   | Data cleaning                    | When A finishes    |
| C   | Data preparation (featurisation) | When B finishes    |
| D   | App data loading                 | When App starts up |
| E   | Data/model versioning            | Weekly             |
| F   | Model training                   | When E finishes    |

Each data source — here the Open-Meteo weather API and the PVOutput API —
has its own extraction (A) and cleaning (B) flow. Preparation (C)
then builds a *feature set* per model — here the weather model and the PV
model. The source-to-model mapping is not one-to-one: a feature set may draw
on several cleaned sources. The PV model's features join cleaned PV power with
cleaned on-site weather, whereas the weather model's features come from the
weather source alone — so the weather source feeds both feature sets.

### A. Data Extraction

Pulls weather data from the **Open-Meteo API** and PV power readings from the
**PVOutput API**, staging them as CSV to a dedicated raw-data archive bucket
(`gs://pv-prospect-raw`, separate from the working `staging` bucket — see
[Raw Data Archive](doc/infrastructure.md#raw-data-archive)). Each run is scoped to
a single PV system and date range. Implemented in `pv-prospect-data-extraction`.

On GCP, extraction is triggered daily by Cloud Scheduler, orchestrated by Cloud
Workflows, and executed as Cloud Run Jobs. Locally it can be driven by the Docker
Compose `runner` service.

### B. Data Cleaning

Cleans raw CSVs (column selection, renaming, UTC time synthesis) — reading from
the raw archive bucket — and writes them as CSV to the staging bucket
(`cleaned/` prefix). All data sources must be cleaned before preparation can
begin. Implemented in `pv-prospect-data-transformation`.

### C. Data Preparation

Reads cleaned CSVs, performs feature selection, downsampling, joins weather with PV
data, and computes plane-of-array (POA) irradiance via the shared
`pv-prospect-physics` package (`pvlib` under the hood) — shared so the prediction
API computes POA identically to training. The PV-side output
also carries `power_max` — the maximum of the native-cadence cleaned PV power over
each output row's period — derived before the time-weighted average to weather
cadence so that sub-hour clipping survives the reduction. Downstream model
training uses it as a censoring flag: rows whose `power_max` reaches the inverter
capacity have a biased daily-mean `power` and are dropped from the training set.
The prepared data is partitioned into **content-named CSV files** under two
segregated corpora in `prepared/`:

* `weather/weather_{start}_{end}_{gv}-{NN}.csv` — grid-point weather, consumed by
  the weather model.
* `pv/{site}/pv_{site}_{start}_{end}.csv` — PV power joined with on-site weather,
  consumed by the PV model.

`{start}` is inclusive and `{end}` exclusive (ISO `YYYY-MM-DD`); `{gv}` is the
grid-definition version (`0` now); `{NN}` is the zero-padded grid-point sample-file
index. Filenames describe the data they hold — there is no temporal version in any
path, since git tags own versioning — so each weekly versioning run *adds* `.dvc`
files rather than overwriting one, keeping the whole corpus retrievable with a
single `git checkout <tag> && dvc pull`. The daily transform fans `prepare_pv` out
across dates as micro-batch CSVs in `prepared-batches/`, then an assembly step
merges them into the partition files. After all prepare steps succeed, a final
`maintain_validation_window` step maintains a **rolling 90-day validation window**
artifact at `data/served/validation-window/` in the staging bucket:

* `window.csv` — all prepared PV rows for the last 90 days across every known site,
  with `system_id` prepended as the first column.
* `manifest.json` — updated timestamp, window bounds, and per-site row counts.

The window is a derived, regenerable serving cache separate from the DVC feature
store. The cutoff is data-relative (`max(time) − 90 days`) so a temporarily-offline
site is not immediately dropped. The `data/served/` prefix lies outside the weekly
clean's blast radius. No new IAM is needed — the pipeline SA already holds
`objectAdmin` on the staging bucket. The artifact must be seeded once before the
workflow step can run (see
[Seeding the Validation Window](pv-prospect-data-transformation/doc/runbooks/seed-validation-window.md)).
Implemented in `pv-prospect-data-transformation`.

### D. App Data Loading

At startup, `pv-prospect-app`:

1. Downloads the promoted model artifacts from the model store
   (`gs://pv-prospect-versioned-model/promoted/` in production, or a local directory
   for development) and loads them into memory. The store is written by the model
   trainer's promote step (see F below). A failed model load is fatal — without models
   there is nothing to serve.
2. Loads the rolling 90-day validation window from
   `gs://<staging-bucket>/data/served/validation-window/` (see C above). A failed
   window load is non-fatal: `/predict` continues serving; `/validate/*` returns 503
   until the cache self-heals on the next request.
3. Builds the in-memory PV-site registry from
   `gs://<staging-bucket>/resources/pv_sites.csv`. Also non-fatal.
4. Loads the annual-mean **capacity-factor map** PNG from
   `gs://<staging-bucket>/assets/capacity-factor-map.png` into memory and serves it
   at `GET /assets/capacity-factor-map.png` for the home-page resource panel. Like
   the registry, this is a generated, model-dependent asset kept out of the deploy
   image, so its refresh cadence is decoupled from app deploys; the load is
   non-fatal (the panel hides itself if the asset is absent). It is produced
   offline by `pv-prospect-map` — see that package's README for the publish step.

A failed model load is fatal; the other three are non-fatal. Items 2–4 read from
the staging bucket via the same `objectViewer` grant, so no new IAM is needed.

Per-request freshness: before each `/validate/{system_id}` call, the app compares
the in-memory manifest's `updated_at` against the live manifest on GCS. If the
producer has written a fresher window since startup, the app reloads it on the spot.
The check reads only the small manifest file, so unchanged requests pay no I/O cost.

How these loaded models serve requests — the prediction and validation flows — is
covered in the **App Serving** section below.

### E. Data Versioning

Snapshots prepared CSV data on a weekly cadence: stages `.dvc` files pointing at
the prepared-feature corpus and pushes them to `gs://pv-prospect-versioned-feature`,
then commits and tags `data-v<date>` in the instance repo. Implemented in
`pv-prospect-data-versioner`.

The versioner is **accumulate-only**: it `dvc add`s the latest staging output onto
the existing corpus (overwriting by path) and never removes a partition. So the
corpus at a `data-v<date>` tag is the union of every producer's output to date, and
**a full re-base onto a new feature convention cannot be produced by re-running the
pipeline** — any un-regenerated window stays on the old basis. That case needs an
explicit clean rebuild: see
[`pv-prospect-data-transformation/doc/runbooks/re-base-corpus.md`](pv-prospect-data-transformation/doc/runbooks/re-base-corpus.md).

### F. Model Training

Triggered automatically by the version workflow (E) on success. The model trainer
(`pv-prospect-model-trainer`):

1. Clones `pv-prospect-instance` at the `data-v<date>` tag and `dvc pull`s the
   prepared corpus.
2. Trains two neural networks (`pv-prospect-model`): a weather model (temperature,
   DNI, DHI from location + time) and a PV model (capacity factor from POA
   irradiance, temperature, age).
3. Runs a **promotion gate** — compares the new model's clamped-power R² against
   the incumbent. If the new model degrades beyond the configured tolerance, it is
   rejected and the incumbent continues serving.
4. If promoted: `dvc add`s the artifacts, pushes to `gs://pv-prospect-versioned-model`
   (DVC lineage), copies the 4-file serving artifacts to the `promoted/` prefix
   (plain GCS, read by A), commits `models/current.json` + `models/provenance.json`,
   and tags `model-v<date>` in the instance repo.

## App Serving

![App serving diagram](doc/app.png)

`pv-prospect-app` is the product's front door. It serves a **no-build demo
website** from `/` — static HTML plus vanilla JS with CDN Leaflet (map) and uPlot
(charts), mounted at `/static` and calling the JSON endpoints same-origin (no
CORS, no build step) — fronting the two serving surfaces as browser tabs:

* **Prediction** — click a UK map point and enter panel parameters (capacity,
  azimuth, tilt, age, optional inverter). The page calls `POST /predict` and
  renders the expected annual yield with its uncertainty band (the ±17 % 1σ
  per-site level minimum, calibrated by the cross-site LOSO eval — see the
  `pv-age-feature` report), a monthly bar chart, and the response's own
  `caveats[]`.
* **Validation** — pick a known site (listed dynamically by
  `GET /validate/sites`) and see predicted-vs-actual daily output over the
  rolling 90-day window from `GET /validate/{system_id}`, with the in-sample
  caveat surfaced verbatim.

Behind the surfaces (diagram above), `POST /predict` takes a location (latitude
and longitude) and a period:

1. The **weather model** predicts a time series of DNI, DHI, and temperature from location and time.
2. POA irradiance is computed from the irradiance components and the user-supplied panel geometry (shared `pv-prospect-physics` package — same computation as used during preparation).
3. The **PV model** estimates capacity factor from POA and temperature, yielding predicted energy output.

`/validate/{system_id}` runs the PV model over the validation window for the
specified site and returns predicted vs. actual power output.

See `pv-prospect-app` for the `/predict`, `/healthz`, `/version`,
`/validate/sites`, and `/validate/{system_id}` endpoints. The committed
`openapi.yaml` is the canonical contract the UI binds to; FastAPI also serves it
live at `/docs`. The service is **public by default** (per-IP rate limiting on
`/predict` and `/validate/*` baked into the image guards against Open-Meteo quota
burn and self-DoS; `/healthz` and `/version` are exempt). To return to private,
set `allow_unauthenticated = false` in `terraform.tfvars` and re-apply.

## Operational Workflows

The automated pipeline is driven by Cloud Scheduler cron jobs. See
[`doc/infrastructure.md`](doc/infrastructure.md#scheduled-workflows) for the full
schedule and per-workflow descriptions, and [`doc/runbooks.md`](doc/runbooks.md)
for operational runbooks (manual triggers, failure recovery, pausing).

### Deploying the App & Going Public

See [`pv-prospect-app/README.md`](pv-prospect-app/README.md#deployment) for build and deploy instructions.

## Full Documentation

This README is the high-level entry point. Deeper documentation is organised as
follows.

**Top-level docs:**

| Document | Contents |
|---|---|
| [`doc/infrastructure.md`](doc/infrastructure.md) | The deployed GCP infrastructure: storage buckets, scheduled workflows, Cloud Run compute, scheduling rationale, and cost. |
| [`doc/orchestration.md`](doc/orchestration.md) | The manifest / ledger / plan-commit machinery that coordinates the extraction and transformation pipelines (shared `pv-prospect-etl` design), and the staging bucket layout. |
| [`doc/runbooks.md`](doc/runbooks.md) | Index of operational runbooks — manual triggers, failure recovery, pausing, replaying and re-basing the corpus, seeding the validation window. |
| [`terraform/README.md`](terraform/README.md) | How to **provision** the infrastructure with Terraform: bootstrap, remote state, and the deploy process. |

**Package docs** — every package directory has its own `README.md` (and some a
`doc/` subtree). The principal ones:

| Package | Role |
|---|---|
| [`pv-prospect-app`](pv-prospect-app/README.md) | Prediction / validation API and demo website; deployment instructions. |
| [`pv-prospect-data-extraction`](pv-prospect-data-extraction/README.md) | Extraction pipeline (PVOutput + Open-Meteo) with backfill cursors. |
| [`pv-prospect-data-transformation`](pv-prospect-data-transformation/README.md) | Cleaning + preparation (featurisation) pipeline. |
| [`pv-prospect-etl`](pv-prospect-etl/README.md) | Storage abstraction and workflow orchestration shared across packages. |
| [`pv-prospect-physics`](pv-prospect-physics/README.md) | Shared solar-geometry / POA physics — used by both training and the prediction API. |
| [`pv-prospect-model`](pv-prospect-model/README.md), [`pv-prospect-model-trainer`](pv-prospect-model-trainer/README.md) | Model definitions and the training / promotion pipeline. |
| [`pv-prospect-data-versioner`](pv-prospect-data-versioner/README.md), [`pv-prospect-versioning`](pv-prospect-versioning/README.md) | Weekly DVC data versioning and the shared git + DVC operations it builds on. |
| [`pv-prospect-map`](pv-prospect-map/README.md) | Offline capacity-factor map asset generator. |

