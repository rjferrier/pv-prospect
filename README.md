# PV Prospect

A model of photovoltaic (PV) power outputs according to weather in the UK. 
The website can be accessed [here](https://pv-prospect-app-iwge6y4a5q-nw.a.run.app/).

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

**Uncertainty:** the annual yield estimate carries a ±17 % (1σ) uncertainty floor,
calibrated by leave-one-site-out cross-validation across the 10 training sites. It
captures site-to-site variability but excludes weather-year fluctuations and
real-world variation in shading, soiling, and roof condition. The training corpus is
self-selected by motivated system owners, so estimates are optimistic for an
arbitrary installation.

## System Architecture

![Architecture diagram](doc/architecture.png)

The pipeline is orchestrated around the following named flows:

| Key | Description                      | When it happens    |
|-----|----------------------------------|--------------------|
| E   | Data extraction                  | Daily              |
| C   | Data cleaning                    | When E finishes    |
| P   | Data preparation (featurisation) | When C finishes    |
| A   | App data loading                 | When App starts up |
| V   | Data/model versioning            | Weekly             |
| M   | Model training                   | When V finishes    |

Each data source — here the Open-Meteo weather API and the PVOutput API —
has its own extraction (E) and cleaning (C) flow. Preparation (P)
then builds a *feature set* per model — here the weather model and the PV
model. The source-to-model mapping is not one-to-one: a feature set may draw
on several cleaned sources. The PV model's features join cleaned PV power with
cleaned on-site weather, whereas the weather model's features come from the
weather source alone — so the weather source feeds both feature sets.

### E — Data Extraction

Pulls weather data from the **Open-Meteo API** and PV power readings from the
**PVOutput API**, staging them as CSV to a dedicated raw-data archive bucket
(`gs://pv-prospect-raw`, separate from the working `staging` bucket — see
"Raw Data Archive" below). Each run is scoped to a single PV system and date
range. Implemented in `pv-prospect-data-extraction`.

On GCP, extraction is triggered daily by Cloud Scheduler, orchestrated by Cloud
Workflows, and executed as Cloud Run Jobs. Locally it can be driven by the Docker
Compose `runner` service.

### Raw Data Archive

Raw extracted CSVs live in `gs://pv-prospect-raw`, a bucket dedicated to durable
archival rather than working storage — kept separate from `staging` so its
unbounded growth (weather-grid densification, historical backfills) doesn't
churn alongside the `cleaned/`/`prepared/` data that is rewritten on every run.
Raw is **not** DVC-versioned: it is append-only and non-refetchable from the
source APIs, so what it needs is durability, not git-tag reconstructability. A
GCS lifecycle rule tiers each object from Standard to **Coldline** storage class
in place (same path, no cross-bucket move) once it reaches **8 days old** —
comfortably after the daily transform has consumed it (≤1–2 days), while still
leaving a margin for pipeline catch-up after a short outage. Both extraction
(write) and transform (read) reach this bucket through the same
`staged_raw_data_storage` configuration key, so it is the only raw-data path the
pipeline knows about.

### C — Data Cleaning

Cleans raw CSVs (column selection, renaming, UTC time synthesis) — reading from
the raw archive bucket — and writes them as CSV to the staging bucket
(`cleaned/` prefix). All data sources must be cleaned before preparation can
begin. Implemented in `pv-prospect-data-transformation`.

### P — Data Preparation

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

### A — App Data Loading

At startup, `pv-prospect-app`:

1. Downloads the promoted model artifacts from the model store
   (`gs://pv-prospect-versioned-model/promoted/` in production, or a local directory
   for development) and loads them into memory. The store is written by the model
   trainer's promote step (see M below). A failed model load is fatal — without models
   there is nothing to serve.
2. Loads the rolling 90-day validation window from
   `gs://<staging-bucket>/data/served/validation-window/` (see P above). A failed
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

### V — Data Versioning

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

### M — Model Training

Triggered automatically by the version workflow (V) on success. The model trainer
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
  per-site level floor, calibrated by the cross-site LOSO eval — see the
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
[`doc/workflows.md`](doc/workflows.md) for detailed descriptions and
[`doc/backfill-operations.md`](doc/backfill-operations.md) for operational
runbooks (manual triggers, failure recovery).

| Workflow | Schedule (UTC) | Description |
|---|---|---|
| `pv-prospect-extract` | Daily 02:00 | Daily data extraction for the previous day |
| `pv-prospect-extract-pv-sites-backfill` | Daily 02:40 | PV-sites historical backfill (28-day rolling window) |
| `pv-prospect-extract-weather-grid-backfill` | Daily 03:20 | Weather-grid historical backfill (14-day rolling window) |
| `pv-prospect-transform` | Daily 05:30 | Daily data cleaning and preparation |
| `pv-prospect-daily-transform-pv-sites-backfill` | Daily 06:00 | PV-sites transformation backfill |
| `pv-prospect-daily-transform-weather-grid-backfill` | Daily 08:00 | Weather-grid transformation backfill |
| `pv-prospect-version` | Weekly Sun 23:00 | Data versioning + model training |
| `pv-prospect-app` | Always on (Cloud Run Service) | Prediction API and website |

### Deploying the App & Going Public

See [`pv-prospect-app/README.md`](pv-prospect-app/README.md#deployment) for build and deploy instructions.

