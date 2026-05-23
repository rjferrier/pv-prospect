# pv-prospect

A model of photovoltaic (PV) power outputs according to weather in the UK. This
may be useful if you are thinking about installing solar panels and would like to
know how much energy you could get from them.

This is a work in progress.

## About the Model

The model is a pair of deep neural networks trained on the power output data of 10
existing PV systems and corresponding weather data. The PV data has been kindly
uploaded by the system owners to [PVOutput](https://pvoutput.org/). Historical
weather data is available from [Open-Meteo](https://open-meteo.com/).

To predict energy yield, the model requires the following inputs:

* location (latitude and longitude)
* period (start and end dates)
* solar panel power rating
* azimuthal orientation (angle clockwise from North)
* tilt (angle from the horizontal)

From the location and period, the first neural network will predict a time series
of relevant weather variables such as temperature, direct normal irradiance (DNI)
and diffuse horizontal irradiance (DHI). From irradiance data and the solar panel
features, plane-of-array (POA) irradiance can be calculated. The second neural
network will use POA irradiance and other features to estimate power output over
time.

Some caveats:

* The model does not factor in shade from trees and other obstacles. These could
  negatively impact the actual power output.
* The model is trained on systems that may have old or ageing technology. Newer
  technology could mean the actual power output is higher than expected.

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
**PVOutput API**, staging them as CSV to a GCS staging bucket (`raw/` prefix).
Each run is scoped to a single PV system and date range. Implemented in
`pv-prospect-data-extraction`.

On GCP, extraction is triggered daily by Cloud Scheduler, orchestrated by Cloud
Workflows, and executed as Cloud Run Jobs. Locally it can be driven by the Docker
Compose `runner` service.

### C — Data Cleaning

Cleans raw CSVs (column selection, renaming, UTC time synthesis) and writes them
as CSV to the staging bucket (`cleaned/` prefix). All data sources must be cleaned
before preparation can begin. Implemented in `pv-prospect-data-transformation`.

### P — Data Preparation

Reads cleaned CSVs, performs feature selection, downsampling, joins weather with PV
data, and computes plane-of-array (POA) irradiance via `pvlib`. The prepared data
is partitioned into **content-named CSV files** under two segregated corpora in
`prepared/`:

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
merges them into the partition files. Implemented in
`pv-prospect-data-transformation`.

### A — App Data Loading

Loads versioned model data and trained model weights into the application at
startup.

### V — Data/Model Versioning

Snapshots prepared CSV data and trained model artefacts on a weekly cadence,
producing a versioned dataset for the next training run.

### M — Model Training

Trains two neural networks from versioned CSV data: one to predict weather
variables (temperature, DNI, DHI) from location and time, and another to predict
PV power output from POA irradiance and other features. Implemented in
`pv-prospect-model`.

## Operational Workflows

The automated pipeline is driven by Cloud Scheduler cron jobs that execute Cloud
Workflows on a daily or weekly basis:

* **Daily Extraction (`pv-prospect-extract`)**: Runs at **02:00 UTC**. Triggers
  the data extraction workflow for the previous day.
* **PV-Sites Extraction Backfill (`pv-prospect-extract-pv-sites-backfill`)**: Runs
  at **02:40 UTC**. Orchestrates daily PV-site backfill for historical data,
  covering a 28-day window that marches backwards through history. Extracts PV
  output sequentially (one Cloud Run Job per site) and weather data in parallel.
  A **GCS position checkpoint** (`tracking/checkpoints/pv_sites_backfill.json`,
  a small `{"next_pv_task_index": N}` document) is written after each dispatched
  PV task, so a manually re-triggered run resumes from where the previous
  execution stopped rather than starting over. The checkpoint is deleted on
  successful completion.
* **Weather-Grid Extraction Backfill (`pv-prospect-extract-weather-grid-backfill`)**:
  Runs once daily at **03:20 UTC**. Orchestrates the daily grid-point weather
  backfill via a paced manifest-driven process: 9 batches dispatched sequentially
  in a single workflow execution, separated by `sleep_seconds_between_batches`
  (default 720 s = 12 min). The in-batch sleep is what keeps the workflow under
  Open-Meteo's 5,000 requests/hour limit — a sliding 60-minute window covers at
  most ~3 batches × 1,330 calls ≈ 3,990 calls. Total wall time ≈ 3 h 24 min;
  the cursor commits once every batch has been attempted, so a workflow-level
  failure rolls back the day cleanly (tomorrow re-plans the same window).
* **Data Transformation (`pv-prospect-transform`)**: Runs at **05:30 UTC**.
  Orchestrates the data cleaning and preparation steps to generate the prepared
  datasets for all data extracted earlier in the day.
* **PV-Sites Transformation Backfill (`pv-prospect-daily-transform-pv-sites-backfill`)**:
  Runs at **06:00 UTC**. Unlike the other pipelines, this one has no Cloud
  Workflow — Cloud Scheduler invokes the `data-transformation` Cloud Run Job
  directly with `JOB_TYPE=run_transform_backfill`. The job plans, runs (with
  internal thread-pool parallelism over the clean → prepare → assemble
  phases), flushes its run ledger to a single consolidated file, and commits
  the marker — all in a single execution. Plans its work from the PV-sites extraction backfill's
  committed task-outcome ledger rather than a cursor of its own: each
  `completed` extraction entry becomes a transform unit over that entry's
  window. A small **consumed-through marker** bounds each run to the next
  `MAX_EXTRACT_RUNS` (default 4) unconsumed extraction ledgers; the marker
  advances at the end of the run only if every phase completes (an exit-2
  terminating error skips the commit). Per-unit failures inside a phase are
  logged-and-swallowed, so a transiently-failed transform task is a recorded
  hole rather than a perpetual retry.
* **Weather-Grid Transformation Backfill
  (`pv-prospect-daily-transform-weather-grid-backfill`)**: Runs at **08:00 UTC**.
  Same pattern as above but planning from the weather-grid extraction
  backfill's ledger (weather-only task graph). Uses an independent marker so
  it can advance separately from the PV-sites transform backfill. The start
  time sits past the weather-grid extract's ~06:44 finish + consolidation, so
  the planner always sees a consolidated ledger to read from.
* **Data Versioning (`data-versioner`)**: Runs weekly at **Sunday 23:00 UTC**.
  Snapshots prepared CSV data and trained model artefacts, producing a versioned
  dataset for the next training run.

> **Scheduling rationale**: The daily extraction and PV-sites extraction backfill
> both use the PVOutput API, which rate-limits at 300 requests/hour. With
> individual Cloud Run Job tasks capped at 30 minutes, a 40-minute gap between
> each PVOutput-using workflow prevents concurrent API calls that could breach
> this limit. The weather-grid extraction backfill uses OpenMeteo exclusively,
> but its 9 daily batches breach the 5,000 requests/hour limit if run in a single
> window — so it is split across two windows 70 minutes apart. The transformation
> workflows do not call any external API, so they can run back-to-back; they are
> scheduled after all extraction runs have safely completed.

### Trigger the Pipeline Manually (Optional)

You can trigger an ad-hoc run using the `gcloud` CLI:

```bash
gcloud workflows run pv-prospect-extract \
  --location=europe-west2 \
  --data='{"pv_system_ids": [12345], "date": "2025-06-24"}'
```

#### Resuming the PV-Sites Extraction Backfill after a timeout

If `pv-prospect-extract-pv-sites-backfill` is interrupted (e.g. the Cloud Workflows
execution times out or is cancelled), simply re-trigger it:

```bash
gcloud workflows run pv-prospect-extract-pv-sites-backfill \
  --location=europe-west2
```

The workflow will read the position checkpoint at
`gs://<staging-bucket>/tracking/checkpoints/pv_sites_backfill.json` and resume
dispatching from `next_pv_task_index`, logging `"Resuming from checkpoint --
starting at PV task N"`. No duplicate extraction occurs. The checkpoint is
deleted automatically once the full run completes, so the next scheduled run
starts from scratch.

#### Re-triggering the Weather-Grid Extraction Backfill after a failure

The weather-grid backfill has **no per-batch checkpoint** — it runs as a single
linear pass that paces 9 batches across ~3 h 24 min and commits the cursor only
once every batch has been attempted. A workflow-level failure (or a deliberate
re-trigger before the scheduler fires) therefore leaves the cursor at
yesterday's position; the next run re-plans the same window from scratch. The
cost of a re-trigger is roughly 3 hours of OpenMeteo budget, but in exchange
there is no checkpoint-shaped coordination surface for concurrent same-day
executions to race on.

```bash
gcloud workflows run pv-prospect-extract-weather-grid-backfill \
  --location=europe-west2
```

#### Resuming a Transformation Backfill after a timeout

The transform backfills run as Cloud Run Job executions (not workflows), so
re-trigger them directly:

```bash
gcloud run jobs execute data-transformation \
  --region=europe-west2 \
  --update-env-vars=JOB_TYPE=run_transform_backfill,BACKFILL_SCOPE=pv_sites
```

(Use `BACKFILL_SCOPE=weather_grid` for the weather-grid backfill.)

The consumed-through marker is only advanced at the end of a successful run,
so a run that crashed before commit re-derives the same plan from the
extraction ledger on re-trigger. If the prior attempt reached its end-of-run
ledger flush, the orchestrator's `filter_remaining_tasks` reads that
consolidated ledger and the re-run skips the finished units; an attempt that
crashed before the flush re-runs every unit — the clean/prepare/assemble steps
overwrite idempotently, so that is safe, just repeated work. To deliberately re-transform history (e.g. after a
feature-spec change), reset the marker at
`gs://<staging-bucket>/tracking/cursors/pv-prospect-transform-<scope>-backfill.json`
to `{"consumed_through": ""}` and the next runs re-derive every task from
the oldest extraction ledger forward.
