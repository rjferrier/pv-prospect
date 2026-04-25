# pv-prospect

A model of photovoltaic (PV) power outputs according to weather in the UK. This may be
useful if you are thinking about installing solar panels and would like to know how much
energy you could get from them.

This is a work in progress.

## About the Model

The model is a pair of deep neural networks trained on the power output data of 10 existing
PV systems and corresponding weather data. The PV data has been kindly uploaded by the
system owners to [PVOutput](https://pvoutput.org/). Historical weather data is available
from [Open-Meteo](https://open-meteo.com/).

To predict energy yield, the model requires the following inputs:

* location (latitude and longitude)
* period (start and end dates)
* solar panel power rating
* azimuthal orientation (angle clockwise from North)
* tilt (angle from the horizontal)

From the location and period, the first neural network will predict a time series of
relevant weather variables such as temperature, direct normal irradiance (DNI) and
diffuse horizontal irradiance (DHI). From irradiance data and the solar panel features,
plane-of-array (POA) irradiance can be calculated. The second neural network will use
POA irradiance and other features to estimate power output over time.

Some caveats:

* The model does not factor in shade from trees and other obstacles. These
  could negatively impact the actual power output.
* The model is trained on systems that may have old or ageing technology.
  Newer technology could mean the actual power output is higher than expected.

## System Architecture

![Architecture diagram](doc/architecture.png)

The pipeline is orchestrated around the following named flows:

| Key | Description           | When it happens    |
|-----|-----------------------|--------------------|
| E   | Data extraction       | Daily              |
| C   | Data cleaning         | When E finishes    |
| P   | Data preparation      | When C finishes    |
| A   | App data loading      | When App starts up |
| V   | Data/model versioning | Weekly             |
| M   | Model training        | When V finishes    |

### E — Data Extraction

Pulls weather data from the **Open-Meteo API** and PV power readings from the **PVOutput API**,
staging them as CSV to a GCS staging bucket (`raw/` prefix). Each run is scoped to a single
PV system and date range. Implemented in `pv-prospect-data-extraction`.

On GCP, extraction is triggered daily by Cloud Scheduler, orchestrated by Cloud Workflows,
and executed as Cloud Run Jobs. Locally it can be driven by the Docker Compose `runner` service.

### C — Data Cleaning

Cleans raw CSVs (column selection, renaming, UTC time synthesis) and writes them as CSV to
the staging bucket (`cleaned/` prefix). All data sources must be cleaned before preparation
can begin. Implemented in `pv-prospect-data-transformation`.

### P — Data Preparation

Reads cleaned CSVs, performs feature selection, downsampling, joins weather with PV data, and
computes plane-of-array (POA) irradiance via `pvlib`. Uses a scatter-gather pattern: worker
jobs write headerless micro-batch CSVs to `prepared-batches/`, then a single assembly step
merges them into cumulative master CSVs in `prepared/` (one file per system/location rather
than one per date). Implemented in `pv-prospect-data-transformation`.

### A — App Data Loading

Loads versioned model data and trained model weights into the application at startup.

### V — Data/Model Versioning

Snapshots prepared CSV data and trained model artefacts on a weekly cadence,
producing a versioned dataset for the next training run.

### M — Model Training

Trains two neural networks from versioned CSV data: one to predict weather variables
(temperature, DNI, DHI) from location and time, and another to predict PV power output from
POA irradiance and other features. Implemented in `pv-prospect-model`.

## Operational Workflows

The automated pipeline is driven by Cloud Scheduler cron jobs that execute Cloud Workflows on a daily basis:

* **Daily Extraction (`pv-prospect-extract`)**: Runs at **02:00 UTC**. Triggers the data extraction workflow for the previous day.
* **PV Site Backfill (`pv-prospect-extract-pv-site-backfill`)**: Runs at **02:40 UTC**. Orchestrates daily PV-site backfill for historical data, covering a 28-day window that marches backwards through history. Extracts PV output sequentially (one Cloud Run Job per site) and weather data in parallel. A **GCS checkpoint** (`resources/pv_site_backfill_checkpoint.json`) is written after each successful site extraction, so a manually re-triggered run resumes from where the previous execution stopped rather than starting over.
* **Weather Grid Backfill (`pv-prospect-extract-weather-grid-backfill`)**: Runs at **03:20 UTC**. Orchestrates the daily grid-point weather backfill via a paced manifest-driven process. Dispatches one Cloud Run Job per batch with a configurable sleep between batches to respect OpenMeteo's 5,000/hour rate limit. A **GCS checkpoint** (`resources/weather_grid_backfill_checkpoint.json`) is written after each successful batch, so a manually re-triggered run resumes from the first incomplete batch.
* **Data Transformation (`pv-prospect-transform`)**: Runs at **04:00 UTC**. Orchestrates the data cleaning and preparation steps to generate the prepared datasets for all data extracted and backfilled earlier in the day.

> **Scheduling rationale**: The daily extraction and PV site backfill both use the PVOutput API, which rate-limits at 300 requests/hour. With individual Cloud Run Job tasks capped at 30 minutes, a 40-minute gap between each PVOutput-using workflow prevents concurrent API calls that could breach this limit. The weather grid backfill uses OpenMeteo exclusively and does not conflict with PVOutput, but the 40-minute gap from the PV site backfill also gives the prior workflow time to complete cleanly before the transformation step starts.

### Trigger the Pipeline Manually (Optional)

You can trigger an ad-hoc run using the `gcloud` CLI:

```bash
gcloud workflows run pv-prospect-extract \
  --location=europe-west2 \
  --data='{"pv_system_ids": [12345], "date": "2025-06-24"}'
```

#### Resuming the PV Site Backfill after a timeout

If `pv-prospect-extract-pv-site-backfill` is interrupted (e.g. the Cloud Workflows execution times out or is cancelled), simply re-trigger it:

```bash
gcloud workflows run pv-prospect-extract-pv-site-backfill \
  --location=europe-west2
```

The workflow will read the checkpoint file at
`gs://<staging-bucket>/resources/pv_site_backfill_checkpoint.json` and
skip any PV systems that already completed successfully, logging
`"Resuming from checkpoint — N sites already done"`. No duplicate
extraction occurs. The checkpoint is deleted automatically once the full
run completes so the next scheduled run starts from scratch.

#### Resuming the Weather Grid Backfill after a timeout

The same pattern applies to `pv-prospect-extract-weather-grid-backfill`:

```bash
gcloud workflows run pv-prospect-extract-weather-grid-backfill \
  --location=europe-west2
```

The workflow reads
`gs://<staging-bucket>/resources/weather_grid_backfill_checkpoint.json`,
skips any batches (indexed by position in the manifest) that already
completed, and resumes from the first outstanding batch — including
honouring the configured inter-batch sleep so the rate limit is
respected. The checkpoint is deleted on success.
