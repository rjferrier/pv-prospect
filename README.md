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

| Key | Description | When it happens |
|-----|-------------|-----------------|
| EW | Weather data extraction | Daily |
| EP | PV data extraction | Daily |
| TW | Weather data transformation | When EW and EP finish |
| TP | PV data transformation | When EW and EP finish |
| L | App data loading | When App starts up |
| V | Data/model versioning | Weekly |
| MW | Weather model training | When V finishes |
| MP | PV model training | When V finishes |

### EW — Weather Data Extraction

Pulls hourly and 15-minute weather data from the **Open-Meteo API** and stages it as CSV to Google
Cloud Storage (GCS). Each run is scoped to a single PV system and date range.
Implemented in `pv-prospect-data-extraction`.

### EP — PV Data Extraction

Pulls historical power readings from the **PVOutput API** and stages them as CSV to GCS.
Each run is scoped to a single PV system and date range.
Implemented in `pv-prospect-data-extraction`.

On GCP both extraction flows are triggered daily by Cloud Scheduler, orchestrated by
Cloud Workflows, and executed as Cloud Run Jobs. Locally they can be driven by the Docker Compose
`runner` service.

### TW — Weather Data Transformation

Cleans and processes staged weather CSVs into versioned Parquet files keyed by lat/lon, ready for
model training. Implemented in `pv-prospect-data-transformation`.

### TP — PV Data Transformation

Cleans and processes staged PV CSVs, joins them with cleaned weather data, and computes
plane-of-array (POA) irradiance via `pvlib`. Outputs versioned Parquet files.
Implemented in `pv-prospect-data-transformation`.

### L — App Data Loading

Loads versioned Parquet data and trained model weights into the application at startup.

### V — Data/Model Versioning

Snapshots the processed Parquet data and trained model artefacts on a weekly cadence,
producing a versioned dataset for the next training run.

### MW — Weather Model Training

Trains a neural network to predict weather variables (temperature, DNI, DHI, etc.)
from location and time. Consumes the versioned Parquet data produced by TW.
Implemented in `pv-prospect-model`.

### MP — PV Model Training

Trains a neural network to predict PV power output from POA irradiance and other features.
Consumes the versioned Parquet data produced by TP. Implemented in `pv-prospect-model`.
