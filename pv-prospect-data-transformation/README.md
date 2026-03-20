# Data Transformation

Data transformation pipeline for PV Prospect. This pipeline processes raw data through cleaning and processing stages to produce datasets for model training.

## Pipeline Stages

The transformation process consists of six steps, organised into three layers:

### Cleaning (raw → cleaned)

#### `clean_weather`
- **Input**: Raw OpenMeteo historical weather CSV (`raw/`).
- **Role**: Selects columns for a given weather model (e.g. `best_match`), strips the model suffix from column names, drops excluded columns (e.g. `pressure_msl`, `wind_direction_80m`), and optionally downsamples time resolution.
- **Output**: Cleaned weather data (CSV, `cleaned/`).

#### `clean_pv`
- **Input**: Raw PVOutput CSV (`raw/`).
- **Role**: Synthesises a UTC `time` column from `date` and `time` columns (converting from UK local time), retains only `time` and `power`, and drops rows where `power` is NaN.
- **Output**: Cleaned PV output data (CSV, `cleaned/`).

### Preparation (cleaned → prepared-batches)

#### `prepare_weather`
- **Input**: Cleaned weather CSV (`cleaned/`).
- **Role**: Selects a subset of weather features (e.g. `temperature`, `direct_normal_irradiance`, `diffuse_radiation`), injects latitude/longitude, and downsamples time resolution. Output is associated with a location (latitude/longitude), not a PV site, to support a large sample space for weather-model training.
- **Output**: Headerless micro-batch CSV (`prepared-batches/weather/{location_id}_{date}.csv`).

#### `prepare_pv`
- **Input**: Cleaned weather CSV + cleaned PV output CSV (`cleaned/`).
- **Role**: Inner-joins the two cleaned datasets on `time`, calculates Plane of Array (POA) irradiance using `pvlib` (accounting for panel tilt, azimuth, and area fraction), selects the final feature set (e.g. `temperature`, `plane_of_array_irradiance`, `power`), and downsamples time resolution.
- **Output**: Headerless micro-batch CSV (`prepared-batches/pv/{system_id}_{date}.csv`).

### Assembly (prepared-batches → prepared)

The prepare steps fan out across dates in parallel. Once all batches are written, a single-threaded assembly step merges them into cumulative master CSVs. This avoids per-date format overhead (cleaned data is tens of rows per file).

#### `assemble_weather`
- **Input**: All weather batch CSVs (`prepared-batches/weather/`).
- **Role**: Merges batches with any existing master, deduplicates on `(latitude, longitude, time)` keeping the latest value, sorts, and deletes consumed batches.
- **Output**: Cumulative weather CSV (`prepared/weather.csv`).

#### `assemble_pv`
- **Input**: Batch CSVs for a single PV system (`prepared-batches/pv/{system_id}_*.csv`).
- **Role**: Merges batches with any existing master for that system, deduplicates on `time` keeping the latest value, sorts, and deletes consumed batches.
- **Output**: Per-system cumulative CSV (`prepared/pv/{system_id}.csv`).
