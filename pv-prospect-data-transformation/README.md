# Data Transformation

Data transformation pipeline for PV Prospect. This pipeline processes raw data through cleaning and processing stages to produce datasets for model training.

## Pipeline Stages

The transformation process consists of four scripts, organised into two layers:

### Cleaning (raw → intermediate)

#### `clean_weather`
- **Input**: Raw OpenMeteo historical weather CSV.
- **Role**: Selects columns for a given weather model (e.g. `best_match`), strips the model suffix from column names, drops excluded columns (e.g. `pressure_msl`, `wind_direction_80m`), and optionally downsamples time resolution.
- **Output**: Cleaned weather data (Parquet).

#### `clean_pvoutput`
- **Input**: Raw PVOutput CSV.
- **Role**: Synthesises a UTC `time` column from `date` and `time` columns (converting from UK local time), retains only `time` and `power`, and drops rows where `power` is NaN.
- **Output**: Cleaned PV output data (Parquet).

### Processing (intermediate → versioned)

#### `prepare_weather`
- **Input**: Cleaned weather data.
- **Role**: Selects a subset of weather features (e.g. `temperature`, `direct_normal_irradiance`, `diffuse_radiation`) and downsamples time resolution. Output is associated with a location (latitude/longitude), not a PV site, to support a large sample space for weather-model training.
- **Output**: Prepared weather data (Parquet, versioned).

#### `prepare_pv`
- **Input**: Cleaned weather data + cleaned PV output data.
- **Role**: Inner-joins the two cleaned datasets on `time`, calculates Plane of Array (POA) irradiance using `pvlib` (accounting for panel tilt, azimuth, and area fraction), selects the final feature set (e.g. `temperature`, `plane_of_array_irradiance`, `power`), and downsamples time resolution.
- **Output**: Prepared PV model training data (Parquet, versioned).
