# Data Transformation

Data transformation pipeline for PV Prospect. This pipeline processes raw data through several stages to produce a clean, interpolated dataset for PV power prediction.

## Pipeline Stages

The transformation process is split into three sequential scripts:

### 1. Initial Preprocessing (`transform_1.py`)
- **Inputs**: Raw OpenMeteo historical data and PVOutput power data (`data-0`).
- **Role**: Performs **spatial interpolation** of weather data from the 4 OpenMeteo bounding box vertices down to the exact coordinates of each PV site. Continuous variables (e.g. temperature, wind speed, irradiance) use **bilinear interpolation**; categorical variables (e.g. `weather_code`) use **nearest-neighbour** selection. The interpolated weather data is then joined with the corresponding PVOutput power readings.
- **Output**: Joined weather and power data micro-batches (7-day blocks) in `data-1`.

### 2. Physical Modeling (`transform_2.py`)
- **Inputs**: Spatially interpolated micro-batches from `data-1`.
- **Role**: Calculates Plane of Array (POA) irradiance for each site using `pvlib`, accounting for specific panel orientations (tilt and azimuth). It also filters for a specific weather model. Optionally applies time averaging over a configurable number of days (`TIMESCALE_DAYS`): numeric columns are reduced by time-weighted mean, and categorical columns (e.g. `weather_code`) by time-weighted mode.
- **Output**: Processed micro-batches with POA irradiance in `data-2`.

### 3. Final Concatenation (`transform_3.py`)
- **Inputs**: Processed micro-batches from `data-2`.
- **Role**: Consolidates all micro-batches for each PV site into a single, chronologically sorted CSV file.
- **Output**: Final per-site datasets in `data-3` (format: `{site_id}.csv`).

