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

### Preparation (cleaned → prepared)

#### `prepare_weather`
- **Input**: Cleaned weather CSV (`cleaned/`).
- **Role**: Selects a subset of weather features (e.g. `temperature`, `direct_normal_irradiance`, `diffuse_radiation`), injects latitude/longitude/elevation, and downsamples time resolution. This step runs **only for grid-point weather** — the weather-grid backfill — and its prepared rows feed the `weather/` corpus. PV-site weather is cleaned (for the `prepare_pv` join) but never prepared; it is sparse and not wanted in the weather corpus.
- **Output**: Prepared frames assembled with latitude/longitude/elevation from
  the raw metadata sidecar, ready for merging into `prepared/weather/` partition
  files. This step runs inline within `produce_weather_slice` — no separate
  `assemble_weather` step.

#### `prepare_pv`
- **Input**: Cleaned weather CSV + cleaned PV output CSV (`cleaned/`).
- **Role**: Inner-joins the two cleaned datasets on `time`, calculates Plane of Array (POA) irradiance using `pvlib` (accounting for panel tilt, azimuth, and area fraction), selects the final feature set (e.g. `temperature`, `plane_of_array_irradiance`, `power`, `power_max`), and downsamples time resolution.
- **`power_max` derivation**: The maximum of the native-cadence `pv_df['power']` over each output row's period, looked up *before* the PV is time-weighted-averaged onto weather cadence. Computing it post-reduce would smear sub-hour clipping into the hourly average — an inverter spike lasting a few minutes can be ~5–10× the hour's mean, so the post-reduce max would systematically under-report. Downstream PV-model training uses `power_max` as a censoring flag: a row whose `power_max` reaches the inverter capacity has a biased daily-mean `power` (the inverter has truncated the panel's actual output) and must be dropped from the training set.
- **Output**: Per-day micro-batch CSV (`prepared-batches/pv/{system_id}_{date}.csv`)
  in the daily transform. In the in-container backfill (pull mode), day frames are
  accumulated in memory within `produce_pv_slice` and merged directly into the
  `prepared/pv/{site}/` partition file — no batch CSV is written.

#### Downsampling

`clean_weather`, `prepare_weather`, and `prepare_pv` share a single
`downsample_by_days` helper (in `helpers/data_operations.py`). It
time-weighted-averages each `timescale_days` window and labels each output
row by the **start** of the window it represents — a daily aggregate of
`2026-05-07`'s hourly data is therefore labelled `2026-05-07`, not
`2026-05-08`.

### Assembly (prepared → prepared partition files)

The prepare steps fan out across units in parallel; the assembly step then
merges their output into **content-named partition files** under two
segregated corpora:

```
prepared/
  weather/weather_{start}_{end}_{gv}-{NN}.csv   # grid-point weather
  pv/{site}/pv_{site}_{start}_{end}.csv         # PV power + on-site weather
```

`{start}` is inclusive and `{end}` exclusive (ISO `YYYY-MM-DD`); `{gv}` is the
grid-definition version (`0` now, bumped on a regridding); `{NN}` is the
zero-padded grid-point sample-file index. Unique, content-descriptive names
mean each weekly versioning run *adds* `.dvc` files rather than overwriting
one, so the corpus is retrievable as a whole — see the top-level `README.md`.

**Two hand-off modes.** In the *daily transform*, `prepare` and `assemble`
run as separate Cloud Run tasks, so `prepare_pv` writes batch CSVs to
`prepared-batches/` and `assemble_pv` reads, merges, and deletes them. In the
*in-container backfill*, the whole transform runs in one process per slice:
`produce_weather_slice` and `produce_pv_slice` each read the slice's raw
files, run clean -> prepare -> assemble entirely in memory, and write one
prepared partition file directly. No intermediate `cleaned/` files are
written; independent slices execute in a `ThreadPoolExecutor(max_workers=32)`.

#### `assemble_weather`
- **Input**: The collector's slice for one `(grid_point_sample_index, start,
  end)` (weather-grid backfill only).
- **Role**: Writes that slice as the partition file
  `prepared/weather/weather_{start}_{end}_{gv}-{NN}.csv`, merging into any
  file already at that path, deduplicating on `(latitude, longitude, time)`
  keeping the latest value, and sorting. One `assemble_weather` task runs
  per distinct `(sample, window)` — the planner emits one task each. This
  is load-bearing: the resume filter pools `completed` task hashes across
  every ledger ever written for the workflow, so a single bulk-drain task
  hashed only on `(start, end)` would let one slice's completion mask every
  other slice that shared those dates. Per-task slicing makes the hashes
  disjoint, so each slice's completion is recorded independently.
- **Output**: `prepared/weather/weather_{start}_{end}_{gv}-{NN}.csv`.

#### `assemble_pv`
- **Input**: Batch CSVs for one PV system on one date
  (`prepared-batches/pv/{system_id}_{date}.csv`) in the daily transform, or
  the collector's slice for one `(system_id, start, end)` in the backfill.
- **Role**: Merges, deduplicates on `time` keeping the latest value, and sorts.
  A PV file is named for the range it *actually* covers. The daily transform
  buckets each day into its ISO week (Mon–Sun): the day is merged into that
  week's open file, which grows day by day and is renamed to match the span
  it now holds (the old name removed). The backfill writes one file per
  `(system, window)`. One `assemble_pv` task runs per distinct
  `(system, start, end)` — symmetric to `assemble_weather` above and
  load-bearing for the same reason: a single bulk-drain task hashed only on
  `system_id` would let one slice's completion mask every other slice for
  that system on a later run. The daily path also deletes consumed batches.
- **Output**: `prepared/pv/{system_id}/pv_{system_id}_{start}_{end}.csv`.

### Post-pipeline steps

Two cleanup steps run as separate Cloud Run tasks **after a barrier on the
full prepare phase** (all clean + prepare + assemble tasks complete):

#### `consolidate_logs`

Merges the per-task `.jsonl` ledger shards written during the run into a
single consolidated log file under `tracking/logs/`.

#### `maintain_validation_window`

Reads every PV partition currently in `staged/prepared/pv/`, merges them
into the rolling 90-day serving artifact at
`data/served/validation-window/` (writing `window.csv` + `manifest.json`),
and trims rows older than 90 days before the latest timestamp. The merge
is idempotent: re-running or catching up a skipped day simply deduplicates
on `(system_id, time)`.

The step **fails closed** if the artifact has not been seeded — it never
bootstraps from scratch. See
[`doc/runbooks/seed-validation-window.md`](doc/runbooks/seed-validation-window.md)
for the one-time seed procedure.

### How the stages connect

**Daily transform.** Clean → prepare → assemble run as three **ordered
phases**: every task in one finishes before the next begins.
`build_transform_phases` (in `transform_backfill.py`) enumerates each
`TransformInput`'s tasks and the phase barrier alone orders them. This leaves
one cross-step dependency **implicit**: `prepare_pv` joins cleaned PV power
with cleaned on-site weather, but those come from *separate* tasks — `clean_pv`
and a `clean_weather` from a different `TransformInput` — and `prepare_pv`
reads the cleaned weather back by shared path convention (see `run_prepare_pv`
in `core.py`). Nothing declares that dependency: it holds only because every
clean runs before any prepare and because writer and reader agree on the
cleaned-data path. A `clean_weather` that never ran (weather extraction having
failed for that site) is not flagged here — `run_prepare_pv` finds no file and
skips the day.

**In-container backfill (pull mode).** Each slice task (`produce_weather_slice`
/ `produce_pv_slice`) reads its raw inputs, runs clean → prepare → assemble in
the same call stack, and writes one partition file. The cross-step dependency is
explicit: both `clean_weather` and `clean_pv` are sequential function calls
within `produce_pv_slice`, not separate tasks. There is no phase barrier and no
`cleaned/` write — the cleaned frames live only in memory.
