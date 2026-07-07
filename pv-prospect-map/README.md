# pv-prospect-map

Offline generation of a **UK annual-mean PV capacity-factor map**. Runs the
production prediction chain (weather model → POA → PV model) over a Cartesian
lat/lon grid masked to the UK landmass, for a fixed panel geometry, and renders a
filled-contour map.

This is an analysis/asset-generation tool, **not** a serving path. It is a
separate package so its plotting/geo dependencies (`matplotlib`, `shapely`,
`pyshp`) stay out of the deployed `pv-prospect-app` image.

## Contents

- [What it computes](#what-it-computes)
- [Elevation](#elevation)
- [The UK landmask](#the-uk-landmask)
- [Usage](#usage)
- [Publishing to the website](#publishing-to-the-website)
- [Caveats](#caveats)
- [Development](#development)

## What it computes

For each land cell, for each of the 12 calendar months:

1. **Weather model** (`pv-prospect-model`) → monthly-mean DNI / DHI / temperature
   from `(latitude, longitude, elevation, day-of-year)`. This is climatological,
   so it needs **no** weather-API calls.
2. **POA reconstruction** (`pv-prospect-physics`, the same
   `reconstruct_daily_mean_poa` the prediction API uses) → daily-mean
   plane-of-array irradiance for the fixed tilt/azimuth.
3. **PV model** (`pv-prospect-model`) → daily-mean **capacity factor**.

The 12 monthly capacity factors are then day-weighted into an annual mean per
cell.

### How it differs from `pv-prospect-app`'s `predict_yield`

It reuses the same primitives, so the map is faithful to `/predict`, but:

- It returns the PV model's raw **capacity factor**, not energy (kWh). Capacity
  factor is panel-rating-independent, so with a fixed tilt/azimuth the map is a
  pure climatological *resource surface*.
- **No inverter clamp** (a per-install AC limit, irrelevant to a resource map).
- `age_years` defaults to **0** — the map is for a new install.

It depends on `pv-prospect-model`, `-physics`, and `-common` only — **never on
`pv-prospect-app`**. Artifacts are loaded directly via `pv-prospect-model`'s
`load_artifact` / `load_weather_artifact`.

## Elevation

Elevation is the chain's only external input (a weather-model feature). It is
fetched in batches from the same Open-Meteo forecast API the app uses, so each
cell gets the grid-cell DEM elevation the weather model was trained on, and the
results are cached to `<out-dir>/elevation_cache.json` so re-runs need no network.

## The UK landmask

The mask is the Natural Earth 10m `United Kingdom` polygon — the same source as
`pv-prospect/uk-geo/uk_geo.py`. The minimal shapefile load is replicated here
(via `pyshp` + `shapely`) because that standalone grid-generation tool is not a
package dependency of the monorepo. The shapefile is cached under
`~/.cache/pv-prospect-geo/`.

The render then makes the contours meet the coast cleanly. A filled contour only
fills a grid quad when all four corners are finite, so masking to land alone
leaves the fill short of the coastline. To fix this the field is first
extrapolated onto a band of *ghost* cells just outside land (nearest-neighbour),
so the coastal quads have values, and the resulting contours are then clipped to
the exact same UK polygon — extending past the boundary, then trimmed back to it.

The fill uses `CAPACITY_FACTOR_CMAP` (a low-to-high blue→teal→amber→sun ramp)
rather than a stock matplotlib colormap, so the render shares the PV Prospect
website's palette — the same gradient the front end's PV-potential legend uses.
Update both together if the brand palette changes.

The output is **bare and transparent**: just the UK silhouette (gradient fill +
white contour lines + a navy coastline), with no axes, title, or colour bar, on a
transparent background. It is built to drop straight onto the website backdrop, so
the page supplies its own gradient legend rather than baking one into the PNG.

## Usage

```bash
poetry install
poetry run capacity-factor-map \
    --store-dir <local-store> \
    --out-dir out/cf-map \
    --resolution 0.2
```

(If a flaky network leaves `poetry install` unable to finish the large torch
wheel, the same entry point runs as
`poetry run python -m pv_prospect.map.entrypoint`.)

Outputs under `--out-dir`:

- `capacity_factor.csv` — per-cell `latitude, longitude, capacity_factor`.
- `capacity_factor_map.png` — the contour render (bare transparent UK silhouette).
- `capacity_factor_map_meta.json` — the min/max contour-level percentages the colour ramp spans (the website gradient legend reads these to label its endpoints).

Options: `--resolution` (degrees, default 0.2), `--tilt` (default 37.5),
`--azimuth` (default 180), `--age-years` (default 0).

### Which store

`--store-dir` is a **local** directory with a `promoted/{pv,weather}` layout
(gs:// stores are not read directly — fetch one locally first). The PV artifact
must match the **current** feature spec: capacity factor from `day_of_year,
temperature, plane_of_array_irradiance`, with age routed structurally and an
**empty** `binary_features`. The older `gate-a` spec carried an `age_known`
binary feature and is **not** compatible.

The genuinely complete, current-spec store is the one the app serves
(`gs://pv-prospect-versioned-model/promoted/`); copy it to a local directory and
point `--store-dir` at it.

To reproduce from the artifacts already in this repo, hand-assemble a store from
the current-spec PV artifact and a weather artifact (these were trained in
separate runs, so this pairing is for local rendering only — it is not a
promoted-together store):

```bash
# from the instance repo root
STORE=.tmp/cf-store
mkdir -p "$STORE/promoted"
cp -r models/pv-age-feature-2026-06-13/pv         "$STORE/promoted/pv"
cp -r models/gate-a-store-2026-06-12/promoted/weather "$STORE/promoted/weather"
# then: --store-dir ../../.tmp/cf-store
```

## Publishing to the website

`pv-prospect-app` serves this render on its home page, read at startup from
`gs://pv-prospect-staging/assets/capacity-factor-map.png` (its `assets_dir` /
`ASSETS_DIR`) — a generated, model-dependent asset kept **out** of the deploy
image, mirroring how `pv_sites.csv` is hosted under `resources/`. Publishing is a
manual upload of the locally generated PNG; a regenerated render is picked up on
the app's next restart.

```bash
# from pv-prospect-map/, after a `capacity-factor-map` run wrote out/cf-map/
gcloud storage cp out/cf-map/capacity_factor_map.png \
    gs://pv-prospect-staging/assets/capacity-factor-map.png
gcloud storage cp out/cf-map/capacity_factor_map_meta.json \
    gs://pv-prospect-staging/assets/capacity-factor-map-meta.json
```

The app's service account already has `objectViewer` on the staging bucket, so no
IAM change is needed. To re-skin the render without re-running the chain (e.g. a
palette change), re-render from the saved per-cell `capacity_factor.csv` by
scattering it back onto the grid axes and calling `render_contour_map` — no model
store or elevation API needed — then upload as above. CI automation of the full
regeneration is deferred (it needs a model store and the elevation API).

## Caveats

The map inherits the PV model's uncertainty (the ±17 % 1σ per-site minimum) and the
optimistic-corpus bias documented in the top-level README — it is a relative
resource surface, not a guaranteed per-site yield.

## Development

```bash
poetry install
poetry run pytest tests/
poetry run ruff check .
poetry run mypy . --ignore-missing-imports --disallow-untyped-defs \
    --explicit-package-bases --namespace-packages
```
