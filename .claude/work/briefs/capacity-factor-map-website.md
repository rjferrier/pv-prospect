# Show the capacity-factor map on the website (via the staging bucket)

## What

Surface the `pv-prospect-map` annual-mean capacity-factor render
(`capacity_factor_map.png`) on the public website, served **out of the staging
bucket at runtime** — the same path the app already uses for `pv_sites.csv` —
rather than baked into the deploy image.

The map is currently an offline artifact only: `pv-prospect-map` writes it to a
local `--out-dir` and nothing references it. The website's `#prediction-map` /
`#validation-map` are unrelated interactive Leaflet widgets, not this render.

## Why

The PNG is a **generated, model-dependent** asset that changes whenever the PV
model is retrained — it is not source. The codebase already distinguishes two
kinds of front-end asset:

- **Image-baked static** (`app/static/`: favicons, the mark SVG, JS/CSS) — fixed,
  versioned with the code, served from the mounted `/static` dir.
- **Bucket-hosted runtime resources** (`gs://pv-prospect-staging/resources/`,
  e.g. `pv_sites.csv`) — read at startup via `filesystem_for(config.resources_dir)`.

The map belongs with the second group: keeping it out of the image means its
refresh cadence is decoupled from app deploys, and a regenerated render does not
bloat or churn the container. This mirrors the existing resources pattern instead
of inventing a third mechanism.

## What is needed

**Publish side** (get the PNG into the bucket):

1. Decide the bucket location (see Open decisions) and publish the generated
   `capacity_factor_map.png` there.
2. Near-term this can be a manual / scripted `gcloud storage cp` of a locally
   generated render; the longer-term automation question (CI job that runs
   `capacity-factor-map`) is deferred — generation needs a model store and the
   elevation API, so it is not a trivial CI step (see `briefs/elevation-api.md`).

**Serve side** (app surfaces it to the browser):

3. Add a FastAPI route (e.g. `GET /assets/capacity-factor-map.png`) that returns
   the PNG read through the same storage filesystem the app already uses for
   resources (`filesystem_for(...)`), fetched once at startup and cached in memory
   like the `pv_sites.csv` load in `lifespan` — so no public-bucket exposure and
   no per-request bucket read.
4. Add the image to `index.html` in a suitable section/view (a "resource map"
   panel), with a short caption linking it to the model's caveats (it is a
   relative resource surface, not a per-site yield — see the map package README
   and the `pv-age-feature` report band).

## Open decisions

- **Bucket prefix.** Reuse `resources/` (one place the app already reads) vs a
  dedicated `assets/` prefix (keeps `resources/` CSV-only, cleaner cache/ACL
  semantics). Leaning `assets/` with a matching `config.assets_dir`.
- **Publish automation.** Manual/scripted upload now (recommended) vs extending
  the instance-repo `upload-static.yml` Action — note that Action currently globs
  `data/static/*.csv` only and lives in the **outer** repo, and wiring it to the
  PNG would mean committing a regenerated binary into git. Defer until the
  regeneration story is decided.
- **Staleness.** Startup-fetch means a new upload is picked up on the next app
  restart/deploy; a TTL re-fetch is probably overkill given the map only changes
  on model retrain. Confirm restart-to-refresh is acceptable.
- **UI placement.** Which view/tab the map lives in, and copy for the caption.

## Cross-repo note

Serve route + UI + any publish helper are **submodule** (`pv-prospect-app` /
`pv-prospect-map`) work; only the optional `upload-static.yml` change touches the
**outer** instance repo. Keep the binary out of the deploy image either way.

## Blockers

None hard. A current-spec render must exist to publish (the `capacity-factor-map`
entry point produces one; see its README for the store requirement). The
elevation-cost / regeneration-automation question is related but separable
(`briefs/elevation-api.md`).
