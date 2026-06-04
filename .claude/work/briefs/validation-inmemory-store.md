# App in-memory validation store — startup load + freshness reload

Second task of the Validation API roadmap. Consumes the artifact from
[validation-window-producer.md](validation-window-producer.md); feeds
[validation-api.md](validation-api.md).

## Why

The Validation API serves recent model-vs-actual per site from data held in app
memory (the `InMemoryData` node). This task loads the producer's window artifact
into that memory at startup and keeps it fresh — the consumer half of the
"transformer pushes the artifact, app pulls it" data path.

## What

1. **Startup load (A2).** Mirror the model store's `load_store`: a config-driven
   read (local dir in dev, `gs://…/data/served/validation-window/` in prod, via the
   `pv-prospect-etl` storage the app already uses) of `window.csv` into an app-local
   in-memory repository keyed `system_id → DataFrame`. Keep it beside the model-store
   load in `store.py`, **not** in `pv-prospect-common` (those are domain singletons;
   this is serving state). Per `system-design.md`, the app now has *two* `gs://`
   startup loads — extract the shared `gs://`→temp-dir download from `load_store` into
   a common helper used by both, rather than duplicating it for the window.

2. **Freshness reload (A3).** The app is `min_instances=0, max_instances=2`
   (scale-to-zero, multi-instance), so a background poller is dead weight and a
   single-target push misses the second instance. Instead, lazy generation-checked
   reload: on a `/validate` request, a cheap GCS metadata GET compares the loaded
   object `generation` to the current one; re-download only on change. Each instance
   self-refreshes; "recent days" tolerates request-time latency trivially.

3. **PV-site repo (new app dependency).** To run the PV model on window rows the app
   needs `age_years`/`age_known` (from install date) and `panels_capacity` /
   `inverter_capacity` — all from `pv_sites.csv`, *not* the window. The Prediction
   path takes these from the request, so the app may not build the site repo today.
   Build it at startup via `build_pv_site_repo` from `pv_sites.csv` (already synced to
   `staging/resources/`).

## Scope

`pv-prospect-app` (`store.py`: window loader + repo + generation-checked reload;
`config.py`: `validation_window_dir` env mirroring `STORE_DIR`; startup site-repo
build). Reuse `pv-prospect-etl` storage + `build_pv_site_repo`. The app-SA read grant
on the served prefix lives in
[validation-serving-docs.md](validation-serving-docs.md).
