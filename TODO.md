# Project TODO

## After the pull-mode bake-in (target: ~1 week from cutover)

Merge the `cleanup/remove-push-path` branch, which removes:

- The push-mode transform-backfill handler (`_run_transform_backfill`).
- The `BACKFILL_MODE` env-var dispatch (only one mode left after this).
- `PreparedBatchCollector` and the in-memory prepare→assemble hand-off it
  provided (the pull handler does not use it).
- The grid-point-weather branches of `build_transform_phases`,
  `_run_transform_step`, and the `Transformation` enum
  (`PREPARE_WEATHER` / `ASSEMBLE_WEATHER` and their `run_*` /
  `assemble_*` functions). These were only emitted by the
  weather-grid backfill, which now runs through `_run_transform_backfill_pull`.
- The collector-mode branches of `run_prepare_pv` and `assemble_prepared_pv`
  (push-handler only).

The daily-transform path is preserved end-to-end: `TransformInput`,
`build_transform_phases` (PV-only branches), `_run_one_transform_unit`,
and the batches-mode of `assemble_prepared_pv` all stay.

The cleanup commit is drafted on `cleanup/remove-push-path`; merge after
a stable pull-mode bake-in window with no rollback events.

After merging, also stop writing `data/cleaned/` for the backfill happens
automatically (no code touches it from the pull path).
