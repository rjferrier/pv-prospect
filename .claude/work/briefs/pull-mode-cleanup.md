# After the pull-mode bake-in (target: ~1 week from cutover)

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

## Post-merge cleanup

After merging, also stop writing `data/cleaned/` for the backfill —
happens automatically (no code touches it from the pull path).

### Minor SPLIT_BY tidy-up

Fold in while the area is open:

- Drop the redundant `SPLIT_BY='day'` from
  `pv_backfill.py::_pv_task_env` — `default_split_period(PVOUTPUT)`
  already returns `Period.DAY`.
- Prune the `SPLIT_BY` env-var passthrough in
  `entrypoint.py::_run_plan_extract` and its inclusion in `base_env`
  in `_extract_into_collectors`. No deployed scheduler sets it; the
  per-source default is correct for every production path.
- The `Period.WEEK` re-split branch in `_final_ranges`
  (`entrypoint.py:207–208`) is dead code under the current data
  sources — no deployed task sets `SPLIT_BY=week`, and the only
  source that would trigger it (a non-multi-date weather source)
  doesn't exist. Remove with the env-var passthrough.
- Keep `default_split_period` and the `--split-by` CLI flag in
  `runner.py` — that's a real local affordance for one-off
  `docker compose run` invocations.

Pull-mode's read layout (window-spanning weather file + per-day PV
files) matches the per-source defaults exactly, so removing the
override mechanism cannot drift it.
