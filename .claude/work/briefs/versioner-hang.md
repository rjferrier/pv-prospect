# Investigate the data-versioner hang-on-exit

The data-versioner Cloud Run job completes all its work (dvc add, dvc push,
git commit/tag/push, staging cleanup) within ~90s, then something in the
container fails to exit. The task wall-clock runs until Cloud Run's 30-min
timeout kills it, then Cloud Run retries. The retry's readiness check correctly
raises `ReadinessError: No prepared partition files found to version` because
the first attempt already wiped staging. Final execution status: `Completed False`.

## Impact

The run is misleadingly tagged Failed when in fact the DVC corpus + git tag
pushed cleanly. Every weekly run will burn 29 min of idle compute and flap the
status. **Don't chase the "Failed" badge before reading the logs** — check for
the success sequence (`Deleted N file(s) from prepared staging`) before
investigating further.

When a versioner run shows `Failed`, look for the
`Deleted N file(s) from prepared staging` log line. If present, the versioning
is intact (tag pushed, DVC remote updated). If absent, real failure —
investigate normally.

## Symptoms

- Last log: `Deleted N file(s) from prepared staging` (~90s into run).
- ~29 min of silence.
- `Terminating task because it has reached the maximum timeout of 1800 seconds`
  at ~30 min.
- Cloud Run retries and fails readiness check (staging already empty).

Example observed evidence (2026-05-24T23:00):
- Last log at 20:45:54 (`Deleted 164 file(s) from prepared staging`).
- Cloud Run timeout fired at 21:14:25 (28m 30s of silence).
- Retry's `ReadinessError: No prepared partition files found...` at 21:14:49
  confirms the first attempt successfully wiped.

## Probable cause

A non-daemon thread or unclosed handle preventing process exit. Candidates:
gcsfs background thread, DVC subprocess, an HTTP keep-alive pool.

## Diagnosis sketch

- Add a final log line right before `main()` returns to see if Python
  reaches the end of `main`.
- If yes, the hang is in interpreter teardown — instrument with
  `threading.enumerate()` and `gc.get_objects()` at exit, or call
  `os._exit(0)` deliberately as a sticking-plaster while the real
  cause is hunted down.
- If no, the hang is inside `version_data`. Bisect by logging at
  each section boundary (dvc push, git push, staging cleanup, return).

Not investigated in detail as of 2026-05-25.
