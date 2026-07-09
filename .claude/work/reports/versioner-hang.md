# The data-versioner "hang-on-exit"

## Summary

The weekly `data-versioner` Cloud Run job had reported `Completed=False` on
every run since it was introduced (2026-04-10). The task brief attributed this
to a hang during interpreter teardown — a non-daemon thread (gcsfs, DVC
subprocess, HTTP keep-alive pool) preventing process exit after `main()`
returned — and proposed `os._exit(0)` as a sticking plaster.

That diagnosis was wrong. The job never reached teardown, and `main()` never
returned. It was stalling inside `_clean_staging`, which deleted the staging
bucket's `cleaned/` prefix one HTTP request per object. That prefix held
**2,661,924 objects**. At a generous 50 ms per round-trip a full sweep needs
about 37 days; even at 10 ms it needs over 7 hours, against a 30-minute Cloud
Run timeout. The sweep could never have completed, and never did — so
`cleaned/` was never wiped, grew every week, and pushed each successive run
further from finishing.

The fix removes the cleaned sweep from the versioner entirely and expires the
prefix with a GCS lifecycle rule instead. The accumulated backlog is left for
that rule to drain.

## Contents

1\. Introduction\
2\. Evidence against the teardown hypothesis\
3\. The actual mechanism\
4\. Why a lifecycle rule\
5\. Changes made\
6\. Follow-up left open

## 1. Introduction

`briefs/versioner-hang.md` recorded that the versioner "completes all its work
(dvc add, dvc push, git commit/tag/push, staging cleanup) within ~90s, then
something in the container fails to exit", and that the task then ran until the
30-minute Cloud Run timeout killed it. The brief noted the cause was "not
investigated in detail as of 2026-05-25" and sketched a teardown-thread
hypothesis.

This report records what the investigation actually found, why the recorded
hypothesis was wrong, and what was changed. It matters beyond the code diff for
two reasons: the false diagnosis had been propagated into the Terraform
workflow definition as a deliberate `try/except` workaround, and the real cause
left 2.66M objects in the staging bucket that no code change will remove.

## 2. Evidence against the teardown hypothesis

The decisive evidence is log ordering, available in the brief itself.

`version_data` (`core.py`) calls `_clean_staging(prepared_fs, cleaned_fs)` and
only afterwards logs `Versioning complete: <tag>`. `_clean_staging` iterated
`[(prepared_fs, 'prepared'), (cleaned_fs, 'cleaned')]`, emitting
`Deleted N file(s) from <label> staging` at the end of *each* iteration.

The brief's own observed evidence (2026-05-24T23:00) is that the last log line
was `Deleted 164 file(s) from prepared staging` — the **first** iteration — and
that `Versioning complete` never appeared. Therefore:

- the second (`cleaned`) iteration never finished;
- `_clean_staging` never returned;
- `version_data` never returned;
- `main()` never returned;
- interpreter teardown was never reached.

`git show 8f917a6` confirms both loop iterations were present in the code as of
2026-05-12, before the evidence date, so this is not an artefact of the brief
having gone stale.

## 3. The actual mechanism

`GcsFileSystem.delete` (`pv-prospect-etl/.../storage/backends/gcs.py`) issues
one blob-delete HTTP round-trip per object:

```python
def delete(self, path: str) -> None:
    blob = self._bucket.blob(self._blob_path(path))
    blob.delete()
```

`_clean_staging` called it serially, once per listed entry. Object counts at
the time of investigation (2026-07-09):

| Prefix | Objects |
|---|---:|
| `gs://pv-prospect-staging/data/prepared/` | 39 |
| `gs://pv-prospect-staging/data/prepared-batches/` | 0 |
| `gs://pv-prospect-staging/data/cleaned/` | 2,661,924 |

`prepared/` drains in seconds, which is why its log line always appeared.
`cleaned/` cannot: 2.66M serial round-trips is roughly 37 days at 50 ms each.
The job was killed at 30 minutes, mid-sweep, every week.

`list_files` is not implicated — it paginates 1000 blobs per page and completes
in seconds. The cost is entirely in the delete loop (and, secondarily, the
per-folder `rmdir` calls that follow it, which are also one request each).

This was self-reinforcing. Because the sweep never completed, `cleaned/` was
never emptied; because it was never emptied, it accumulated every run's output
and the next sweep started from a larger set.

## 4. Why a lifecycle rule

Two fix shapes were considered: making the delete fast (batched, concurrent
deletion in the shared GCS backend), or not deleting synchronously at all.

The lifecycle rule was chosen. `cleaned/` is intra-run working data — the clean
step writes it and the prepare step reads it back within the same run, and the
clean stage must complete for all sources before prepare begins. Nothing
depends on it surviving across runs, so there is no reason for a compute job to
be spending wall-clock time deleting it. GCS expires objects asynchronously and
at no compute cost, which also drains the existing backlog without a
destructive bulk-delete command.

The rule is `age = 7` days on `matches_prefix = ["data/cleaned/"]`, mirroring
the raw bucket's week of margin: generous enough for pipeline catch-up and
backfill re-runs.

Batched/concurrent deletion in `GcsFileSystem` remains a reasonable improvement
on its own merits, but it would have had to clear the entire 2.66M backlog
inside one 30-minute run on first deploy, which is a much tighter margin than
it appears.

## 5. Changes made

- `terraform/modules/storage/main.tf` — lifecycle rule deleting
  `data/cleaned/**` at age 7. Verified as an in-place bucket update
  (`Plan: 0 to add, 1 to change, 0 to destroy`), not a replacement.
- `pv-prospect-data-versioner/.../core.py` — `_clean_staging` now takes only
  `prepared_fs`; `version_data` no longer takes `cleaned_fs`. The path list is
  materialised once rather than re-derived in a generator expression, which
  reads better but changes no behaviour.
- `pv-prospect-data-versioner/.../entrypoint.py`, `config.py` — dropped the now
  unused `staged_cleaned_data_storage` config key and filesystem handle.
- `terraform/modules/version/workflow/main.tf` — comment corrected; the
  `try/except` wrapper around the versioner step is **retained**. See §6.
- `pv-prospect-data-versioner/README.md` — cleanup section rewritten.

## 6. Follow-up left open

- **The backlog drains asynchronously.** At the time of writing, `data/cleaned/`
  still holds ~2.66M objects. Once the rule is applied, GCS will expire
  everything older than 7 days over the following days. Nothing needs to be run,
  but the drain is not instant and object counts will stay high for a while.
- **HNS folders are not expired.** Lifecycle rules delete objects, not folders,
  and the staging bucket has hierarchical namespace enabled. The `data/cleaned/`
  tree will therefore accumulate empty folders. `pv-prospect-etl/scripts/
  cleanup_empty_folders.py` exists for this and can be pointed at the prefix;
  it is not scheduled.
- **Cost and listing overhead were never measured.** 2.66M small objects sat in
  Standard storage for three months. Whether the accumulated `cleaned/` prefix
  also slowed the daily transformation's listing calls was not investigated.
- **This was never verified against a live run.** The fix is deployed by
  redeploying the versioner image and applying Terraform; the next weekly run
  is the first real test. Expect `Deleted N file(s) from prepared staging`
  followed — for the first time — by `Versioning complete: data-v<date>`.

- **The workflow's `try/except` around the versioner step is deliberately
  retained.** It was introduced to tolerate the (misdiagnosed) failure, and it
  is tempting to remove now that the cause is fixed. Do not, yet. The sweep ran
  *after* `dvc push` and `git push`, so even a timed-out versioner lands a valid
  `data-v<date>` tag — the wrapper is what lets the trainer run at all today.
  More importantly, the brief's original teardown hypothesis (a non-daemon
  gcsfs/DVC/keep-alive thread blocking interpreter exit) was never *disproven*;
  it was untestable, because `main()` never returned to reach teardown. If such
  a hang remains, removing the wrapper converts a cosmetic red badge into a
  trainer that never runs. Remove it only after a live run logs
  `Versioning complete` **and** Cloud Run reports success. If a residual
  teardown hang does appear, `logging.shutdown()` + stream flush + `os._exit(0)`
  after `main()` returns forces exit regardless of lingering threads.
