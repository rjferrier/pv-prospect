# Clarify end-date semantics in backfill cursors and manifests

`BackfillCursor.next_end_date` is exclusive throughout, so a 28-day window
ending just before 2026-05-15 is serialised as `"end_date": "2026-05-15"` in
manifests and `END_DATE=2026-05-15` in task env vars. Both visually read as
"today is included" when today is in fact excluded — a recurring source of
off-by-one confusion when reading logs or debugging plans.

## Options

### (a) Switch to inclusive-end

Cursors store `next_end_date` as the last *included* date (`2026-05-14`). Callers
add one day when constructing the exclusive-end `DateRange`. Manifests and env
vars read naturally; no more mental +1/-1. Requires touching `BackfillCursor`,
`BackfillPlan`, manifest serialisation, path builders, and every test asserting
on cursor/plan shapes. The GCS cursor JSON files need a one-off migration (or a
read-compat shim).

### (b) Rename to `*_exclusive`

Keep semantics, rename `next_end_date` → `next_end_date_exclusive` in cursor JSON
and `end_date` → `end_date_exclusive` in manifest/env vars. Rename-only, no
arithmetic changes, but longer names everywhere and still requires migrating the
live cursor files.

## Assessment

Option (a) is cleaner long-term; (b) is safer/smaller in the short term.
