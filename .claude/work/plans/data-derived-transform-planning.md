# Data-Derived Planning for the Transformation Backfills — plan

> **Companion to `briefs/data-derived-transform-planning.md`**, implementing
> `reports/data-pipeline-retrospective.md` §7 recommendation 1. This plan pins the
> design down against the code as it stands (2026-07-09); §2 corrects two premises
> the brief inherited from the report.

## 1. Target state

| Aspect | Today | After |
|---|---|---|
| Planner input | Extraction consolidated ledgers, bounded by a consumed-through marker | Listings of observed data state: raw present − prepared present |
| Consumed-through markers | Two live marker files under `tracking/cursors/` | Deleted |
| Transform ledger | Authority: `completed_task_hashes()` filters re-runs | Audit trail only; nothing reads it |
| Replay a window | Marker rewind + move ledgers to `superseded-ledgers/` | Delete the prepared partition(s), re-trigger |
| Transform-logic change | No-ops against old `completed` entries unless ledgers superseded by hand | Weather: bump `weather_grid.version` → whole grid corpus re-plans. PV: delete partitions (re-base runbook) |
| Daily transform (Phase 2) | Workflow → plan job → manifest → per-task Cloud Run fan-out → consolidate | Same single-process data-derived run; manifests cease to exist |

Extraction is deliberately excluded (per the system-design consistency rule, this
is a named exclusion, not an oversight): deliberate failure holes mean raw-file
absence cannot distinguish "never planned" from "attempted and failed", so the
extraction ledger and cursors remain authoritative there.

## 2. Corrections to the brief

Two premises need updating against the code:

**(a) The reconciliation surface is `data/prepared/`, not `data/prepared-batches/`.**
The backfill slice producers (`slice_producer.py`) write one window-named partition
file per slice directly to staged `data/prepared/` (`pv_partition_path`,
`weather_partition_path`); no batch files are involved. `data/prepared-batches/` is
the *daily* transform's transient hand-off from `prepare_pv` to `assemble_pv` —
consumed and deleted within the same run — and it disappears entirely if Phase 2
lands. The report's §4 caveat predates the slice migration.

**(b) Staged `prepared/` is not durable, so the set difference needs a second
term.** The data-versioner's step 6 (`_clean_staging`) deletes everything under
staged `prepared/` (and `cleaned/`) after each `data-v` tag. A naive
*raw − staged-prepared* diff would re-plan the entire versioned history every run —
and re-read raw from Coldline while doing it. The durable record of produced
partitions is the **instance repo's `data/prepared/**/*.csv.dvc` tree**, committed
by the versioner. Hence:

```
prepared present  =  staged partitions  ∪  versioned-corpus partitions
work to do        =  candidate slices from raw listings  −  prepared present
```

Both terms are data (the corpus's `.dvc` tree *is* the versioned data's observable
state), so the design stays true to the principle: no side-channel bookkeeping is
consulted.

## 3. Design

### D1. Slice identity from listings (the brief's precondition)

**Raw path anatomy** (all under `gs://pv-prospect-raw/timeseries/`):

| Source | Path shape |
|---|---|
| PVOutput | `pvoutput/{system}/pvoutput_{system}_{YYYYMMDD}[_{YYYYMMDD}].csv` |
| PV-site weather | `openmeteo/{res}/pv-sites/{system}/openmeteo-{res}_{system}_{YYYYMMDD}[_{YYYYMMDD}].csv` |
| Grid weather | `openmeteo/{res}/weather-grid/{lat}_{lon}/openmeteo-{res}_{locid}_{YYYYMMDD}_{YYYYMMDD}.csv` |

plus a `-meta.json` sidecar per CSV. Findings on parseability:

- Date tokens are exactly-8-digit trailing tokens (one for a single day, two for a
  window), so a regex anchored on the tail is unambiguous for UK coordinates
  (filename-friendly location tokens are 5–8 chars and never match `\d{8}` for UK
  lat/lon ranges). `ArbitrarySite.from_id` round-trips the location id;
  `DataSource` flattens `/`→`-` in filenames and its values contain no `_`, so the
  leading token is unambiguous too. Where belt-and-braces is wanted, identity can
  be cross-checked against the subfolder (`{system}` / `weather-grid/{bin}`).
- **Verdict: the existing contract is sufficient — no renaming needed.** The work
  item is an *inverse* of `build_time_series_csv_file_path` (a
  `parse_time_series_csv_file_path` in `pv-prospect-data-sources/paths.py`) with
  round-trip property tests.

**Prepared name anatomy** (staged and corpus alike):

- `weather/weather_{start}_{end}_{gv}-{NN}.csv` — exactly reconstructible from
  (window, sample index, grid version): existence is an exact-name check.
- `pv/{sys}/pv_{sys}_{start}_{end}.csv` — the backfill names by slice window
  (exact-name check works); the daily assembler content-names ISO-week files
  (Phase 2 revisits this, see D6). `_parse_pv_partition_dates` already parses the
  shape; `readiness.py` pins the same regexes independently (by design).
- Stray/legacy names in the corpus listing (e.g. the pre-partition
  `data/prepared/weather.csv.dvc`) are ignored by the same pattern-matching the
  readiness check uses.

**Weather slices**: raw grid files carry per-grid-point locations, but a
`WeatherSlice` is keyed by sample index. The planner loads the sample files
(`sample_file.py`; `count_sample_files` exists) and builds a location → index map.
Sample files are versioned static *inputs* on the resources storage, not tracking
state — consulting them is consistent with the principle.

**Grid version**: raw names don't encode it; the planner constructs prepared names
with the *current* `weather_grid.version`. Bumping the version therefore makes the
whole grid corpus "unprepared" and the planner recomputes it — the regridding
semantics already documented on `weather_partition_path`, now planned automatically.
The report's convention-change hazard becomes a feature on the weather side. PV has
no version token, so PV transform-logic changes use the delete-partitions replay.

**PV slice windows**: PVOutput raw is one file per *day* (backfill and daily
alike); the slice window is delimited by the window-spanning **PV-site weather**
file (`_{start}_{end}` names). Candidate `PVSlice`s are derived from the two-date
weather files; day files map into the window containing them. A window whose
weather extraction failed produces no candidate — the same hole today's planner
leaves (extraction ledger `failed` ⇒ no descriptor). Orphan single-date raw files
belong to the daily transform (Phase 1 ignores them; Phase 2 subsumes them).

### D2. Observing the versioned corpus

Options considered for listing versioned partitions:

- **(A) Shallow-clone the instance repo and list `data/prepared/**/*.csv.dvc`** —
  **recommended**. The repo minus DVC content is small; `pv-prospect-versioning`
  already provides `setup_ssh` + `clone_instance_repo`. Needs a **read-only**
  deploy key secret wired to the data-transformation Cloud Run Job (terraform:
  same pattern as the versioner's key, separate key). One clone per planning run
  per day — negligible.
- (B) Stop wiping staged `prepared/` (staging becomes a full corpus mirror) —
  rejected: the versioner would re-download/re-hash the whole corpus each run
  (or need its own diff), staging deletion becomes corpus-threatening, and the
  "staging is transient" invariant is load-bearing in several docs/runbooks.
- (C) Versioner-maintained corpus-index object on GCS — rejected as authority
  (it is bookkeeping-as-authority again, the exact bug class being removed);
  acceptable later as a regenerable *cache* if clone cost ever matters.
- The DVC `feature` remote is content-addressed — no filenames; unusable.

The clone reads the same branch the versioner pushes (`instance_repo_branch`).

### D3. Diff and staleness semantics

For each scope, per planning run:

1. List raw (one recursive listing per source prefix), parse into candidate slices
   (D1).
2. Build the prepared index: staged `prepared/` listing (with per-object update
   times) ∪ corpus listing (names only).
3. A slice is planned iff its prepared partition is **absent from both**, or the
   **staged** partition is **older than any raw file in the slice** (GCS `updated`
   timestamps).

The staleness clause replaces the causal ordering the consolidated ledger used to
provide: today the transform only sees a window after extraction *consolidates*,
so it never races a half-written extraction; under raw listings it can, and
timestamp staleness closes that race. It also upgrades `partial` partitions
automatically when late raw arrives (the producers' merge-on-rerun semantics
already support exactly this). Versioned partitions are done unconditionally —
refreshing them is a deliberate operation (delete from the corpus; re-base
runbook). Failure holes remain holes: no raw ⇒ no candidate ⇒ no work.

Degradation mode throughout: a wrong/raced listing produces **redundant idempotent
recomputation**, never a silent gap — the property the whole change buys.

### D4. Work cap, ordering, and cost

- `MAX_SLICES` (per-run cap, replacing `MAX_EXTRACT_RUNS`; default ~64 ≳ today's
  4-ledger bite) so a large backlog cannot outrun the Cloud Run Job timeout. A
  timeout mid-run is safe: partitions written so far are visible to the next
  run's diff.
- Deterministic ordering, most-recent window first (recent data is worth more to
  the model).
- Listing cost: the raw tree at full history is O(10⁵–10⁶) objects (incl.
  sidecars) ⇒ ≤ ~1k Class A list-ops per run. Note the ledger scans are no
  longer the benchmark they once were: `reports/ledger-scan-cost.md` cut them
  to a single page each by pushing the workflow-name filter server-side. Plan
  the raw-tree listing to be *scoped* (a prefix or a `start_offset` bound), not
  a full walk filtered afterwards — that is the mistake that report unwinds.
  This task still *removes* the transform backfills'
  `completed_task_hashes()` ledger walks entirely.
- Coldline: planning only **lists** raw (no retrieval fees); only planned slices
  *read* raw. Excluding versioned work from the diff is what keeps routine runs
  from re-reading the archive; large deliberate replays incur retrieval costs —
  say so in the runbook.

### D5. Ledger demotion

Keep `LedgerCollector` + the end-of-run `flush` (audit); keep `record_outcome`
with the existing per-slice hashes (now purely ledger keys). Remove the
`completed_task_hashes()` resume filter from `_run_transform_backfill` — its only
transform-backfill use. No planner reads transform ledgers; `superseded-ledgers/`
mechanics die (existing files stay put for `tracking-restructure` to archive).
`filter_remaining_tasks` itself is shared code used by extraction and the daily
transform — untouched in Phase 1.

### D6. Daily transform assessment (brief item 5)

**Recommendation: adopt, as Phase 2.** The daily transform runs the same work at
smaller scale through strictly more machinery: a Workflow, a plan job writing a
manifest, ~40 per-task Cloud Run executions/day with per-task ledger files, a
consolidation job, and the same unscoped resume scan in its plan step. Under
data-derived planning the daily/backfill split *dissolves on the transform side*:
any raw not yet reflected in prepared gets produced, regardless of which pipeline
extracted it. The daily transform becomes a more frequent invocation of the same
single-process run.

Design points specific to Phase 2:

- **Single-day raw → ISO-week slices.** Day files newer than the newest backfill
  window bucket into nominal ISO-week `PVSlice`s. Name the partitions by the
  *nominal* week window (`pv_{sys}_{mon}_{nextmon}.csv`) rather than today's
  content-named grow-and-rename dance — stable identity, exact-name
  reconciliation, matches the weather partitions' documented rationale. As new
  days arrive, the staleness rule (D3) re-produces the week's partition from raw;
  the merge is idempotent. `_find_pv_partition_for_week` / `_write_pv_partition`
  renaming logic becomes obsolete. Readiness regexes are unaffected (same shape).
  *Transition note:* any open content-named week file in staging is replaced on
  first re-produce (delete-old-on-rename already exists); versioned content-named
  weeklies stay as-is — the planner must treat a week as prepared if a partition
  *covering* it exists (reuse the parse-dates check), not only on exact name.
- **PV-site weather day files** are inputs to the week slices (via the in-memory
  clean), not separately planned outputs. `cleaned/` files and `prepared-batches/`
  cease to be written; the versioner's unassembled-batches readiness check becomes
  vestigial (simplify it in the same change).
- **Post-run steps in-process**: `maintain_validation_window` and the ledger/log
  flush run at the end of the single job. The `pv-prospect-transform` Cloud
  Workflow is deleted; Cloud Scheduler triggers the job directly (terraform:
  `transform/workflow` module removed; the direct-scheduler pattern already exists
  for the backfills). `plan_transform`, `build_transform_phases`,
  `TransformInput`, the manifest write, and the per-task `TRANSFORM_STEP` dispatch
  path are deleted. (Losing the per-task path also loses ad-hoc single-step local
  runs; ad-hoc local invocation becomes slice-shaped — acceptable, note in docs.)
- On landing, **delete `manifest-env-footprint`** (transform manifests no longer
  exist).

Phase 2 is separable: Phase 1 changes nothing the daily transform depends on, and
the daily keeps running unchanged until Phase 2 lands.

## 4. Implementation steps — Phase 1 (backfills)

1. **`pv-prospect-data-sources`**: `parse_time_series_csv_file_path` (inverse of
   the builder) returning (data source, site identity, window); round-trip unit
   tests. Export via the package `__init__`.
2. **`pv-prospect-versioning`**: `list_corpus_partitions(repo_url, branch, key)`
   — shallow clone, return relative paths under `data/prepared/` stripped of
   `.dvc`. Terraform: read-only deploy key secret + git in the data-transformation
   image (if absent).
3. **`pv-prospect-data-transformation`**: new planner module (e.g.
   `processing/data_derived_planner.py`) of pure functions — raw index → candidate
   slices (incl. the location→sample-index map and PV window derivation), prepared
   index (staged + corpus + current grid version), diff with staleness, cap +
   ordering — with the I/O at the entrypoint edge (per the purity rule).
4. **Rewire `_run_transform_backfill`**: plan via the new planner; drop the
   `completed_task_hashes()` filter; keep collectors/flush; `MAX_EXTRACT_RUNS` →
   `MAX_SLICES` (terraform scheduler env payloads updated).
5. **Delete marker machinery**: `ConsumedMarker`, `serialize/deserialize_marker`,
   `load/save_marker`, `_consume_extract_descriptors`,
   `_descriptor_to_*_slice_key`, `extract_workflow_name_for`, the old
   `plan_slices`, and their tests. `CURSORS_PREFIX_OVERRIDE` becomes obsolete for
   shadow runs — remove and update the shadow-run docs.
6. **Tests**: unit for every pure planner function; integration over
   `LocalStorageConfig` with a fabricated raw/staged/corpus layout covering: fresh
   window, already-staged, already-versioned, staleness upgrade, missing-weather
   hole, grid-version bump, cap.
7. **One-time ops on landing**: shadow-run parity check (see §6), then delete the
   two marker files from `tracking/cursors/`. Leave old consolidated ledgers and
   `superseded-ledgers/` in place for `tracking-restructure`.
8. **Docs**: rewrite `doc/orchestration.md` §Transformation Backfills (+ the
   cursor/marker table); collapse `replay-window.md` to the delete-partitions
   procedure (staged vs versioned deletion paths, Coldline cost note); simplify
   `resume-transform-backfill.md` (re-trigger only — idempotent by construction);
   touch up `re-base-corpus.md`; top-level README (external implications: marker
   files gone from `tracking/cursors/`, transform job now needs read access to
   the instance repo).

## 5. Implementation steps — Phase 2 (daily transform)

1. Extend the planner: ISO-week bucketing of single-date raw; "covering
   partition" prepared-check for legacy content-named weeklies; nominal-week
   naming in the PV producer path.
2. Single `run_transform` job type computing the full diff (scoping arg retained
   so PV-sites and weather-grid triggers stay independent); in-process
   `maintain_validation_window` + flushes.
3. Terraform: delete the `pv-prospect-transform` Workflow; scheduler → job direct.
4. Delete `plan_transform` / manifest / per-task dispatch / `TransformInput` /
   `build_transform_phases` and associated tests; simplify the versioner
   readiness batch check; delete the `manifest-env-footprint` brief.
5. Docs: `doc/orchestration.md` daily-transform section, README data-flow notes.

## 6. Verification

- Unit + integration per §4.6.
- **Shadow parity run** before cut-over: `PREPARED_PREFIX_OVERRIDE` (+
  `LEDGER_PREFIX_OVERRIDE`) with the real corpus + real raw. The shadow's staged
  prefix is empty, so its diff ≈ exactly what prod's marker position implies is
  outstanding — compare the planned slice set against the marker/ledger view;
  discrepancies are either the known silent-gap class (expected finds — the
  point of the change) or planner bugs (investigate before landing).
- **Idempotency acceptance**: trigger twice in GCP; the second run plans zero
  slices (or only staleness-driven ones).
- **Replay acceptance**: delete one staged partition, re-trigger, confirm exactly
  that slice re-runs and the partition reappears.
- First production runs may legitimately exceed the historical cadence if silent
  gaps surface; the cap bounds each run.

## 7. Risks and open decisions

- **Corpus read access** (D2, decision): the transform container gains a
  dependency on the instance repo (read-only deploy key). Alternative if this
  coupling is unwanted: option C as a *cache* with documented
  regenerate-from-git semantics — weaker, not recommended.
- **First-run surprises**: windows the marker consumed but whose partitions were
  never written surface as work — the feature, but with Coldline retrieval cost;
  the shadow parity run sizes this before cut-over.
- **Versioner races**: the versioner pushes the corpus *before* cleaning staging,
  so the union stays complete; a mid-clean listing can at worst re-plan a slice
  (benign recompute). Check `git_commit_and_tag` tolerates a re-versioned
  identical partition (no-diff commit edge).
- **Parse ambiguity outside UK coordinates** (8-digit location tokens): assert in
  the parser; fall back to subfolder-derived identity if ever needed.
- **Phase 2 naming transition** (nominal-week vs content-named): flagged in D6;
  the covering-partition check makes both readable, so the corpus needs no
  migration.

## 8. Sequencing and follow-ups

- Safety precondition satisfied: raw is durable in `pv-prospect-raw`
  (`reports/archive-raw-data.md`).
- Land **before** `tracking-restructure` and `end-date-semantics` (they migrate
  files this task deletes). `ledger-scan-cost` has landed
  (`reports/ledger-scan-cost.md`): keep the `since=marker` bound on
  `_consume_extract_descriptors`' extract-ledger listing, which survives this
  task; only the transform-side `completed_task_hashes()` scans go away.
- On landing, re-scope the dependent briefs: `outage-recovery` → extract-side
  cursor-rewind runbook only; `tracking-restructure` → pure legibility/archival;
  `end-date-semantics` → extraction cursors only; `failure-carry-over` → note the
  transform analogue is gone; Phase 2 deletes `manifest-env-footprint`.
