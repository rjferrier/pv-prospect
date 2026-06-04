# Plan — App in-memory validation store (startup load + freshness reload)

Task 2 of the Validation API roadmap. Brief:
[validation-inmemory-store.md](../briefs/validation-inmemory-store.md). Consumes the
producer artifact (`window.csv` + `manifest.json` at
`gs://pv-prospect-staging/data/served/validation-window/`); feeds the `/validate`
endpoint in [validation-api.md](../briefs/validation-api.md).

## Goal

`pv-prospect-app` loads the validation-window artifact into app-local memory at
startup, keeps it fresh per-request, and builds the PV-site repo at startup — so
task 3 can run the PV model over real window rows. No new endpoints in this task.

## Design decisions

Three diverge from the brief's literal wording or affect the working prediction
path; each is surfaced to the user before implementation.

1. **Freshness signal: manifest `updated_at`, not GCS `generation`.** The brief
   says compare GCS object `generation` *and* go "via the pv-prospect-etl storage
   the app already uses." Those conflict — the ETL `FileSystem` protocol
   (`storage/base.py`) exposes no generation/metadata accessor. `updated_at`
   (set fresh by the producer every run) is functionally equivalent for
   change-detection, works in local dev, and stays on the mandated abstraction.
   Generation is reachable only via the raw `google.cloud.storage` client, which
   would lose local-dev parity and re-couple to the client we're moving off.
   → **Recommend `updated_at`.**

2. **Startup load is non-fatal (degraded), model load stays fatal.** Without
   models there is nothing to serve, so `load_store` crashing the app is correct.
   The window + site-repo loads are different: if they fail, `/predict` still
   works. Making them fatal would risk the working prediction path for a feature
   that isn't even wired up yet. → **Recommend: log + continue; the (task-3)
   `/validate` route 503s until a load succeeds, and the freshness check retries
   naturally.** `/healthz`-green-only-when-both-loaded is specced in task 4 and
   deferred there.

3. **`load_store` refactor breadth — minimal, behavior-preserving.** The genuine
   duplication is the `gs://`-vs-local resolution inline in `load_store`. Extract
   it to a shared `storage_config_for(location)` used by the model store, the
   window, and the resources read. Do **not** push the local model path through a
   temp dir to "unify" branches — that changes the deployed prediction path for no
   benefit. Deeper re-plumbing of the GCS model download through `FileSystem.
   read_bytes` is possible but optional. → **Recommend: minimal de-dup only;
   leave the model download mechanics untouched.**

Settled (no user input needed): config uses `STORE_DIR`-style location strings
(`VALIDATION_WINDOW_DIR`, `RESOURCES_DIR`) — brief-endorsed and consistent within
the app, which deliberately chose strings over the monorepo's `AnyStorageConfig`
YAML blocks. The window store lives in `store.py` (per brief), **not**
`pv-prospect-common` (those are domain singletons; this is serving state).

## Changes

### `pyproject.toml`
- Add `pv-prospect-etl` (local path dep) and `pandas` (used directly by the window
  store; currently only transitive). Both in `[project].dependencies` (bare names)
  and `[tool.poetry.dependencies]` (`pv-prospect-etl = {path = "../pv-prospect-etl",
  develop = true}`, `pandas = ">=2.0,<3"`).
- etl's only local dep is `pv-prospect-common`, which the app already declares — so
  this is **not** the seed-script transitive-dep trap. After `poetry lock`, watch
  for a missing `google-cloud-storage-control` (etl pulls it).

### `pv_prospect/app/config.py`
Add two fields mirroring `store_dir`:
```python
@dataclass
class AppConfig:
    store_dir: str
    validation_window_dir: str
    resources_dir: str

    @classmethod
    def from_dict(cls, data):
        store_dir = os.environ.get('STORE_DIR') or data['store_dir']
        validation_window_dir = (
            os.environ.get('VALIDATION_WINDOW_DIR') or data['validation_window_dir']
        )
        resources_dir = os.environ.get('RESOURCES_DIR') or data['resources_dir']
        return cls(store_dir, validation_window_dir, resources_dir)
```

### `pv_prospect/app/resources/config-default.yaml`
Add local-dev defaults (prod overrides via env in task-4 Terraform):
```yaml
validation_window_dir: '/tmp/pv-prospect-validation-window'
resources_dir: 'resources'
```

### `pv_prospect/app/store.py` (new members beside the model store)
Imports from the ETL package root only:
`from pv_prospect.etl.storage import FileSystem, get_filesystem, parse_storage_config`.

- **`storage_config_for(location: str) -> AnyStorageConfig`** (pure, testable
  without a GCS client):
  ```python
  def storage_config_for(location):
      if location.startswith('gs://'):
          bucket, _, prefix = location.removeprefix('gs://').partition('/')
          return parse_storage_config(
              {'backend': 'gcs', 'bucket_name': bucket, 'prefix': prefix})
      return parse_storage_config({'backend': 'local', 'prefix': location})
  ```
- **`filesystem_for(location: str) -> FileSystem`** = `get_filesystem(
  storage_config_for(location))`.
- Re-route `load_store` to obtain its backend via `storage_config_for(store_dir)`
  instead of the inline `gs://` check (minimal de-dup; download mechanics
  unchanged).
- **Artifact-name constants** `WINDOW_CSV = 'window.csv'`,
  `WINDOW_MANIFEST = 'manifest.json'` — duplicated from the producer
  deliberately; the app must not depend on `pv-prospect-data-transformation`. This
  is a small, stable serving contract.
- **`ValidationWindowStore`** dataclass: `windows: dict[int, pd.DataFrame]`,
  `manifest: dict`; `updated_at` property (`manifest['updated_at']`), `for_site(
  system_id) -> pd.DataFrame | None`, `system_ids` property.
- **`parse_window(csv_text: str) -> dict[int, pd.DataFrame]`** (pure): `pd.read_csv(
  StringIO(text), parse_dates=['time'])` then group by `system_id` into a dict of
  per-site frames.
- **`load_validation_window(fs: FileSystem) -> ValidationWindowStore`**: `manifest =
  json.loads(fs.read_text(WINDOW_MANIFEST))`; `windows = parse_window(fs.read_text(
  WINDOW_CSV))`.
- **`ValidationWindowCache`** (freshness coordinator; holds `fs`, a
  `threading.Lock`, current store):
  - `load()` — startup/forced load; swap under lock.
  - `current() -> ValidationWindowStore` — read manifest `updated_at` (cheap,
    **no lock**); if unchanged vs loaded, return current; else reload and swap
    under lock. Lock is held only around the reference read + swap, never around
    I/O or parsing, so unchanged requests never serialize on a round-trip. A
    manifest-read error returns the last good store (logged) rather than crashing.

### `pv_prospect/app/main.py` (lifespan only; no endpoints yet)
- Globals `_window_cache: ValidationWindowCache | None`.
- In `lifespan`, after the (fatal) model load, **non-fatally**:
  - `_window_cache = ValidationWindowCache(filesystem_for(config.validation_window_dir))`
    then `_window_cache.load()` inside try/except (log on failure; cache retries
    via `current()`).
  - `build_pv_site_repo(StringIO(filesystem_for(config.resources_dir).read_text(
    'pv_sites.csv')))` inside try/except.
- Teardown sets `_window_cache = None`.

## Tests (`tests/unit/store/`, one module per member)
- `test_storage_config_for.py` — gs:// (with/without nested prefix) → gcs config
  with right bucket/prefix; local → local config with `prefix=path`. Pure.
- `test_parse_window.py` — csv text → dict keyed by int system_id; `time` parsed
  as datetime; all `WINDOW_COLUMNS` preserved; empty/multi-site cases.
- `test_validation_window_cache.py` — drive a real `LocalFileSystem` on `tmp_path`
  (no patches, per purity rules): write artifact → `load()` → `current()` returns
  it; bump `updated_at` + rewrite csv → `current()` reloads; unchanged → same
  object returned.
- Freshness staleness is a trivial string compare — covered via the cache test,
  not extracted (purity rules).
- **Caveat for any future startup/integration test:** `build_pv_site_repo` has an
  `if len(...) != 0: return` singleton guard over module-level
  `pv_sites_by_system_id`; reset it between tests to avoid cross-test leakage. No
  lifespan test in this task (orchestration → covered by the task-4 smoke test).

## Verification
- `poetry lock && poetry install`; `poetry run pytest tests/`; `ruff check .`;
  `ruff format .`; `mypy .` (per CLAUDE.md flags).

## Out of scope (later tasks)
- `/validate/sites` + `/validate/{system_id}` routes, PV-arm extraction, metrics
  (task 3).
- `/healthz` gating on the window, `/version` reporting `updated_at`, Terraform
  app-SA read grant + env wiring, diagram, smoke test, doc finalisation (task 4).

## Finalisation
Docs/diagram/brief-deletion happen in task 4, which finalises all four validation
briefs together. This task commits code + tests only.
