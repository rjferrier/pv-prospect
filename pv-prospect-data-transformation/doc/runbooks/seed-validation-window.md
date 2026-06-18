# Seed the Validation Window

The `maintain_validation_window` workflow step is fail-closed: it raises if the
artifact does not already exist. Before enabling the daily workflow step, seed the
window once from the locally-pulled DVC prepared corpus:

```bash
# In pv-prospect-instance — pull only the recent PV partitions.
# Adjust the date threshold to suit; 90 days back from today is sufficient.
dvc pull data/prepared/pv/

# In pv-prospect (submodule)
cd util/seed-validation-window
poetry install
poetry run seed-validation-window \
  --prepared-dir ../../../data/prepared \
  --window-dest gs://pv-prospect-staging/data/served/validation-window \
  --days 90
```

`seed-validation-window` writes `window.csv` + `manifest.json` to the specified
destination. Use a local path for `--window-dest` to dry-run locally before
writing to GCS.
