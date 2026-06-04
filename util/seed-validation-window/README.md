# seed-validation-window

One-time seed for the validation window artifact. Reads partition CSV files
from a locally-pulled prepared PV corpus and writes a rolling N-day window to
a local directory or GCS URI.

Run once at launch before enabling the daily `maintain_validation_window`
transformer job. See the top-level README's _Seeding the Validation Window_
section for the full procedure (including the targeted DVC pull).

## Setup

```bash
poetry install
```

## Usage

```bash
poetry run seed-validation-window \
    --prepared-dir /path/to/instance-repo/data/prepared \
    --window-dest gs://pv-prospect-staging/data/served/validation-window \
    [--days 90]
```
