# pv-prospect-physics

Shared solar-irradiance physics for PV Prospect. Currently provides the
plane-of-array (POA) irradiance calculation used by both the transformation
pipeline and the prediction API.

## Why this is a package

POA must be computed **identically** wherever it appears: the value the PV model
trains on (computed in `prepare_pv`) and the value fed to that model at inference
(computed in the prediction API) have to be on the same scale, or predictions
drift. Keeping a single implementation here — rather than duplicating it per
consumer — is what guarantees that parity.

It is a standalone package (rather than living in `pv-prospect-common`) so that
the `pvlib` dependency stays out of the packages that don't need it
(`pv-prospect-data-extraction`, `-data-sources`, `-data-versioner`, `-etl`),
which all install `pv-prospect-common`.

## API

```python
from pv_prospect.physics import compute_poa_irradiance, ALTITUDE
```

`compute_poa_irradiance(times, dni, dhi, location, panel_geometry, altitude=ALTITUDE)`
returns the `poa_global` series (W/m2) for a **single** panel geometry, using
pvlib's `get_total_irradiance` with the isotropic sky-diffuse model. Callers with
multiple panel geometries (e.g. `prepare_pv`) invoke it per geometry and weight
the results by `area_fraction`.

### Conventions the caller owns

- **Time labelling.** `times` must already carry any cadence-labelling
  correction the caller needs (the transformation pipeline subtracts 30 minutes
  because its weather data is right-labelled hourly). tz-naive timestamps are
  interpreted as UTC.
- **Altitude.** POA is computed at sea level (`ALTITUDE = 0`). Site elevation is
  an input *feature* of the weather model, not a POA parameter; do not pass a
  per-site elevation here unless training did the same.

## Development

```bash
poetry install
poetry run pytest tests/
poetry run ruff check .
poetry run mypy . --ignore-missing-imports --disallow-untyped-defs \
    --explicit-package-bases --namespace-packages
```
