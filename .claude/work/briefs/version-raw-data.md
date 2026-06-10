# Version Raw Extracted Data

## What

Extend the data versioner (`pv-prospect-data-versioner`) to also snapshot raw
extracted CSVs from `staging/raw/` into a versioned raw store (`VersionedRaw` in
the architecture diagram — `gs://pv-prospect-versioned-raw` or similar), in
addition to the prepared feature corpus it already versions.

In the architecture diagram this corresponds to the currently-unimplemented V1 and
V2 interactions:

- **V1**: DataVersioner reads raw CSVs from `StagingRaw`.
- **V2**: DataVersioner pushes `.dvc` pointers to `VersionedRaw` and commits/tags
  alongside the feature-store tag (`data-v<date>`).

## Why

Raw data versioning enables full pipeline reproducibility: with both raw and
prepared corpora tagged, any historical data state can be reconstructed end-to-end.
It also guards against accidental deletion or corruption of the staging bucket — a
source of raw data that cannot be re-fetched (PVOutput API has historical limits).

## What is needed

1. Decide on the bucket name and DVC remote name for the raw store.
2. Add a raw-versioning step to the versioner job (parallels the existing prepared
   step, ideally sharing the DVC/GCS helper logic already in
   `pv-prospect-versioning`).
3. Update the weekly Cloud Workflow if the versioner step's IAM or output changes.
4. Update the arch diagram label V1/V2 once implemented.

## Blockers

None known. The raw staging bucket already exists; only the versioned-raw GCS
bucket and DVC remote need provisioning (Terraform).
