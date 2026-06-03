# Align OpenMeteo vintage between prepared-weather and prepared-PV corpora

## Problem

The weather model and PV model draw on data from different OpenMeteo historical
reanalysis snapshots, introducing a systematic bias in the end-to-end prediction
chain.

**How the mismatch arises.** OpenMeteo continuously updates its historical
reanalysis as new observations are assimilated. The weather and PV extraction
jobs run at different times (weather runs as a daily rolling window; PV
extraction is site-specific and triggered on a different schedule). For any
given date in the prepared corpus, the hourly irradiance values used to compute
the **prepared-PV** `plane_of_array_irradiance` may differ from the values in
the **prepared-weather** partitions for the same date and grid cell, because the
OpenMeteo API returned different numbers on two different days.

**Observed effect.** End-to-end check at site 89665 (Feb–May 2025/2026):

| Month    | Weather-model DNI (W/m²) | Corpus PV POA (W/m²) | Reconstructed POA (W/m²) | Error |
|----------|-------------------------:|---------------------:|-------------------------:|------:|
| 2025-02  | 86 | 83 | 56 | –32% |
| 2025-10  | 90 | 85 | 68 | –20% |
| 2026-05  | 125 | 249 | 155 | –38% |

MAPE = 32.7 %. This is above the ~15 % threshold for the smoke test to be
meaningful; it propagates through to a systematic underestimate of annual yield
in the Prediction API.

The gap has two components:
1. **Weather-model error** — the model predicts DNI ±30–94 % vs the corpus
   values it was trained on (see validation in the productionise-models plan).
2. **Cross-vintage POA drift** — the PV corpus POA was computed from a
   higher-vintage (more recent) OpenMeteo snapshot than the weather corpus,
   so even correcting for weather-model error would leave a residual mismatch.

## Fix

**Primary fix — same-vintage extraction.** Ensure that for every `data-v<date>`
snapshot, the weather partitions and the cleaned weather used by `prepare_pv`
are drawn from the *same* OpenMeteo API call, not two separate calls on
different days. Practically:

* The data-versioner already runs weather and PV extraction in the same weekly
  batch. Confirm (or enforce) that `prepare_pv` for each site reads the cleaned
  weather produced *in the same run*, not a cached version from an older run.
* If the two jobs cannot be made truly atomic, add a **vintage-stamp** to each
  extraction output (the OpenMeteo `generation_time` field is a proxy) and
  reject any `prepare_pv` input whose vintage differs from the current batch by
  more than one week.

**Secondary fix — trainer validation gate.** After training and before
promotion, compute the weather model's DNI/DHI predictions for each held-out
grid cell and compare to the corpus values for the same (location, month)
pairs. Log MAPE as a metric; alert if it exceeds a configured threshold (e.g.,
20 %). This doesn't fix the root cause but makes the bias visible and stops
silently-degraded models from reaching production.

**Workaround (documented bias).** Until the above fixes land, the Prediction API
documents the ~30 % systematic underestimate of annual yield as a known caveat
in every `/predict` response.  This is implemented as of Phase 3 (see
`productionise-models.md` §2.3).
