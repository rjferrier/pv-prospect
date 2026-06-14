# Replace Open-Meteo elevation with a cheaper API

## What

Open-Meteo is currently used to query elevation for a given lat/lon (as a side-effect of the
weather API call, or as a dedicated call). A dedicated elevation API would likely be free or
cheaper per-call, and more semantically appropriate for a single static lookup.

## Why

Elevation is a static property of a location — it never changes. Using a full weather API to
obtain it is wasteful. Candidates include:

- **Open-Elevation** (open-source, self-hostable, free public instance) — `https://api.open-elevation.com/api/v1/lookup`
- **Google Elevation API** — paid but high-quality; $5 per 1000 requests (after free tier)
- **USGS Elevation Point Query Service** — free, US-only
- **AWS Terrain Tiles / Mapbox Terrain-RGB** — tile-based, requires decoding but free tier generous

Open-Elevation is the most likely candidate: free, no API key, and straightforward JSON interface.

## Where to look

Find the site where elevation is queried from Open-Meteo in the ETL or data-sources packages.
Check whether it is fetched once at site-registration time or on every run; if the latter,
caching or a one-time lookup would also be worth adding.

## Blockers

None. Low-priority, deferred to Later.