# Rate-limit the public app endpoints (then go public by default)

> Prerequisite for the public deploy (TODO "Later"). The decision is
> **public-by-default**, justified by per-IP rate limiting — so the
> `allow_unauthenticated` default flip and the README "public by default" wording
> ship **with** this task, putting the protection and the exposure in one image.
> Until this lands, the service stays private by default.

## Why

Going public exposes `/predict` and `/validate/*` without auth. The sharp risk
(see the **App Serving** / **Going Public** notes in the top-level README) is
Open-Meteo quota burn: every `/predict` for a *novel* coordinate makes one
Open-Meteo forecast call (`pv_prospect/app/elevation.py` — the only external call
on the path; its `lru_cache` absorbs repeats but not adversarial varied coords).
A flood can get the project throttled or blocked by Open-Meteo — collateral
damage to the daily **extraction pipeline**, which uses the same free tier.
Secondary risks: self-DoS of the 2-instance service (CPU-heavy model inference,
Cloud Run default concurrency 80/instance), and free-oracle / site-enumeration
abuse. `max_instances=2` already caps cost; rate limiting is the missing lever
for the spamming/hammering vector.

## Scope

Per-IP rate limiting via `slowapi` (the Starlette/FastAPI limiter), baked into the
app image:

- Limit `POST /predict` and the `/validate/*` routes (per-IP; e.g. 10–30 req/min,
  config-driven so the limits can be tuned without a code change).
- **Exempt `/healthz` and `/version`** — uptime / monitoring probes must not 429.
- Per-instance in-memory limiter state is acceptable at `max_instances=2`
  (effective ceiling ~2×). No Redis/Memorystore. Cloud Armor (edge per-IP throttle
  behind an external HTTPS load balancer, with the app `ingress` locked to
  internal-and-load-balancing so it can't be bypassed via the `run.app` URL) is the
  robust path if this graduates beyond a demo — **out of scope** here.
- **Closing steps (ship with the limiter, same image/deploy):** flip
  `allow_unauthenticated` default `false → true` in `terraform/variables.tf` (and
  its description), and flip the README "private by default" wording in the three
  spots — the **App Serving** auth note, the operational `pv-prospect-app` bullet,
  and **Deploying the App & Going Public** — to public-by-default + rate-limited.
  This couples the exposure to its protection.

## Design notes

- **Client IP behind Cloud Run is the silent-failure point.** slowapi's default
  `get_remote_address` reads `request.client.host`, which on Cloud Run is the
  Google front-end — without a custom `key_func` every request shares one IP and
  the limiter throttles *globally*. Use the `X-Forwarded-For` client entry, but
  verify Cloud Run's XFF semantics first: left-most entries are client-spoofable,
  so don't hardcode an index on a guess. Test that (a) two distinct clients get
  separate buckets and (b) a client-supplied `X-Forwarded-For` can't reset its own
  count.
- **429 in the website JS.** The demo's Prediction / Validation tabs call these
  endpoints; a 429 should surface a "you're going too fast, try again shortly"
  message, not a broken UI.
- **Tests.** The existing acceptance smokes (`test_predict_site_smoke`,
  `test_validate_site_smoke`) hit the limited routes — set high/disabled limits in
  the test config so they don't trip. Add a test asserting the limiter fires (N+1
  requests → 429) and one asserting per-IP keying (distinct IPs are independent; a
  spoofed `X-Forwarded-For` can't bypass).
