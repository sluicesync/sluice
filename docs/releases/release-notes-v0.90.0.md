# sluice v0.90.0

# sluice v0.90.0 — `/readyz` for service-mode orchestration + eleven new type-stance integration pins

**Headline:** `sluice sync start --metrics-listen ADDR` now exposes a third HTTP endpoint, **`/readyz`**, alongside the existing `/metrics` + `/healthz`. The signal flips from 503 "not ready" to 200 "ready" once the streamer is past its cold-start / warm-resume preamble and about to begin applying — giving k8s `readinessProbe`, Heroku release-phase scripts, and systemd unit-started gates a real signal to wait on instead of just "the process opened the listener." Drop-in from v0.89.0; no config / schema / IR / lineage-format changes.

## Features

- **`/readyz` on the metrics HTTP server.** Returns 503 "not ready" during cold-start (snapshot + bulk-copy + schema apply + cache prime), then 200 "ready" once the apply loop is about to begin consuming events. Monotonic — a streamer that exits brings down the process; the orchestrator restarts and starts at 503 again. **Deliberately does not check apply lag** (alert on the `sluice_seconds_since_last_apply` gauge from `/metrics` instead); the choice was the operator-confirmed default in the design review. See [ADR-0069](https://github.com/orware/sluice/blob/v0.90.0/docs/adr/adr-0069-service-mode-readyz.md) for full rationale.
- **New operator guide:** [`docs/operator/running-as-a-service.md`](https://github.com/orware/sluice/blob/v0.90.0/docs/operator/running-as-a-service.md) — systemd unit / docker-compose / Kubernetes Deployment (with the `replicas: 1 + strategy: Recreate` caveat called out) / Heroku release-phase wiring, plus the Prometheus alerting story (lag on the metric, not on `/readyz`).

## Test coverage (no behavior change)

- **Eleven new integration pins** document sluice's current behavior on edge classes mined from prior incidents (broader gap review) and the ADR-0051 Stage-2 verbatim-carry type list. Each pin asserts **one of three legitimate outcomes** — refuse-loudly with an operator-grep-able hint / preserve correctly / fail loudly on silent type-loss — and only silent flatten fails the test. Together these freeze the loud-failure surface so a future regression that silently maps any of these to text/varchar gets caught in CI.

  - **Broader-mining gaps (7):** PG special floats / temporals (`infinity` / `-infinity` / `NaN`), TOAST round-trip under REPLICA IDENTITY DEFAULT, ENUM mid-stream `ALTER TYPE ADD VALUE` drift, DOMAIN-typed array, `money`, SAVEPOINT / ROLLBACK TO suppression, TRUNCATE CDC event.
  - **Stage-2 type pins (4):** `xml`, `pg_lsn`, `txid_snapshot`, `pg_snapshot`.

  No production code changed by these pins — the value is the regression net under the eleven cases.

## Compatibility

- **Minor version bump (v0.90.0)** — additive, drop-in from v0.89.0. No config / schema / IR / lineage-format changes.
- **No behavior changes** to migrate / sync / verify / backup / restore. The `--metrics-listen` HTTP server gains a new endpoint; existing scrapers of `/metrics` + `/healthz` see no change.

## Who needs this

- **Operators running `sluice sync start` under k8s, Heroku, systemd, or docker-compose** — `/readyz` is the missing piece you've been gating traffic on the wrong signal for. Heroku-managed-PG validation surfaced this concretely (dyno reports "up" the instant the process binds, but bulk-copy may still be 15-60 min from first apply). The new operator guide walks the wiring for all four orchestrators.
- **Everyone else:** no action needed. `--metrics-listen` stays off by default; if you weren't using it, nothing changes.
