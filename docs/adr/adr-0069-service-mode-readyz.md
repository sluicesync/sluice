# ADR-0069: /readyz on `sluice sync start` — readiness = "streaming phase entered"

## Status

**Accepted (2026-05-29); implemented in PR #110 (queue item 18(c)).**

## Context

`sluice sync start --metrics-listen ADDR` already exposes `/metrics`
(Prometheus exposition format) and `/healthz` (a stub returning 200 "ok").
Operators running sluice under k8s, systemd, or Heroku want a third probe:
**/readyz**, so the orchestrator can delay routing traffic — or marking
the unit as `Started` — until the streamer is actually keeping the target
in sync rather than still running its cold-start preamble.

There is no analogous endpoint today. Heroku-managed-PG validation
(2026-05-28) surfaced this as a concrete gap: the dyno reports the
process as "up" the instant `sync start` opens the listener, but the
stream may still be 15–60 minutes from its first apply (bulk-copy is in
progress). An external monitor watching `/healthz` saw "up" and silently
assumed sluice was mirroring; it wasn't.

## Decision

Add `/readyz` to the existing metrics HTTP server.

**Readiness semantics: "streaming phase entered."** `/readyz` returns 200
"ready" once the streamer has completed cold-start (snapshot + bulk-copy
+ schema apply) or warm-resume, primed the schema-history cache, and is
about to begin the apply loop. Before that point it returns **503 "not
ready"**.

**The signal is monotonic.** There is no un-ready path. A streamer that
loses its slot, hits an unrecoverable error, or exits for any reason
brings down the process; the orchestrator restarts sluice and the new
process starts at 503 again. This keeps the handler lock-free
(`atomic.Bool`) and lets operators reason about the signal without
worrying about flapping.

**`/readyz` does NOT check lag.** Two alternatives were considered:

1. **Streaming + configurable lag bound** (`--readyz-max-lag=30s`) —
   stricter; k8s would pull a lagging pod from rotation. **Rejected**
   because sluice doesn't *accept* traffic — it mirrors. Pulling a
   replica because it's lagging doesn't make sense for a one-way CDC
   sink; the operator's response is to alert + fix the source/network,
   not to "fail over." Lag alerting belongs on
   `sluice_seconds_since_last_apply` (already exposed via `/metrics`),
   not `/readyz`.
2. **Streaming + slot/replication-health DB roundtrip** — catches
   "slot dropped under us," but adds a source-DB query on every scrape
   (typical Prometheus cadence: 15s). **Rejected** for cost vs benefit:
   the slot-health probe ADR-0059 / finding F13 already runs in the
   streamer's own loop and surfaces structured WARNs; piling another
   per-scrape roundtrip onto the source isn't worth the
   not-quite-real-time refresh.

Both can be added later behind opt-in flags without breaking the simple
default.

**Liveness vs readiness split.** `/healthz` stays as it is — 200 "ok"
unconditionally. k8s, Heroku, and systemd all distinguish liveness
(`process responsive → don't restart`) from readiness (`stream
mirroring → safe to depend on`); conflating the two would let a
cold-starting streamer get killed mid-bulk by a too-eager liveness probe.

## Consequences

**Operator surface:** [docs/operator/running-as-a-service.md] documents
the three endpoints, the recommended probe wiring for systemd / k8s /
docker, and the lag-alerting story (Prometheus, not `/readyz`).

**Code surface:** one `atomic.Bool` on `MetricsServer`, one
`MarkReady()` call after the streamer's section 4b prime-cache step,
one `/readyz` handler. Pinned by unit tests (initial 503, post-MarkReady
200, idempotent MarkReady, /healthz invariant under MarkReady).

**Backwards compatibility:** additive. Operators not using
`--metrics-listen` see no change; operators already scraping `/metrics`
+ `/healthz` see a new endpoint and nothing else.
