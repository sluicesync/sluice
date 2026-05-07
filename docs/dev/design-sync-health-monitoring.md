# Design: sync-health monitoring + alerting hooks

**Status:** proto-ADR (research / design draft, not yet implementation-bound)
**Author:** main session
**Date:** 2026-05-07
**Related:** `docs/dev/design-sluice-verify.md` (companion — verify covers data-integrity, this covers liveness/lag), ADR-0017/0020/0025/0027 (batched apply, slot-ack, sync stop, source-tx-boundary batching)

## Why

The sluice verify proto-ADR closes the data-integrity side of the user's "100% confidence" goal: operators can ask "is my data correct?" Sync-health monitoring closes the orthogonal liveness side: operators need to know **when their sync stops or falls behind** without manually polling.

The pain shape: PlanetScale-Fivetran customers regularly find out days late that their syncs have stalled. The blast radius is application-visible — analytics dashboards staling, downstream feature stores serving outdated data, replication-based DR no longer protecting recent changes. The fix isn't a manual recovery procedure; it's a louder alarm.

`sluice sync status` (today) gives operators a snapshot when they think to look. What's missing:

- **Continuous staleness measurement**: how far behind is the target, in time? In events?
- **Stalled-stream detection**: a stream that's running but not making forward progress for N seconds is broken even if no error has been logged.
- **Push-based alerting integration**: structured signals an operator's existing alerting infra (Prometheus / statsd / OpenTelemetry / PagerDuty webhook) can consume without parsing logs.
- **Self-test hooks**: a stream's own watchdog that escalates when it's quiescent in a way that shouldn't be possible (no events from a write-active source).

## Decision

Add two new operator surfaces, each with a clear lane:

1. **`sluice sync health` (one-shot probe)** — read the target's `sluice_cdc_state` and source's current position; emit a structured report with lag and liveness metrics. Cron-friendly. Exits 0 on healthy, 1 on unhealthy, 2 on operational error. Operators script this for cheap external monitoring without sluice running its own daemon.

2. **`--metrics-listen ADDR` flag on `sluice sync start`** — when set, the running streamer exposes a Prometheus-format `/metrics` endpoint at the given address. Same data the `sync health` probe reports, surfaced for scrape-based monitoring without polling overhead. Off by default; opt-in.

Both surface the same underlying metrics, just packaged differently. The metric set is the load-bearing artifact.

## Metric set

### Position-derived (source vs. target)

- **`sluice_lag_events`**: count of source events not yet applied to target. Gauge.
- **`sluice_lag_seconds`**: wall-clock seconds between source's most recent committed event and the latest event applied to target. Gauge. Most operator-meaningful for "how out-of-date is my downstream system?"
- **`sluice_target_position`**: the source position the target has applied through, as an opaque string (LSN/GTID). Useful for incident-response correlation; not a metric to alert on directly.
- **`sluice_source_position`**: the source's current position. Same shape.

The PG and MySQL CDC readers already track positions; this just exposes the difference.

### Liveness

- **`sluice_seconds_since_last_event`**: wall-clock seconds since the streamer last received any event from the source. A sustained value much larger than the heartbeat period means the source connection is silently broken.
- **`sluice_seconds_since_last_apply`**: wall-clock seconds since the streamer last committed a target write. A high value with low `sluice_seconds_since_last_event` means the apply path is wedged.
- **`sluice_streamer_state`**: enum gauge with values for `bulk_copying`/`streaming`/`stopping`/`stopped`. Operators alert on transitions out of `streaming`.
- **`sluice_apply_errors_total`**: counter of apply errors since stream start. Sustained increases mean something's repeatedly failing.

### Throughput (operational visibility, not alerting)

- **`sluice_events_received_total`**: counter of CDC events received from source.
- **`sluice_events_applied_total`**: counter of CDC events successfully applied to target.
- **`sluice_bytes_applied_total`**: counter of bytes written. Same scale as the `--max-buffer-bytes` budget from ADR-0028.
- **`sluice_apply_batch_size`** (histogram): distribution of how big each apply batch is. Operators tune `--apply-batch-size` based on this.

Standard Prometheus naming conventions; histograms over counters/gauges per the well-trodden distinctions.

### Per-table (optional, off by default — high cardinality)

- **`sluice_table_rows_applied_total{table=...}`**: per-table apply count. Gated by `--metrics-per-table` flag because tables × instances cardinality blows up Prometheus storage on schemas with thousands of tables.

## `sluice sync health` shape

Same DSN and engine flags as `sync status`. Optional `--max-lag-seconds N` and `--max-events N` thresholds; if either is exceeded, exit 1.

```
$ sluice sync health \
    --target-driver postgres --target 'postgres://...' \
    --max-lag-seconds 60 \
    --max-events 10000

stream: myapp-prod
state: streaming
lag_events: 142
lag_seconds: 2.3
target_position: LSN 0/1A2B3C4D
source_position: LSN 0/1A2B3F12
seconds_since_last_event: 0.4
seconds_since_last_apply: 0.5
apply_errors_total: 0
events_applied_total: 4_872_193

result: healthy
```

JSON form (`--format json`) for machine consumption mirrors the structure 1:1.

## Implementation outline

### New IR surfaces

Liveness and position observation are already partly available on existing engine interfaces. We need:

```go
// In internal/ir/health.go (new file).

// HealthReporter is the optional engine surface for liveness/position
// reporting. Engines that already track CDC positions (MySQL binlog/
// VStream, PG pgoutput) implement straightforwardly; the health probe
// uses optional-interface assertion and reports "not supported" for
// engines without it.
type HealthReporter interface {
    // CurrentPosition returns the source's current head position (most
    // recent committed event).
    CurrentPosition(ctx context.Context, db DBHandle) (Position, error)

    // PositionTime returns the wall-clock timestamp of an event at the
    // given position. Used to compute lag_seconds. Returns ok=false
    // when the engine doesn't carry per-event timestamps for the
    // requested position (rare, but PG's WAL doesn't always carry one
    // for the very latest record).
    PositionTime(ctx context.Context, db DBHandle, p Position) (time.Time, bool, error)
}

// Position is opaque — already exists in IR via cdcPosition; this is
// the reader-side observation surface for it.
```

Same shape as the existing optional surfaces (`RowCounter`, `SnapshotImporter`, `Verifier` from the verify ADR).

### `sluice sync health` orchestrator

`internal/pipeline/health.go` — small orchestrator (~200 LOC) that:

1. Reads target's `sluice_cdc_state` for the supplied `--stream-id`.
2. Reads source's current position via `HealthReporter`.
3. Diffs to compute `lag_events` (best-effort; LSN/GTID arithmetic is engine-specific) and `lag_seconds` (via `PositionTime` on both ends).
4. Renders text or JSON report.
5. Compares against thresholds; sets exit code.

### `--metrics-listen` integration

Streamer (`internal/pipeline/streamer.go`) gains an optional metrics goroutine:

```go
if s.MetricsListen != "" {
    go s.runMetricsServer(ctx, s.MetricsListen)
}
```

Standard `net/http.ServeMux` + `prometheus/client_golang` for the /metrics handler. The metrics struct lives next to the streamer and updates on every CDC event apply. Off by default; opt-in via the new flag.

### CLI

```
$ sluice sync health --help
Probe a running or stopped sync stream's health.

Usage: sluice sync health [flags]

Flags:
  --stream-id              Stream to probe (default: from config or sluice_cdc_state's only row)
  --max-lag-seconds        Threshold; exit 1 if lag_seconds exceeds this (default: 0 = disabled)
  --max-events             Threshold; exit 1 if lag_events exceeds this (default: 0 = disabled)
  --max-stale-seconds      Threshold; exit 1 if seconds_since_last_event exceeds this (default: 0 = disabled)
  --format                 text|json (default: text)
  --output                 file path (default: stdout)
  ... + standard --target/--config/--log-level
```

`sluice sync start --metrics-listen :9090` exposes the same data on `/metrics` for as long as the streamer runs.

## Tenet check

- **Clean elegant code.** Two surfaces (probe + Prometheus endpoint), one shared metric set. Reuses existing position-tracking machinery; doesn't add a parallel measurement system. Optional-interface pattern keeps engines clean.
- **IR-first.** Position + timestamp observation lifts to the IR; engines provide the source-specific arithmetic, the orchestrator computes the lag.
- **Contain Postgres complexity.** PG WAL timestamp extraction is non-trivial (replication-slot stats are pieces; `pg_last_wal_replay_lsn` is a piece). The implementation contains this in the PG `HealthReporter`; operators don't see the complexity.
- **Validate end-to-end.** New CI integration tests that run a stream, intentionally pause the apply path, and verify the metrics surface the staleness correctly.
- **Loud failure beats silent corruption.** This whole feature exists to make silent staleness loud. The threshold flags + exit-code shape mean operators wire this into their existing alerting without parsing log lines.

## Consequences

- **Operators get an alerting surface.** Cron'd `sluice sync health` with thresholds catches stalled streams within minutes instead of days. Prometheus scraping catches everything in real time.
- **Two new optional engine surfaces** (`HealthReporter`); MySQL + PG implement; PlanetScale-MySQL inherits via embedded MySQL engine. Surface area grows by one method-set per engine.
- **prometheus/client_golang dependency.** A real new dep. Reasonable footprint (~1 MB binary growth), widely audited, low API risk. Build-tag layering can hide it behind `--metrics-listen` use, but in practice everyone using sync should have observability — likely best to take it unconditionally.
- **Doesn't replace verify.** Health = liveness + lag; verify = data integrity. Both should live in the operator toolbox; they're complementary.
- **Doesn't replace the operator's alerting infra.** Sluice provides the signal; the operator wires it to PagerDuty/Slack/etc. Same posture as `verify` (probe + exit code; delivery is operator's responsibility).
- **Off-by-default Prometheus listener.** Listener is opt-in so no surprise port allocations; binary size growth from the dep is the only "always on" cost.

## Open questions

1. **Lag-events arithmetic across engines.** PG WAL LSNs and MySQL GTIDs are both linear-ish but non-trivial to subtract. Engines need to expose a `PositionsDifference(a, b) (events int64, ok bool)` method, or the orchestrator estimates from byte-distance + average-event-size. Cleanly engineerable; just needs spec.

2. **Per-stream vs. per-target metrics.** A target with multiple streams (currently rare; would become common with multi-source aggregation per the parallel design doc) needs metrics scoped by `stream_id`. The Prometheus labelling shape: `sluice_lag_seconds{stream_id="myapp-prod"}` vs. `sluice_lag_seconds_myapp_prod`. Standard pattern is the former; we go with that.

3. **Source-write-rate awareness.** A stream that's "0 events behind" because the source isn't writing isn't necessarily healthy — it could be that the source connection died and we're reading no events for a *different* reason. The watchdog from ADR-0026 (MySQL CDC heartbeat) is one input; PG needs an analogous mechanism (PG's logical-decoding heartbeat is `wal_sender_timeout` server-side; sluice can probe `pg_stat_replication` for the slot's last-flush time). Distinguishing "quiet source" from "broken connection" is the load-bearing design question.

4. **OpenTelemetry vs. Prometheus.** Prometheus is the most-deployed observability tool in our user demographic; OTel is the future-leaning standard. Could ship Prometheus first, add an OTel exporter later via the ecosystem's adapters. Don't try to ship both natively in the MVP.

5. **Health check during `migrate` (not just sync).** Migrate has progress logging today (per-2s ticker); does it need a `migrate health` probe equivalent? Probably yes, but with different metrics (rows-copied / rows-remaining / ETA, not lag). Out of scope for the v1 of this feature; mention as a follow-up.

6. **Webhook delivery option.** Should sluice itself fire a webhook on threshold breach? Tempting but complicates the surface (auth, retry, payload format). Operator-facing answer: no; pipe `sluice sync health` exit code into the operator's alertmanager / blackbox-exporter / cronjob runner. Sluice produces signal, not delivery.

## What this is not

- **Not a service-mesh / observability platform.** Sluice exposes its own state; it doesn't aggregate other systems' metrics or do dashboarding.
- **Not a control plane.** No "self-healing" or "auto-restart on staleness." Recovery is the operator's call (or k8s liveness-probe at the process level). Sluice surfaces signal; operator decides response.
- **Not an SLO tool.** Computing 30-day uptime is the operator's monitoring system's job; sluice provides the raw signal.

## Sequencing

If this lands, suggested staged delivery:

1. **MVP: `sluice sync health` probe.** New `HealthReporter` interface + both engines implement + new orchestrator + CLI + threshold flags. Closes the cron-friendly health-probe gap. ~2 weeks.
2. **`--metrics-listen` Prometheus endpoint.** prometheus/client_golang dep + metric-update plumbing on the streamer + tests. ~1 week.
3. **Per-table metrics + PG slot health.** Higher-cardinality + the source-write-rate awareness (Open Question #3). ~1 week.
4. **OpenTelemetry exporter** (later). ~3 days.

Total full-feature ~5 weeks. MVP slice (probe + thresholds) is the right place to start since it's cron-friendly without committing to the prometheus dep.

## Recommendation

**Yes, ship the MVP slice.** Same reasoning as `sluice verify`: closes a real operator pain (Fivetran-stops-silently style) using building blocks sluice already has. The probe-with-threshold-flags shape is a clean, scriptable interface that integrates cleanly with whatever monitoring stack the operator already runs.

The Prometheus listener follow-up is the higher-leverage step — once it's in place, every operator running sluice in a Prometheus-using environment gets monitoring for free, which materially raises the floor of operator confidence in production deployments.

Path to no: only if we believe operators will universally set up their own external monitoring (custom-coded probe scripts, vendor agents) and sluice's built-in surfaces are redundant. The historical pattern from comparable tools (Vitess metrics, Postgres exporter, MySQL exporter) is the opposite — built-in metrics are the de-facto standard for production data tooling.
