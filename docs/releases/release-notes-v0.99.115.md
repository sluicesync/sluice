# sluice v0.99.115

**A standalone `sluice metrics-watch` daemon — point it at a PlanetScale database and watch its CPU/memory/storage/lag live and fire Slack/webhook threshold alerts, with no sync attached — plus a richer own-`/metrics` surface that makes the daemon a standalone PlanetScale-metrics Prometheus exporter.**

These close the two demand-gated leftovers of the ADR-0107 PlanetScale-metrics work. Both are observability-only: advisory, failure-isolated, and never on the value or apply path.

## Added

**`sluice metrics-watch` — standalone target-metrics watch daemon.** Until now sluice's target-metrics alerter only ran *alongside* an active migration/sync. This adds the sync-independent sibling: `sluice metrics-watch --engine mysql|postgres --planetscale-org ORG --planetscale-metrics-db DB` polls that database's PlanetScale metrics endpoint on an interval, prints a live `cpu= mem= storage= lag= conns=` line, and fires the **same** edge-triggered, cooldown'd, hysteresis-guarded Slack/webhook alerts as `sync start`. The rule set, evaluator, and notify sinks are shared code, so the daemon can never drift from the in-sync alerter. It opens **no connection to the database** — only the PlanetScale control-plane metrics API — so it needs just the `--planetscale-metrics-token-id` / `--planetscale-metrics-token` credentials, not a `--target` DSN. `--once` polls a single sample and exits (scripts); `--quiet` suppresses the live line for headless alert-only operation; the `--notify-storage-util` / `-cpu-util` / `-mem-util` / `-lag-seconds` / `-storage-growth-per-min` / `-cooldown` / `--notify-webhook` / `--notify-slack` flags mirror `sync start` one-for-one. A dead sink is logged and swallowed; an unobserved metric never fires (the `*Known` honesty contract).

**A richer own-`/metrics` Prometheus surface.** Every sluice `/metrics` endpoint now also emits `sluice_build_info{version,commit,go_version}` (the standard exporter build-info series) and a compact Go-runtime block — `sluice_go_goroutines`, `sluice_go_gomaxprocs`, `sluice_go_memstats_heap_alloc_bytes` / `_heap_sys_bytes` / `_heap_objects`, and cumulative `sluice_go_gc_completed_total` / `sluice_go_gc_pause_seconds_total`. This lets operators watch sluice's **own** process health — the load-bearing signal for the bounded-memory guarantees of the auto-shard and fan-out copy paths — in the same Grafana/Datadog stack as the stream metrics. All scrape-time only; no apply-path instrumentation.

**`metrics-watch --metrics-listen ADDR` — a standalone PlanetScale-metrics exporter.** With the watch daemon, this serves a Prometheus `/metrics` endpoint re-exporting the watched database's health as the `sluice_target_*` gauge family alongside `build_info` + the runtime block — turning sluice into a credential-light PlanetScale-metrics exporter that needs only the metrics token, no database connection.

## Compatibility

Additive and opt-in. A new top-level `metrics-watch` subcommand; new always-on `sluice_build_info` + `sluice_go_*` lines on the existing (opt-in) `/metrics` endpoint. No change to migration, sync, backup, or restore behaviour; no value-path or apply-path code touched. The only outbound surfaces are the same advisory, credential-gated, failure-isolated notify sinks introduced in v0.99.108.

## Who needs this

Anyone running into a **PlanetScale** target who wants to watch a database's CPU/memory/storage between migrations, wire storage/CPU alerts into Slack without an attached sync, or scrape sluice's own process metrics (and a watched PlanetScale database's metrics) into Prometheus.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.115
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.115
```
