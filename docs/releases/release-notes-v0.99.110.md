# sluice v0.99.110

**PlanetScale telemetry on a Postgres target now reads CPU and memory — they were being silently dropped.** A small but real correction to the PlanetScale-metrics support (ADR-0107), found by a live MySQL → PlanetScale-Postgres test: CPU and memory are the operator's stated #1-priority signals, and they weren't observed on PG targets.

## Fixed

**CPU/mem on a Postgres target.** PlanetScale emits the per-pod CPU/mem metric (`planetscale_pods_*_util_percentages`) once **per container** on a Postgres branch — `postgres`, `pgbouncer`, `walg-daemon`, all under `planetscale_component="hzinstance"` with no `tablet_type` label. sluice's primary-series selector was Vitess-shaped (it looks for `component="vttablet"` + `tablet_type="primary"`), found no match, and — with more than one series — refused to guess, so `CPUKnown`/`MemKnown` stayed false for every PG target. (Storage was unaffected: the PG volume metric is a single series, so the single-series fallback already selected it correctly.)

The selector is now engine-aware: a Postgres target selects the `planetscale_container="postgres"` series; MySQL/Vitess keep the existing `vttablet` + `primary` cascade unchanged, and the single-series and graceful-refuse fallbacks are preserved for both. Verified against the live PlanetScale Postgres metrics endpoint. This makes the proactive AIMD back-off (item 32), the rolling metrics history (item 35), and the threshold alerts (item 36) all actually see CPU/mem on a Postgres target.

**Postgres replica-lag metric name.** ADR-0107 Phase 3 guessed `planetscale_postgres_replica_lag_seconds`, which the live endpoint does not expose (it has `planetscale_postgres_wal_archiver_lag_bytes` / `wal_size_bytes`, a different signal; and a single-node PG has no replica lag). The bogus name already produced `LagKnown=false` (no match), so this is behaviorally a no-op — the constant is simply unset now, keeping PG replica-lag an honest "unobserved" rather than naming a non-existent series. PG connection metrics remain intentionally unmapped, as before.

## Compatibility

No configuration changes. MySQL/Vitess targets are byte-for-byte unchanged (the selector only adds a Postgres-container preference ahead of the existing cascade). For a Postgres target with PlanetScale telemetry configured, CPU/mem now populate where they previously read as unobserved; nothing that was already correct changes. Advisory/observability only — no effect on the apply path or the exactly-once contract.

## Who needs this

Anyone running `sluice sync` into a **PlanetScale Postgres** target with metrics telemetry configured (`--planet-scale-org` + the metrics token): CPU and memory utilisation now feed the proactive back-off, the rolling history table, and the threshold alerts. No action required beyond the existing opt-in flags.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.110
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.110
```
