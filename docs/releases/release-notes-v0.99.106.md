# sluice v0.99.106

**PlanetScale metrics integration Phase 3 (ADR-0107): the opt-in target telemetry now shapes apply concurrency, shows up in `sluice diagnose`, and speaks Postgres.** Three advisory follow-ups that make sluice act on and surface the target-health signal it already collects — all default-off when PlanetScale telemetry isn't configured, with no behaviour change for any other sync.

## Added

**(a) Auto lane-count from headroom.** When `--apply-concurrency` is left unset (the adaptive `auto:N` default) and PlanetScale telemetry is wired, the initial CDC apply lane count is now clamped by the target's live CPU/memory utilisation at startup. A target already near the high-water mark starts with fewer lanes — quartered at/above the mark, halved when approaching it — instead of piling the full default fan-out onto a hot instance and relying purely on the reactive per-lane AIMD to claw it back.

This is deliberately a STARTUP bias, not mid-stream re-partitioning. The CDC apply path partitions changes across a FIXED PK-hash→lane map; changing the lane *count* mid-stream would break the same-key→same-lane invariant that makes the concurrent apply exactly-once. The per-lane AIMD already owns the dynamic, per-lane sizing — this only sets the *initial* count more conservatively when the target is hot. It never raises an explicit operator value (only the unset `auto:N` path is touched), and it degrades to today's behaviour exactly when there is no provider, no fresh telemetry sample, or neither CPU nor memory observed. Because the lane count is re-resolved on each attempt, a transiently-hot target at one start yields more lanes on a later warm-resume once headroom recovers.

**(b) `sluice diagnose` target-health section.** The diagnose bundle now includes `health/target_health.json` — the most recent CPU / memory / storage / replica-lag / connection snapshot — so a recipient can see WHY apply was slow (a hot or storage-constrained target) without leaving the bundle. The `diagnose` subcommand accepts `--planet-scale-org` plus the metrics-token flags to populate it via a one-shot poll with a bounded warm-up. The section is honest in every state: "telemetry not configured" when no provider, `{"fresh": false}` when configured but no fresh sample landed, and otherwise the snapshot with each metric gated by its observed flag — an unobserved value is omitted, never reported as 0/idle.

**(c) Postgres-target metric names.** The metric-name table is now engine-parameterised. A Postgres target reads `planetscale_volume_available_bytes` / `planetscale_volume_capacity_bytes` and `planetscale_postgres_replica_lag_seconds` rather than the Vitess `planetscale_vttablet_volume_*` / `planetscale_mysql_*` names (CPU and memory are engine-shared pod metrics). PostgreSQL connection metrics are intentionally left unmapped: the documented PG connection surface is a per-state breakdown that does not fit the single-value-per-pod selection and has not been confirmed against the live endpoint, so it stays an honest "unobserved" gap (connections are a secondary signal) rather than risking a wrong reading — a tracked follow-up once the shape is verified live.

## Compatibility

No configuration changes and no behaviour change for any sync that does not configure PlanetScale telemetry (no `--planet-scale-org`). The auto lane-count clamp engages only on the unset `auto:N` path with a wired provider and a fresh sample; an explicit `--apply-concurrency` is honored verbatim as before. The exactly-once CDC contract and the per-lane AIMD are unchanged — this release biases only *how many* lanes start, never *what* they apply. The new diagnose section is additive.

## Who needs this

Anyone running a continuous `sync` into a **PlanetScale** target who has opted into metrics telemetry (`--planet-scale-org` + the metrics service token): apply concurrency now starts in proportion to the target's live headroom, and `sluice diagnose` bundles carry the target's resource snapshot. Postgres-on-PlanetScale targets now get the correct storage/lag metric names, which the upcoming non-Metal PG storage-autoscale validation depends on. Automatic; no action required beyond the existing opt-in flags.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.106
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.106
```
