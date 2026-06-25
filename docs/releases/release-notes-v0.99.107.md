# sluice v0.99.107

**Rolling target-metrics history (ADR-0107 item 35): sluice now persists the polled PlanetScale target-health metrics into a bounded table on the target, so you can see the CPU/memory/storage trend without scripting the metrics API.** An advisory, default-on-when-telemetry-is-configured observability follow-up to the v0.99.92/v0.99.95/v0.99.106 PlanetScale metrics work.

## Added

**Persisted rolling history.** When PlanetScale telemetry is configured (`--planet-scale-org` + the metrics service token), a slow-tick sidecar — mirroring the storage-headroom watch, off the apply hot path — records each ~60 s metrics poll into a new `sluice_target_metrics_history` table on the target (where sluice's other metadata, like `sluice_cdc_state`, already lives). Each row carries the poll's CPU / memory / storage utilisation, the raw volume available/capacity bytes, replica lag, and connection counts, stamped with the source sample timestamp (deduped — the source only updates once a minute, so the cached sample is never double-recorded).

**Surfaced as a trend.** The `sluice diagnose` bundle gains a `health/target_metrics_history.json` section: the recent rows plus computed **current / 1 m / 5 m / 10 m avg+max** aggregates for CPU, memory, and storage — the "is storage climbing? did CPU just spike?" view — alongside the existing point-in-time `health/target_health.json`. Or just `SELECT ... FROM sluice_target_metrics_history` on the target yourself.

**Bounded and advisory.** Rows older than 7 days are pruned on a periodic pass, so the table stays tiny (~10 k rows at the 60 s cadence). The recorder is purely advisory: every ensure/record/prune error is logged at WARN and **swallowed** — it can never stall or crash the sync. An unobserved metric is stored as SQL `NULL` and reconstructed as "unknown" on read (never a misleading `0`/idle), the same honesty contract the live snapshot keeps.

**Engine-general.** Implemented behind the engine-neutral `ir.TargetMetricsHistoryStore` seam with both MySQL and Postgres stores, so a Postgres-on-PlanetScale target gets the same history table and diagnose section.

## Compatibility

No configuration changes and no behaviour change for any sync that doesn't configure PlanetScale telemetry (no `--planet-scale-org`) — the recorder simply never starts. When telemetry is configured, recording is **on by default**; `--suppress-target-metrics-history` opts out. The new table is additive (it never touches `sluice_cdc_state`, schema history, or user data). No resume-format, wire, or exactly-once changes — this is observability only.

## Who needs this

Anyone running a continuous `sync` into a **PlanetScale** target who has opted into metrics telemetry and wants the PlanetScale-UI-style trend (CPU/mem/storage over the last minutes/hours) inside sluice — for diagnosing a slow apply, or watching a non-Metal storage auto-grow approach — without building a metrics-API integration. Automatic once telemetry is on.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.107
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.107
```
