# sluice v0.99.121

**MySQL targets now get the same connection-budget bound on automatic copy parallelism that Postgres already had — plus a no-credential, PlanetScale-tier-aware CPU cap (ADR-0116, roadmap item 41).**

## Added

sluice bounds its automatic bulk-copy parallelism (cross-table × within-table) to what the target can actually serve. Until now that bound only applied to Postgres, which exposes a connection-budget prober; MySQL/PlanetScale targets had no prober, so the automatic product passed through unbounded. This release implements the prober for the MySQL engine.

**Connection budget (Part A).** The MySQL engine now reads the target's connection accounting — `@@max_connections`, `Threads_connected`, and the per-account / global user-connection limit — and caps the copy pool to the real free-slot budget, mirroring the Postgres path: it refuses loudly when not even one slot is free, and degrades to a warning-and-proceed (the prior unbounded behaviour) when a managed target forbids the accounting queries (a common PlanetScale/managed posture). The orchestrator picks the prober up structurally — no pipeline change — so the budget step simply activates for MySQL targets.

**No-credential PlanetScale tier cap (Part B) — the part that matters most on PlanetScale.** On PlanetScale, connections are abundant (vtgate fronts a large shared pool) but CPU is the scarce resource on small tiers, so a connection budget alone bounds the wrong thing. sluice has no credential-free way to read a branch's CPU allocation directly — but `@@innodb_buffer_pool_size` scales monotonically by plan tier and isn't masked by vtgate (live-measured PS-10 0.125 GB → PS-160 9.80 GB). sluice buckets that size into a conservative parallelism cap (`<256 MB → 2`, `<2 GB → 4`, `<8 GB → 6`, `≥8 GB → 8`) and folds it into the budget as a lower bound, so a small PlanetScale instance — or a tiny self-hosted box — isn't fanned out wider than its CPU can serve, without needing a metrics token.

This complements the metrics-aware clamp from v0.99.119 (ADR-0115): the telemetry clamp is the robust, always-correct path when configured; this buffer-pool heuristic is the credential-free fallback, and the genuine connection bound for self-hosted MySQL with a real `max_connections` wall.

**Guarantees:** advisory and safe — it only ever lowers the automatic parallelism, never raises it; an unreadable buffer-pool size is a no-op; a probe failure logs a warning and proceeds with the prior behaviour. The bucket boundaries are pinned by a change-detector test, and the prober is covered by unit tests (the budget math, the managed-permission-denied degrade) plus container-backed integration tests.

## Compatibility

No flag changes. The effect is that an automatic MySQL/PlanetScale bulk copy now starts no wider than the target's connection budget and tier-proxy cap allow (it could previously over-subscribe a small target). An explicitly-pinned `--table-parallelism` / `--bulk-parallelism` still flows through the existing budget split. No change to value handling or correctness.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.121
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.121
```
