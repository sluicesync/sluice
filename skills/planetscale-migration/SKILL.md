---
name: planetscale-migration
description: Use to migrate or continuously-sync against PlanetScale / Vitess (VStream, storage-grow/reparent events, target ownership, control-plane metrics). Grounds the PlanetScale-specific driver choice and gotchas on top of migrate/sync. Gated — writes to the target. Trigger when the source or target is PlanetScale / Vitess / a *.psdb.cloud host, or the user says "migrate to/from PlanetScale / Vitess".
---

# planetscale-migration

Handle the PlanetScale/Vitess specifics that sit on top of a normal migrate or sync. State-changing; the destructive-flag approval rules from `migrate-preflight` / `cdc-sync-operator` still apply.

## When to use
Either endpoint is PlanetScale or a self-managed Vitess keyspace — a `*.connect.psdb.cloud` host, a PS branch, or a vtgate. The engine matrix registers `mysql`, `planetscale`, and `vitess` flavors (`sluice engines`).

## Inputs you need
- Source/target DSNs (env: `SLUICE_SOURCE` / `SLUICE_TARGET`) and the correct **driver** (the load-bearing choice below).
- For control-plane metrics: `PLANETSCALE_METRICS_TOKEN_ID` / `PLANETSCALE_METRICS_TOKEN` env (a service token granted `read_metrics_endpoints`), the org slug, and the database name.

## Steps

1. **Pick the driver deliberately — this is the #1 gotcha.** `--source-driver planetscale` selects the **VStream** CDC path (the correct, shard-aware source for a PS/Vitess keyspace). A `*.psdb.cloud` host under plain `--source-driver mysql` is **binlog CDC** (with `_vt_*` internal tables auto-excluded), NOT VStream — a different, non-shard-aware path. If you want VStream, you must say `planetscale`; don't rely on the hostname.

2. **Preflight, then run.** Use `migrate-preflight` (`sluice migrate --dry-run --format json …`) or `cdc-sync-operator` (`sluice sync start --dry-run …`). VStream cold-copy parallelism tunes via `--vstream-copy-table-parallelism` (read axis) and `--copy-fanout-degree` (write-side fan-out for a PS-MySQL target); the generic `--bulk-parallelism`/`--table-parallelism` are inert on a VStream source (a WARN fires if set) — the help text names the right knob.

3. **Expect storage-grow / reparent events mid-run — they're normal.** A Vitess keyspace under load grows its volume and reparents (primary hand-off); sluice rides these (errno-2013/1105 transition handling landed). A blip during the serving-transition window is expected, not a failure — don't abort on it. On a resume that lands on a purged binlog position (routine inside PS's retention window), sluice **auto-re-snapshots by default** (idempotent VStream copy absorbs the overlap without dropping the target). `--no-auto-resnapshot` converts that into a loud decision when a full re-snapshot is expensive and should be deliberate.

4. **Target ownership is advisory, never auto-fixed.** When the PG/MySQL target connects as an ephemeral `pscale_api_*` role, objects sluice creates are owned by it. sluice **surfaces this advisory and never auto-`ALTER OWNER`** (the "contain Postgres complexity" tenet). Relay the advisory so the operator reassigns ownership out-of-band if needed.

5. **Watch control-plane metrics (optional, no data-plane connection).** `sluice metrics-watch --engine <mysql|postgres|planetscale|vitess> --planetscale-org <slug> --planetscale-metrics-db <db>` polls the PS metrics endpoint for CPU/mem/storage/lag and fires threshold alerts (`--notify-slack`/`--notify-webhook` + `--notify-storage-util` etc.), with `--once` for a one-shot sample. Tokens come from the env vars above (never on the command line). The same telemetry can clamp migrate/restore parallelism by live headroom via `--planetscale-org` on those commands.

## What you return
- **Driver verdict:** VStream (`--source-driver planetscale`) vs binlog (`mysql`) — stated explicitly, with why.
- **Plan/run result:** the migrate/sync outcome; any storage-grow/reparent events observed and that they were ridden, not failures.
- **Ownership advisory:** if the target is a `pscale_api_*` role, the surfaced advisory + that sluice did not change ownership.
- **Metrics (if watched):** current CPU/mem/storage/lag and any threshold alerts.
- **Destructive steps (if any):** `--no-auto-resnapshot` fall-through remedies (`--restart-from-scratch` / `--reset-target-data`) named and flagged for human approval.

## References (canonical — don't duplicate)
`AGENTS.md` (drivers, envelope, PS telemetry env vars) · `docs/operator/error-codes.md` (`SLUICE-E-INDEX-STATEMENT-TIME-LIMIT`, `-INDEX-DIRECT-DDL-DISABLED`) · `skills/migrate-preflight/SKILL.md` · `skills/cdc-sync-operator/SKILL.md` · `sluice sync start --help` / `sluice metrics-watch --help`.
