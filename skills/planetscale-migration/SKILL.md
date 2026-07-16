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
- For a safe-migrations target branch (deploy-ddl / expand-contract / the index-build fallback): `PLANETSCALE_SERVICE_TOKEN_ID` / `PLANETSCALE_SERVICE_TOKEN` env (a service token with branch + deploy-request scopes) — env, never argv.

## Steps

1. **Pick the driver deliberately — this is the #1 gotcha.** `--source-driver planetscale` selects the **VStream** CDC path (the correct, shard-aware source for a PS/Vitess keyspace). A `*.psdb.cloud` host under plain `--source-driver mysql` is **binlog CDC** (with `_vt_*` internal tables auto-excluded), NOT VStream — a different, non-shard-aware path. If you want VStream, you must say `planetscale`; don't rely on the hostname.

2. **Preflight, then run.** Use `migrate-preflight` (`sluice migrate --dry-run --format json …`) or `cdc-sync-operator` (`sluice sync start --dry-run …`). VStream cold-copy parallelism tunes via `--vstream-copy-table-parallelism` (read axis) and `--copy-fanout-degree` (write-side fan-out for a PS-MySQL target); the generic `--bulk-parallelism`/`--table-parallelism` are inert on a VStream source (a WARN fires if set) — the help text names the right knob.

3. **Target branch has safe migrations ON? Follow the governed DDL path — NEVER "disable safe migrations" on your own.** A safe-migrations PlanetScale branch refuses EVERY direct DDL statement (Error 1105), so a fresh `sluice migrate`/`sync` into it refuses coded `SLUICE-E-PS-DIRECT-DDL-BLOCKED` naming the exact refused statement — that refusal is the branch's production guardrail working, not a sluice failure. sluice never toggles the setting; disabling it is a human decision with a named tradeoff (surface it, don't improvise it). The governed path, in order:
   - **Bootstrap sluice's control tables once:** `sluice control-tables ddl` prints the exact CREATE statements (read-only, no credentials); ship each via `sluice deploy-ddl --org <org> --database <db> --ddl '<statement>'` (service token from `PLANETSCALE_SERVICE_TOKEN_ID` / `PLANETSCALE_SERVICE_TOKEN` env — never argv; `--dry-run` makes zero control-plane calls).
   - **Pre-create the user tables the same way:** `sluice schema preview` prints the target DDL; `sluice deploy-ddl` ships each CREATE.
   - **Then run:** a fresh `sluice migrate` needs NO flag — its pre-create shape gate (ADR-0166, v0.99.258+) detects each pre-created table, verifies the column shape matches, and skips the refused CREATE with an INFO (a mismatch refuses upfront with `SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH`). A sync stream skips schema-apply with `sluice sync start --schema-already-applied`.
   - **Arm the index-build fallback:** `migrate`'s deferred post-copy indexes can hit the same block (or the ~900s statement wall) — refusal `SLUICE-E-INDEX-DIRECT-DDL-DISABLED` when unarmed. Arm it with `migrate --planetscale-org <slug>` + the service-token env vars (optional `--planetscale-database` / `--planetscale-branch` / `--planetscale-deploy-timeout`); the pending indexes then build via a dev branch + deploy request on the already-copied data, no re-copy.
   - Schema *changes* against the branch later ride the same family: `sluice deploy-ddl` for one statement, `sluice expand-contract` for the full expand→migrate→contract pattern (`--yes` confirms a production DROP COLUMN — human approval required), `sluice backfill` for in-place data migration. See `docs/schema-change-runbook.md`.

4. **Expect storage-grow / reparent events mid-run — they're normal.** A Vitess keyspace under load grows its volume and reparents (primary hand-off); sluice rides these (errno-2013/1105 transition handling landed). A blip during the serving-transition window is expected, not a failure — don't abort on it. On a resume that lands on a purged binlog position (routine inside PS's retention window), sluice **auto-re-snapshots by default** (idempotent VStream copy absorbs the overlap without dropping the target). `--no-auto-resnapshot` converts that into a loud decision when a full re-snapshot is expensive and should be deliberate.

5. **Target ownership is advisory, never auto-fixed.** When the PG/MySQL target connects as an ephemeral `pscale_api_*` role, objects sluice creates are owned by it. sluice **surfaces this advisory and never auto-`ALTER OWNER`** (the "contain Postgres complexity" tenet). Relay the advisory so the operator reassigns ownership out-of-band if needed.

6. **Watch control-plane metrics (optional, no data-plane connection).** `sluice metrics-watch --engine <mysql|postgres|planetscale|vitess> --planetscale-org <slug> --planetscale-metrics-db <db>` polls the PS metrics endpoint for CPU/mem/storage/lag and fires threshold alerts (`--notify-slack`/`--notify-webhook` + `--notify-storage-util` etc.), with `--once` for a one-shot sample. Tokens come from the env vars above (never on the command line). The same telemetry can clamp migrate/restore parallelism by live headroom via `--planetscale-org` on those commands.

## What you return
- **Driver verdict:** VStream (`--source-driver planetscale`) vs binlog (`mysql`) — stated explicitly, with why.
- **Safe-migrations verdict (if the target is a PS branch):** whether the branch has safe migrations ON, and — on a `SLUICE-E-PS-DIRECT-DDL-BLOCKED` refusal — the governed bootstrap you followed (`control-tables ddl` → `deploy-ddl` → shape-gated `migrate` / `--schema-already-applied` sync), never a recommendation to disable the toggle.
- **Plan/run result:** the migrate/sync outcome; any storage-grow/reparent events observed and that they were ridden, not failures.
- **Ownership advisory:** if the target is a `pscale_api_*` role, the surfaced advisory + that sluice did not change ownership.
- **Metrics (if watched):** current CPU/mem/storage/lag and any threshold alerts.
- **Destructive steps (if any):** `--no-auto-resnapshot` fall-through remedies (`--restart-from-scratch` / `--reset-target-data`) named and flagged for human approval.

## References (canonical — don't duplicate)
`AGENTS.md` (drivers, envelope, PS token env vars) · `docs/operator/error-codes.md` (`SLUICE-E-PS-DIRECT-DDL-BLOCKED`, `-PS-SAFE-MIGRATIONS-DISABLED`, `-PS-BRANCH-STALE-BASE`, `-PS-DEPLOY-REQUEST-FAILED`, `-TARGET-TABLE-SHAPE-MISMATCH`, `-INDEX-STATEMENT-TIME-LIMIT`, `-INDEX-DIRECT-DDL-DISABLED`) · `docs/schema-change-runbook.md` (the deploy-ddl / expand-contract / backfill family) · `docs/managed-services.md` (safe-migrations bootstrap) · `skills/migrate-preflight/SKILL.md` · `skills/cdc-sync-operator/SKILL.md` · `sluice sync start --help` / `sluice metrics-watch --help`.
