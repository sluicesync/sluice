# ADR-0116: MySQL connection-budget prober + PlanetScale buffer-pool tier-proxy cap

- Status: Accepted
- Date: 2026-06-24
- Deciders: sluice maintainers
- Relates: ADR-0076 (two-axis copy budget split), ADR-0115 (metrics-aware restore headroom clamp), ADR-0107 (PlanetScale target telemetry), ADR-0019 (parallel within-table bulk copy), ADR-0084 (cross-table copy pool)

## Context

The connection-budget preflight (`internal/pipeline/connection_budget.go`, `resolveTargetCopyParallelism`) auto-caps the bulk-copy parallelism so a wide `--bulk-parallelism × --table-parallelism` product never exhausts a small target's connection slots mid-run. It discovers the capability structurally — `target.(ir.TargetConnectionBudgetProber)` — and acts on the returned `ir.ConnectionBudget`. **Only Postgres implemented the prober**, so for a MySQL/PlanetScale target the budget step was a clean no-op and the auto product passed through unbounded.

Two distinct gaps follow from that:

1. **Self-hosted MySQL had no connection bound at all.** A wide auto fan-out against a small MySQL `max_connections` could hit Error 1040 "Too many connections" mid-copy — the MySQL analogue of the Postgres slot-exhaustion FATAL the whole feature exists to prevent.

2. **On PlanetScale, a connection budget bounds the wrong resource.** Connections are abundant — vtgate fronts a large shared pool (`conns=6/250` observed during the large-scale program) — so connections are *not* the scarce small-tier resource. **CPU is**: a PS-10 is 1/8 vCPU and pins at 100% under a wide cold copy. There is no credential-free way to read a PlanetScale branch's CPU allocation directly.

ADR-0115 / ADR-0107 solve gap (2) the robust way — a live CPU/mem telemetry clamp — but that path requires the operator to configure a PlanetScale control-plane metrics token. We want a sensible default bound *without* any extra credential.

## Decision

**Implement `ir.TargetConnectionBudgetProber` on the MySQL engine (both flavors), mirroring the Postgres implementation, and fold in a no-credential PlanetScale-aware tier cap derived from `@@innodb_buffer_pool_size`.** No pipeline change is needed: the resolver already discovers the prober structurally, so the MySQL engine simply implementing the interface activates the budget step for MySQL targets.

### Part A — connection budget (the self-hosted-MySQL bound)

`Engine.ProbeTargetConnectionBudget` opens a short-lived connection via the engine's existing `parseDSN` + `openDB` path and reads:

- `MaxConnections` = `@@max_connections`.
- `InUse` = `Threads_connected` (`SHOW GLOBAL STATUS LIKE 'Threads_connected'`).
- `RoleLimit` = the tighter positive value of the per-user `MAX_USER_CONNECTIONS` (`mysql.user` for `CURRENT_USER()`) and the global `@@max_user_connections`; non-positive (MySQL's "no limit") normalises to the `unlimited` sentinel (`-1`, matching the Postgres catalog convention the `ir.ConnectionBudget` struct documents).
- `DatabaseLimit` = `unlimited` — MySQL has no per-database connection cap.
- `Available` = `min(MaxConnections − InUse, RoleLimit)`.
- `CopyBudget` = `Available − connBudgetReserve` (reserve = 4, matching the Postgres engine: the control/CDC connection + operator headroom), then min'd with the Part-B tier cap.
- `EffectiveParallelism` / `Capped` / `Refuse` / `RefusalError` are folded from `requested` + `ceiling` exactly as the Postgres engine does.

The role term is the per-user *limit* itself, not `limit − current`: MySQL exposes no cheap per-user current-connection count comparable to Postgres' `pg_stat_activity`-by-`usename`. This is a conservative upper bound on the role's free slots — it can only over-state availability when the user already holds many connections, which the server-level `Threads_connected` term already accounts for.

### Part B — buffer-pool tier-proxy cap (the PlanetScale CPU bound)

`@@innodb_buffer_pool_size` scales **monotonically by PlanetScale plan tier** and is **not** vtgate-masked. Live-measured (large-scale program, 2026):

| Tier | `@@innodb_buffer_pool_size` |
|---|---|
| PS-10 | 0.125 GB (128 MiB) |
| PS-20 | 0.83 GB |
| PS-40 | 1.64 GB |
| PS-80 | 4.91 GB |
| PS-160 | 9.80 GB |

So it is a usable no-credential proxy for "how big is this instance" → a defensible parallelism cap. `bufferPoolParallelismCap` buckets it (boundaries pinned as named constants, change-detector unit-tested):

| Buffer pool | Cap | Covers |
|---|---|---|
| `< 256 MB` | 2 | PS-10 / tiny dev box (128 MB default) |
| `< 2 GB` | 4 | PS-20 / PS-40 |
| `< 8 GB` | 6 | PS-80 |
| `>= 8 GB` | 8 | PS-160+ / sizeable self-hosted |

The cap is folded into the returned budget via the **MIN** of the connection-derived `CopyBudget` and the tier cap. It applies for **both** flavors (`mysql` and `planetscale`) — it is a harmless safe cap for self-hosted MySQL too, where buffer pool also correlates with box size. The boundary above PS-10's 128 MB is chosen so PS-10 buckets to the tightest cap of 2.

## Correctness / safety

- **Loud-failure-safe; worst case is wrong parallelism, never data loss.** The budget only changes *how wide* the copy fans out; the per-row/per-chunk correctness (disjoint chunks, idempotent upsert, position-then-commit) is untouched.
- **Managed-quirk tolerance is the load-bearing safety property.** Reading `mysql.user` requires `SELECT` on the `mysql` schema, which managed MySQL / PlanetScale routinely deny. That denial is **not** a probe failure: `probeRoleLimit` degrades the per-account term to `unlimited` and the budget proceeds from the always-readable `@@max_user_connections`. Likewise `@@innodb_buffer_pool_size` is best-effort — unreadable ⇒ 0 ⇒ the tier cap is a no-op. Only the two always-available variables (`@@max_connections`, `Threads_connected`) are hard requirements; failing to read either sets `ProbeFailed=true` + a `Warning` and returns a **non-refusing** budget so the orchestrator proceeds with the pre-budget (unclamped) behaviour. **Only a connection-OPEN failure (bad DSN) returns a non-nil error** — that's the operator's own DSN, worth failing on, the same posture as every other `Open*`.
- **Never raises, never below 1.** `clampParallelism` only reduces `requested` and floors at 1; the tier cap only lowers `CopyBudget`.
- **Complements, does not replace, ADR-0115/0107.** The metrics clamp is the robust always-correct CPU bound *when telemetry is configured*; this is the credential-free heuristic that applies when it is not, plus the genuine connection bound for self-hosted MySQL. They compose (both only reduce).

## Consequences

- A MySQL target now gets a connection bound it never had; a PlanetScale target gets a conservative CPU-proxy cap with zero extra configuration.
- The cap is coarse by design — the buckets trade precision for needing no credential. An operator who wants a precise CPU-driven bound configures the PlanetScale metrics token (ADR-0107) and gets the ADR-0115-class clamp on top (both reduce, so the tighter wins).
- Engine-general at the pipeline layer: no orchestrator change, the `ir.ConnectionBudget` contract is unchanged, and the resolver's existing structural type-assert is the only wiring.

### Residual

- The role term being the per-user *limit* (not `limit − current`) can slightly over-state availability on a heavily-shared account; the server-level `Threads_connected` term bounds the real exhaustion case, and the tier cap is the binding bound on PlanetScale anyway. A precise per-user current count would need an extra `information_schema.PROCESSLIST` aggregation per probe — not worth it for a startup-only heuristic.
- The buffer-pool buckets are calibrated against PlanetScale's 2026 tier sizes; a future tier re-sizing would shift the mapping. The change-detector test makes any boundary edit a deliberate, reviewed change.
