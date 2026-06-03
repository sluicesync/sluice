# Orphaned-backend resilience (hard-kill / OOM / crash recovery)

**Status:** proposal / finding. Discovered 2026-06-01 while load-testing the
sluice-powered Heroku→PlanetScale migrator (`sluicesync/sluice-heroku-migrator`)
against real Heroku Postgres and a small single-node PlanetScale Postgres.

## What happened

A `sync start` snapshot was hard-killed mid bulk-copy (`docker rm -f` on the
container = SIGKILL; equivalently an OOM kill, a crash, or a network partition).
The local sluice process died instantly, but the **server-side `COPY <table> FROM
STDIN` backend kept running** on the target: a `COPY FROM STDIN` blocks reading
from the client socket, and when the client vanishes without a clean close the
server doesn't notice until TCP keepalive / `tcp_user_timeout` fires — minutes to
hours on managed Postgres. While it waits it **holds a lock on the target table**,
which then blocks the `DROP` / `TRUNCATE` / `CREATE` that a restart's cold-start
or `--reset-target-data` needs. The orphan also **holds a connection slot**; on a
small target we later saw enough leftover/parallel connections to exhaust the
slot budget entirely (`FATAL: remaining connection slots are reserved for roles
with the SUPERUSER attribute`), which blocks *new* connections — including the
operator's psql trying to clean up.

So the failure is self-amplifying: an orphan blocks recovery **and** consumes a
slot, and several of them can lock the operator out of the very database they
need to fix.

## What sluice already does (so this is a narrow gap, not a hole)

sluice **already drains gracefully on SIGTERM/SIGINT** (`internal/pipeline/stream.go`,
`cmd/sluice/cli.go`: the kong context cancels on SIGTERM and the pipeline unwinds;
`internal/pipeline/stop_signal.go`). Heroku dyno cycling, `docker stop`, and Ctrl-C
all send **SIGTERM first**, so in normal operation sluice closes its connections
cleanly and the server releases locks immediately. The orphan only appears under
**SIGKILL / OOM / crash / partition**, where no in-process handler can run. That
is the class this proposal targets.

## Gaps and proposed capabilities

### 1. Self-identifying connections via `application_name` (enabler + observability)

Today sluice does **not** set a distinctive `application_name` on its own
connections — the only `application_name` references in the tree
(`internal/engines/postgres/position_from_manifest_preflight.go`) *read*
`pg_stat_replication` to detect Patroni standbys; nothing labels sluice's own
sessions. Set `application_name=sluice/<role>/<stream-id>` (role ∈ snapshot,
applier, cdc-reader, schema, control) on every connection — role *before* the id
so the `sluice/` prefix + role survive PostgreSQL's silent 63-byte
(`NAMEDATALEN-1`) truncation of `application_name`; the id tail is what gets
clamped (one DSN/conn-config param per engine).

Value on its own: operators can find sluice's sessions in `pg_stat_activity`
instead of guessing a PID. It is also the **enabler** for (2): you cannot safely
reap what you cannot identify.

### 2. Stale-backend detection + opt-in reaping

On `sync start` / `migrate` preflight (and as a standalone `sluice doctor` /
`sluice recover` if warranted), detect backends that are (a) labeled with this
stream's `application_name`, (b) owned by the connecting role, and (c) holding a
lock on a sluice-managed target table while their owning run is gone. Then —
per the **"surface Postgres complexity explicitly, never silently auto-handle"**
and loud-failure tenets — **report loudly by default** and terminate only on
explicit opt-in:

```
sluice: stale sluice backend pid=12345 (stream "ps_import", role "appuser")
        holds AccessExclusiveLock on public.bench_events, orphaned from a run
        last seen 2026-06-01T20:41Z. Re-run with --reap-stale-backends to
        terminate it, or clear it manually with pg_terminate_backend(12345).
```

**Privilege note:** a non-superuser can `pg_terminate_backend` its *own* backends,
and sluice connects as the migration role — so it can reap its own orphans with
**no extra grants**. (This is distinct from terminating *other* roles' backends,
which it should never attempt.)

### 3. Keepalive / `tcp_user_timeout` on Postgres connections

sluice already has a shared keepalive dialer (`internal/netkeepalive`), but it is
wired only into the **MySQL** engine (`internal/engines/mysql/connect.go`,
`cdc_reader.go` via `keepaliveNet` / `RegisterDialContext`); the **Postgres**
engine uses pgx defaults. Extend the same dialer (or libpq
`keepalives`/`keepalives_idle`/`keepalives_interval` + `tcp_user_timeout`) to the
PG snapshot, applier, and CDC connections. This shrinks the half-open-detection
window so orphans self-clear faster. It is partial help for the server-side
`COPY FROM` case specifically (the server is *reading*, so its own keepalive
governs detection), but it is correct hygiene and helps every other half-open
case (applier writes, CDC reads).

### 4. Connection-budget awareness for parallel COPY

`--bulk-parallelism` (ADR-0019) defaults to `min(8, NumCPU)` and opens one target
connection per chunk (`COPY FROM STDIN`), **blind to the target's connection
budget** — `runtime.NumCPU()` on a managed-PG host typically reports the host's
core count, so a tight node gets hit with ~8 + control connections regardless of
its `max_connections`. On a small node (e.g. PlanetScale PS-5 with
`max_connections=20`, minus `superuser_reserved_connections`, minus the rig's own
usage), that plus any leaked backends exhausts the slot budget and fails
opaquely *mid-COPY* (`FATAL: remaining connection slots are reserved for roles
with the SUPERUSER attribute`) instead of loud-and-early at preflight.

**Probe the budget at target preflight:**

```sql
SHOW max_connections;                                              -- global cap
SHOW superuser_reserved_connections;                              -- non-superuser can't use these
SELECT count(*) FROM pg_stat_activity;                            -- current total
SELECT rolconnlimit FROM pg_roles WHERE rolname = current_user;  -- per-role cap (-1 = unlimited)
SELECT count(*) FROM pg_stat_activity WHERE usename = current_user;
SELECT datconnlimit FROM pg_database WHERE datname = current_database();
```

**Effective non-superuser budget:**

```
global_available = max_connections - superuser_reserved_connections - current_total
role_available   = rolconnlimit  < 0 ? +inf : rolconnlimit  - role_current
db_available     = datconnlimit  < 0 ? +inf : datconnlimit  - db_current
available        = min(global_available, role_available, db_available)
copy_budget      = available - reserve   # reserve for control conn + CDC reader + operator headroom (~3-4)
effective_parallelism = clamp(requested, 1, copy_budget)
```

- If `copy_budget < 1`, **refuse loudly** at preflight with the numbers
  (`target has 18/20 slots in use; only 2 available to role X, need ≥ <reserve+1>`),
  pointing at the stale-backend reaper (1)+(2) as the likely fix — never start a
  copy that can't finish.
- **Auto-cap is default-on** (it only ever *reduces* parallelism on a tight
  target — safe), with an explicit `--max-target-connections N` ceiling override
  and the existing `--bulk-parallelism` as the requested upper bound.
- **Adaptive backoff (DONE — Phase 2b):** if a chunk's connection open (or
  first statement) returns SQLSTATE `53300` (`too_many_connections`, which also
  covers the superuser-reserved-slots FATAL), the parallel-copy pool
  multiplicatively-decreases the effective parallelism (halve, floor at 1), waits
  a bounded exponential backoff, and retries the chunk rather than failing the
  run; a permanently-saturated target gives up loudly after a bounded number of
  retries / total wait (never an infinite spin). This is defense-in-depth for the
  rare race where slots vanish *after* Phase 1's budget preflight measured them as
  free (a peer process grabbed them mid-copy). Only the slot-exhaustion class is
  retried — every other open error (bad DSN, permission denied, a real COPY
  failure) still fails loudly and immediately, never masked as backpressure. The
  retry is safe against double-copy because a `53300` fails at connection
  open/ping, strictly before any COPY/`WriteRows` runs, so a retried chunk wrote
  zero rows and replays from its recorded cursor. Implementation:
  the engine-side classifier (`ir.ConnectionSlotClassifier`, PG SQLSTATE 53300 in
  `internal/engines/postgres/connection_slot.go`), the pure AIMD decision
  (`internal/pipeline/copy_backoff.go`), and the shared effective-parallelism gate
  + per-chunk retry seam (`internal/pipeline/copy_parallelism_gate.go`,
  `runChunks`/`acquireChunkConn` in `migrate_parallel.go`). Mirrors the applier
  batch AIMD's feel (`change_applier_batch.go` /`sluice_apply_batch_size_*`); the
  copy path is decrease-only (no additive-increase within a bounded one-shot
  copy — re-probing upward would just re-trigger the same 53300).
- `application_name` (1) lets the probe distinguish *sluice's own* connections
  from everyone else's, so the budget math and any reaping target only our slots.

This is the same-incident companion to (1)–(3): reaping stops the leak, budget
awareness makes each run self-limit and fail early instead of opaquely.

## Why this is correctness, not just convenience

The orphan doesn't only annoy an operator — it can **block sluice's own recovery**
(`--reset-target-data` / cold-start `DROP`), and on a small target it can lock out
all new connections. (1)+(2) are the high-value pair: self-identifying sessions
plus an opt-in reaper surfaced in the existing preflight, reusable by the
migrator's reset path. (3) is cheap, isolated, and reduces the window for every
half-open case.

## Test ideas

- Pin: start a snapshot, `SIGKILL` the process mid-`COPY`, confirm a follow-up
  `sync start --resume` (or `--reset-target-data`) detects the orphan in preflight
  and (with the flag) reaps it, then completes — instead of hanging on the lock.
- Pin: assert every sluice connection carries the expected `application_name`
  across snapshot / applier / CDC, both engines.
- Negative: assert sluice never terminates a backend owned by a *different* role.
