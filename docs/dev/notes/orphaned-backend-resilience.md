# Orphaned-backend resilience (hard-kill / OOM / crash recovery)

**Status:** proposal / finding. Discovered 2026-06-01 while load-testing the
sluice-powered Heroku→PlanetScale migrator (`orware/sluice-heroku-migrator`)
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
sessions. Set something like `application_name=sluice/<stream-id>/<role>` (snapshot,
applier, cdc-reader) on every connection (one DSN/conn-config param per engine).

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

### Bonus: connection-budget awareness

The slot-exhaustion we hit is worsened by sluice's **parallel COPY** opening
several connections at once against a small target. Consider a
`--max-target-connections` cap (or adaptive backoff on `53300
too_many_connections` / the superuser-slots FATAL), and have the migrator default
it conservatively for small managed targets. Orthogonal to (1)–(3) but the same
incident surfaced it.

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
