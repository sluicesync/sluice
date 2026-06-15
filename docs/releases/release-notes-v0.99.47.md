# sluice v0.99.47

**Bug-fix patch: the target connection-budget preflight no longer false-refuses cold start on a tight or modern (PG 18) managed Postgres.** If a `sluice migrate`/`sync` ever refused with `target connection budget exhausted` on a small managed Postgres that clearly had connections to spare, this is the fix.

## Fixed

- **Connection-budget preflight counts only client backends.** The preflight
  computed in-use connections with an unfiltered
  `SELECT count(*) FROM pg_stat_activity`, which also counts PostgreSQL's
  **background processes** — checkpointer, background/wal writer, autovacuum
  launcher, archiver, logical-replication launcher, and **PG 18+ async I/O
  workers**. None of those consume a `max_connections` slot, so in-use was
  over-reported by the background-process count (≈9 on a PG-18 managed
  instance). On a tight target — a managed Postgres with `max_connections=25`
  and ~9 background processes — this produced a **false
  `target connection budget exhausted`** that blocked **cold start entirely**,
  even though only ~4 real client backends were in use. The probe now counts
  `WHERE backend_type = 'client backend'` (PG 10+; sluice's pgoutput CDC
  already requires PG 10+). The role/database sub-probes already filtered
  correctly — only the global count was affected.

## Who needs this

- Anyone running sluice against a **small managed Postgres** (tight
  `max_connections`) — especially **PlanetScale Postgres and other PG 18
  instances** with async I/O workers — who hit a spurious
  `target connection budget exhausted` on cold start. No flag or config
  change needed; upgrade and re-run.

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.47
```
