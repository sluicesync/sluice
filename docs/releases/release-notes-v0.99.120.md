# sluice v0.99.120

**Closes the one residual from v0.99.118's reparent-resilience fix: a PlanetScale storage-grow reparent landing on the Postgres-`migrate` *overlapped* index build now rides the reparent instead of aborting the migration (ADR-0114 overlap residual, roadmap item 42).**

## Fixed

v0.99.118 (ADR-0114) made the post-copy DDL phases — index, constraint, view, and identity-sequence creation — ride a storage-grow/reparent instead of aborting after a correct data copy. But that wrap sits at the pipeline orchestrator level, so it couldn't reach the Postgres `migrate` **overlap** optimization (ADR-0077): to hide the index-build tail, that path builds each table's indexes *inside the engine* as that table's copy finishes, concurrently with the other tables still copying. A reparent that hit one of those interleaved `CREATE INDEX` statements still cancelled the whole copy-and-index errgroup and aborted the migration.

This release wraps the overlap path's per-table index build in an in-engine bounded reparent-retry that reuses the **exact same** retry envelope and error classifier as the cold-copy chunk retry. On a classified storage-grow/reparent transient (disk-full, `57P0x`, the read-only serving-transition window, a connection drop) it closes the dead pinned connection, backs off, re-acquires a fresh connection against the reparented primary, re-tunes it, and replays the build — `CREATE INDEX IF NOT EXISTS` is idempotent, so a replay after an interrupted build is clean (a non-`CONCURRENTLY` build is transactional, leaving nothing on interruption, and the `IF NOT EXISTS` skips one that did land). A real DDL fault still fails loudly on the first attempt with no retry; a genuinely-wedged target surfaces a loud terminal error after the wall-clock bound — never silent, never infinite.

Scope: only the Postgres-`migrate` overlap path was exposed — `restore` uses the now-wrapped whole-schema index phase (v0.99.118), and MySQL has no overlap builder. With this, every post-copy index-build path across `migrate` and `restore`, on both engines, rides a PlanetScale storage-grow/reparent.

## Validation

The retry policy is extracted as a pure, SQL-free function and pinned by unit tests: rides-a-transient-then-succeeds (re-acquiring a fresh connection before each retry), a real DDL fault returns terminal with no retry, a re-acquire that itself hits a transient is ridden, budget exhaustion surfaces a loud terminal error wrapping the last transient, and a context cancel during backoff unwinds promptly. Landed through CI's `-race` integration gate before tagging.

## Compatibility

No API or flag changes; no behaviour change on the happy path. The only change is that a transient storage-grow/reparent during the Postgres-migrate overlapped index build is now ridden out instead of aborting the migration.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.120
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.120
```
