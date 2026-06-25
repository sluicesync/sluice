# sluice v0.99.118

**HIGH fix: a PlanetScale storage-grow reparent that lands on the *post-copy DDL phase* (index build, foreign-key/constraint creation, views, identity-sequence sync) no longer aborts the whole migration/restore — those phases now ride the reparent out, the same way the data-copy phase already does (ADR-0114).**

The cold-copy *data* phase has ridden a storage-grow reparent end-to-end for several releases (ADR-0108/0109/0110/0113). This release closes the matching gap in the phases that run *after* the data is copied: building indexes, adding foreign keys/constraints, creating views, and syncing identity sequences. Affects both `restore` and `migrate`/`sync` against any reparenting target (notably a fresh non-Metal PlanetScale instance growing from zero). Data was never at risk here — the failure was always loud — so this removes a needless abort after a successful copy.

## Fixed

The Track-C cross-engine **MySQL → PostgreSQL restore** caught it live: all 29 tables landed byte-perfect against the backup manifest (312,970,106 rows, 0 mismatches) while riding one PlanetScale storage-grow reparent — and then `create index … on live_churn` died with `FATAL: terminating connection due to administrator command (SQLSTATE 57P01)` when a *second* reparent opened exactly as the index phase began. Result: `rc=1` after a fully correct ~32-minute copy.

This was never Postgres-specific. The same `CreateIndexes` / `CreateConstraints` / `CreateViews` / `SyncIdentitySequences` call sites are shared by both engines and live in **both** orchestrators (`restore` and `migrate`), so a plain cross-engine or same-engine migration was equally exposed. We hit it on Postgres first only because a from-zero-growing cross-engine PG target reparents more often during a run and PG `CREATE INDEX` holds a long-lived connection — a wide collision window. MySQL/Vitess reparents the same way (Error 1105 "not serving" / the brief read-only serving-transition window).

**The fix (ADR-0114):** one engine-general bounded reparent-retry helper wraps every post-copy DDL phase, gated by a new optional classifier the engine schema writers implement by delegating to the **same** error classifier the apply / cold-copy paths already use — so the DDL path now recognizes a reparent identically to the row-write path, with no second classifier that could drift. A real DDL fault (a genuine type error, a constraint violation) still fails loudly on the first attempt with no retry; only a classified storage-grow / reparent transient is ridden out, on a ~30-minute wall-clock bound that surfaces loudly on exhaustion. Each wrapped phase is already idempotent on re-run (`CREATE INDEX IF NOT EXISTS` / detect-then-skip constraints / `CREATE OR REPLACE` views / `setval`), so a retry never duplicates or corrupts.

## Validation

- Unit pins: the retry helper retries a classified transient then succeeds; returns a real DDL fault unchanged after a single attempt (no retry); surfaces a loud terminal error wrapping the last transient on exhaustion; unwinds promptly on context cancel; never consults the classifier when the first attempt succeeds.
- Unit pins (both engines): the schema writer's transient verdict classifies the real reparent shapes (PG `57P01`/`57P03`/disk-full grow class; MySQL "not serving"/disk-full/read-only) as retriable and a genuine DDL fault (unique violation, syntax error) as terminal.
- Live origin: the Track-C cross-engine restore that exposed the gap — data verified byte-perfect against the manifest before the abort, confirming this was an index-phase resilience gap, not a data-loss bug.
- Landed through CI's `-race` integration gate before tagging.

## Compatibility

No API or flag changes. No behaviour change on the happy path — a DDL phase that succeeds first try is byte-for-byte identical, and a real DDL fault still fails immediately and loudly. The only change is that a transient storage-grow / reparent during a post-copy DDL phase is now ridden out instead of aborting the run.

**Residual (tracked):** the Postgres-`migrate` *overlapped* copy-and-index path (where per-table indexes are built interleaved with the copy, inside the engine) needs an engine-level retry and is a documented follow-up; the restore path that failed live uses the now-wrapped index phase.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.118
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.118
```
