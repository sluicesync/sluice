# ADR-0082: Per-table migration-state rows — O(1) checkpoint writes

## Status

Accepted — repo-audit task M2.3 / harness #16 (operator-authorized; the audit's HIGH-rated P1 performance finding). Implemented 2026-06-10.

### Implementation notes (what landed)

- `internal/migratestate` — the shared store skeleton (`Store` + the `Config`/`SQL` dialect seam, the legacy decoder, the one-time upgrade transaction, `FormatLegacyBlob`/`FormatPerTableRows`, `UpgradedBlobSentinel`), following the ADR-0081 tier-c precedent: engines pass finished SQL + a two-field config; ensure-DDL and the column-migration mechanism stay engine-side.
- Both engines' `migration_state.go` became thin dialect shims (`newMigrationStateStore` builds the statement set; `EnsureControlTable` keeps the DDL + the `state_format` column migration — PG `ADD COLUMN IF NOT EXISTS`, MySQL detect-then-ALTER).
- `ir.MigrationStateStore` grew `WriteTableProgress(ctx, migrationID, tableName, progress)`; `Write`'s contract changed to "header + upsert any entries present; never delete absent entries".
- Pipeline hot paths (`setTableProgressAndWrite`, the per-batch cursor write in `copyTableWithCursor`, all four chunk-checkpoint sites in `migrate_parallel.go`, `markTableIndexesBuilt`) now clone ONE entry under `stateMu` and upsert ONE progress row. Phase-transition writes (`markPhase` / `markFailed` / `markComplete` / `markFailedLocked`) strip the map via `headerOnly` and write the header row only. `cloneStateForWrite` (whole-map deep clone) was replaced by `cloneTableProgressForWrite` (one entry).
- Schema readers exclude the new `sluice_migrate_table_progress` table alongside the other `sluice_*` bookkeeping tables (ADR-0015).
- Measured numbers below; cross-version pin = `migration_state_upgrade_integration_test.go` ×2 engines against a byte-captured v0.99.x blob.

## Context

Through v0.99.x the resumable-migration state store (ADR-0015/0018) kept the entire `map[table]TableProgress` as one JSON blob in the single `sluice_migrate_state` row. Every per-table breadcrumb (≥2 per table: the in-progress breadcrumb + the terminal complete), every per-5000-row resume cursor, every per-chunk checkpoint, and every ADR-0077 `IndexesBuilt` flag did:

1. deep-clone the whole map under `stateMu` (`cloneStateForWrite`),
2. JSON-encode the whole map,
3. upsert the full blob into the ONE hot row.

At the 10k-table scale the ADR-0076 cross-table pool explicitly targets, that is ~0.86 MB of JSON re-encoded and re-written per checkpoint, ≥20k times per migration — **O(N²) total work** (~17 GB of write amplification through the target's MVCC/TOAST on a single row), plus an O(N) clone inside every `stateMu` hold, contending with all four pool workers. The repo audit measured and rated this HIGH (P1).

## Decision

Split the store into a **header row** and **per-table progress rows**, and give the store an O(1) per-table write surface.

### Schema

```
-- sluice_migrate_state (header; one row per --migration-id; same table as before)
migration_id    VARCHAR(255) PRIMARY KEY
phase           VARCHAR(32)  NOT NULL
table_progress  TEXT NULL        -- ≤v0.99.x: the whole-map blob; now: the upgrade sentinel
state_format    INT NOT NULL DEFAULT 1   -- NEW (additive): 1=legacy blob, 2=per-table rows
started_at / updated_at / last_error     -- unchanged

-- sluice_migrate_table_progress (NEW; one row per table)
migration_id    VARCHAR(255) NOT NULL
table_name      VARCHAR(255) NOT NULL
progress        TEXT NOT NULL    -- ONE ir.TableProgress JSON value (same per-entry wire
                                 -- shape as inside the old blob — the upgrade re-keys,
                                 -- it never re-encodes)
updated_at      TIMESTAMP NOT NULL
PRIMARY KEY (migration_id, table_name)
```

Store surface changes (`ir.MigrationStateStore`):

- `WriteTableProgress(migrationID, tableName, progress)` — NEW; one progress-row upsert. All hot-path checkpoint writes go through it.
- `Write(state)` — header upsert, plus an upsert of any entries present in `state.TableProgress` (sorted, one tx). It **never deletes** absent entries, so the pipeline's phase-transition writes pass a `headerOnly` copy (nil map) and cost one statement. Entries only accrue during a migration, so "snapshot-merge" and "snapshot" are equivalent — and the pre-existing round-trip pins (Write a populated map → Read it back) hold unchanged.
- `Read(migrationID)` — merges header + progress rows; `UpdatedAt` is the max across header and rows so the operator-facing age still reflects checkpoint activity.

The store logic lands ONCE in `internal/migratestate` behind the same dialect-seam shape as ADR-0081 tier (c): engines own quoting/placeholders/upsert syntax/PG schema qualification + the ensure DDL; the shared package owns scan loops, tolerance ladders, the upgrade transaction, and error shapes.

### Legacy upgrade (one-time, transactional) and the one-way sentinel

A row written by ≤v0.99.x reads back with `state_format = 1` (the additive column's default). The first `Read` for that migration_id explodes the blob into per-table rows **inside one transaction**:

```
BEGIN
  DELETE FROM sluice_migrate_table_progress WHERE migration_id = ?   -- orphan clearing
  INSERT ... one row per entry, in sorted table order
  UPDATE header SET table_progress = <sentinel>, state_format = 2
COMMIT
```

**Crash-safety invariant: a crash at ANY point during the upgrade is safe.** Before commit the header still says `state_format = 1` with the blob intact (rolled back), so the next resume re-runs the whole upgrade; after commit the rows are authoritative. The delete-first step makes the re-run idempotent even against orphan progress rows left by an earlier life of the same migration_id (e.g. an old binary's `ClearMigration`, which only knew about the header table). Pinned by the upgrade integration tests (orphan-row pre-seed + double-Read idempotence) and by the seam-level rollback unit pin.

The sentinel written over the blob is **deliberately invalid JSON**: a ≤v0.99.x binary later pointed at the upgraded row fails its `table_progress` decode loudly instead of silently reading "no progress" and re-copying every table (loud failure beats silent re-copy; the sentinel text tells a psql-inspecting operator where the progress went). Every new-format header write — including a fresh migration's first `pending` row — carries the sentinel + `state_format = 2`, so the one-way contract is uniform. State-row compatibility was already documented one-way at v0.4.0 → v0.3.0; this keeps that contract and makes the failure mode loud. Because `loadOrInitState` always `Read`s before any write, an old binary can never blindly `Write` over an upgraded row: its `Read` refuses first.

### Concurrency / deadlock-ordering argument

- Pool workers (ADR-0076: up to `tableParallelism × withinParallelism` checkpoint writers) now upsert **different rows** — that contention reduction is the point. Each `WriteTableProgress` is a single-statement autocommit upsert: it holds at most one row lock and waits on at most one, so no cycle is possible among hot-path writers.
- The only multi-row writers are the upgrade tx and the full-snapshot `Write` tx. Both insert in **sorted table order**, so two concurrent multi-row writers for the same migration lock rows in the same order — no deadlock cycle even though single-process ownership of a migration_id is already the operating contract.
- The in-memory `stateMu` discipline is unchanged (the ADR-0076 `-race` lessons): map mutations happen under the lock; the JSON encode happens outside it on a copy cloned under the lock. The unit of cloning shrank from the whole map (`cloneStateForWrite`) to the touched entry (`cloneTableProgressForWrite` — the entry's `Chunks` backing array is shared with peer chunk goroutines of the same table, so it still must be cloned under the lock).

### Measured delta (the audit's numbers requirement)

In-process per-checkpoint cost at 10k tables (`internal/migratestate` benchmarks, i7-9700K, Go 1.x, windows/amd64):

| | legacy full blob | per-table row | delta |
|---|---|---|---|
| CPU | 11.69 ms/op | 945 ns/op | **~12,400× less** |
| JSON payload | 855,535 B/op | 67 B/op | **~12,770× less** |
| allocations | 6.49 MB / 40,057 allocs | 400 B / 5 allocs | — |

End-to-end against real Postgres (`TestMigrationStateCheckpointCost_Measure10k`, same host, shared testcontainer, 50/1000 iters):

| | legacy full-blob upsert | per-table row upsert | delta |
|---|---|---|---|
| wall/checkpoint | 31.74 ms | 377 µs | **84× less** |
| bytes written/checkpoint | 855,535 (through TOAST, one hot row) | ~67 (one small row) | ~12,770× less |

Over a 10k-table migration (≥20k checkpoint writes), the legacy path writes ~17 GB of state JSON; the new path writes ~1.3 MB.

## Alternatives considered

- **Header-only `Write` (TableProgress ignored on the write side).** Cleanest in theory, but it changes `Write`'s contract under every existing caller and pin, and any missed mutate-then-`Write` site becomes a silent progress loss. The snapshot-merge `Write` keeps the old contract observable (round-trip pins unchanged) while the pipeline opts into header-only via `headerOnly` — the strictness lives at the call site, not in a contract change.
- **Batched/coalesced blob writes (debounce the hot row).** Keeps the O(N) encode and the hot row, just less often — and widens the crash-replay window per checkpoint. Rejected: it trades the audit finding for a worse resume contract.
- **JSONB column + per-key updates (PG `jsonb_set`).** PG-only (MySQL JSON path updates differ structurally), still rewrites the TOASTed datum per update, and puts dialect JSON-path knowledge into shared code — exactly what the IR-first tenet keeps out.

## Consequences

- Checkpoint writes are O(1) in table count; `stateMu` holds no longer scale with N (no O(N) clone under the lock).
- One more `sluice_*` bookkeeping table per target (schema readers exclude it; `--reset-target-data` clears it via `ClearMigration`).
- State-row forward-compat is one-way **and loud**: after a new binary touches a migration row, an old binary refuses to read it (decode error naming the sentinel text) instead of silently re-copying. Operators downgrading mid-migration must start a fresh migration_id or drop the row — release notes must call this out.
- The phase-transition `Write` no longer refreshes per-row `updated_at`s; `Read` surfaces max(header, rows) so the observable `UpdatedAt` semantics are preserved.
- `Read` before `EnsureControlTable` against a legacy-shaped table now errors on the missing `state_format` column (the pipeline always ensures first; the documented tolerant-read contract — missing **table** reads as "no row" — is unchanged and pinned).

## Test pins

- Seam-level unit pins (`internal/migratestate`, scripted fake driver): the header/rows merge incl. `UpdatedAt` max, both missing tolerances, upgrade statement order + sorted inserts + commit, upgrade-failure rollback, header-only vs full-snapshot `Write` split, single-statement `WriteTableProgress`, delete order in `ClearMigration`, validation refusals.
- Pipeline unit pins: `setTableProgressAndWrite` → `WriteTableProgress` (never whole-state `Write`); phase marks are header-only and don't disturb persisted progress.
- **Cross-version pin** (×2 engines, integration): a v0.99.x-shape table + blob — byte-captured from the v0.99.33-identical encoder, covering EVERY persisted `TableProgress` family × shape (bare complete; object complete+indexes_built; cursored in_progress with 1-col and multi-col PKs; bare v0.3.0 in_progress; no-PK sentinel; chunked with complete + in-progress chunks — the Bug 74 pin-the-class discipline) — upgrades once, idempotently, clearing orphans, with the sentinel failing the emulated legacy decoder.
- Pre-existing engine store round-trip pins and the whole `TestMigrate_*Resume*` / cross-table-pool / parallel-chunk / fast-loader / reset integration suites pass unchanged (zero pin edits).
