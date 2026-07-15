# ADR-0159: standalone `sluice backfill` — the expand-contract "migrate" step

## Status

Accepted (implemented; Phases 1–2 of roadmap item 62). Ships `sluice backfill
--driver <engine> --dsn <dsn> --table <t> --set 'col = <expr>' [--set ...]
[--where '<predicate>'] [--batch-size N] [--dry-run] [--restart] [--verify]
[--verify-only]` for MySQL (all flavors — PlanetScale/Vitess ride the same
path) and Postgres. Phase 3 (PlanetScale expand-contract orchestration) is
tracked on roadmap item 62.

## Context

After a schema change ships (on PlanetScale, via a deploy request), users
routinely need the middle step of the expand→migrate→contract pattern: a
**data migration** that backfills/transforms values from an old column into a
new one. No vendor or ORM ships this step as a batched, resumable, online
primitive — Prisma documents a one-off `findMany`+`update` loop in a single
transaction, which on PlanetScale/Vitess hits the same synchronous-
transaction-time wall a direct `ALTER` hits (errno 3024, ground-truthed in
ADR-0148) and on any engine holds long locks and bloats undo. The research
record is `docs/research/data-migration-backfill.md`; its conclusion is that
sluice already owns every hard part — the keyset chunker (ADR-0096), the
resumable cursor store (ADR-0019/0082/0111), and a proven sync-time backfill
kernel (`--BackfillAddedColumn`, ADR-0058 §1c) — and only the standalone
surface was missing.

## Decision

### One engine surface, IR-first

A new optional engine surface in `internal/ir/interfaces.go`:
`ir.BackfillExecutor` (`NextChunkUpperBound` / `ExecBackfillChunk` /
`BackfillStatement` / `CountRemaining` / `Close`) opened via
`ir.BackfillExecutorOpener`, the same optional type-asserted opener pattern as
`MigrationStateStoreOpener`. The orchestrator (`pipeline.Backfiller`,
`internal/pipeline/backfill.go`) never imports engine packages; engines
without the surface (SQLite/D1) are refused loudly with
`SLUICE-E-BACKFILL-UNSUPPORTED-ENGINE`.

### Sequential keyset walk, bounded UPDATE per chunk

The walk discovers its own bounds as it goes — no boundary precompute:
`NextChunkUpperBound(after, limit)` returns the PK tuple of the limit-th row
past the cursor (or the last remaining row, or "done"), then
`ExecBackfillChunk` issues exactly one

```sql
UPDATE t SET col = <expr>, ... WHERE (pk...) > (after...) AND (pk...) <= (upper...) AND (<where>)
```

Both bounds are row-comparison predicates on the PK tuple, compared by the
server in the column's **native collation** — the same exactly-once contract
`BoundedBatchedRowReader` pins for the chunked copy (ADR-0096): the boundary
walk and the chunk UPDATE agree on one total order by construction, so no
boundary-straddling row can be skipped or double-covered. Each statement is
bounded to at most `--batch-size` PK-walk rows (default: the bulk-copy
default, 5000 via `migcore.DefaultBulkBatchSize`), which keeps locks short and
stays far from the PlanetScale errno-3024 wall. **No parallel workers in
Phase 1** — a backfill is a background maintenance operation; sequential keeps
the load profile predictable and the resume state a single cursor.

Tables without a usable key are refused loudly
(`SLUICE-E-BACKFILL-NO-PRIMARY-KEY`): no PK, a sluice-injected planning-only
PK column, or a non-orderable PK type (JSON/array/geometry) — the same
eligibility reasoning `migcore.CanParallelChunkTable` applies to chunked
copies. There is deliberately no force flag: an unbounded UPDATE is the exact
statement shape the command exists to avoid.

### Native verbatim SQL for `--set` and `--where`

A backfill runs inside ONE database, so there is no cross-dialect translation
to do: the `--set 'col = expr'` right-hand side and the `--where` predicate
are emitted verbatim in the engine's own dialect — the `--expr-override`
posture. `--set` is parsed at the FIRST `=` so expressions containing `=`
(CASE arms) pass through intact; a `--set` column that does not exist on the
read schema is refused up front (`SLUICE-E-BACKFILL-UNKNOWN-COLUMN`).
`--dry-run` prints the exact per-chunk UPDATE (engine-rendered, placeholders
symbolic) plus a `CountRemaining` estimate and exits without writing anything
— rows or control table. `CountRemaining` also doubles as the `--where`
preflight: an unparsable predicate fails before any UPDATE runs.

### Resume state in the same database via `migratestate.Store`

The cursor persists through the existing `ir.MigrationStateStore` (the
ADR-0082 header + per-table-progress tables), keyed
`backfill:<table>:<12-hex sha256 of set+where>`: a re-invocation with the same
spec resumes its cursor; a different spec starts fresh; `--batch-size` is
excluded from the hash so retuning it doesn't orphan a cursor. The cursor
(`ir.TableProgress.LastPK`) is written only AFTER each chunk commits; a crash
in the gap re-executes at most one chunk on resume, and the operator's
self-describing `--where` guard (e.g. `new_col IS NULL`) is what makes that
replay a no-op — the same "the target predicate self-describes doneness"
property the CDC apply idempotency relies on. A re-run of a completed spec is
a reported no-op (exit 0, zero rows touched); `--restart` clears the stored
state and walks again from the start.

**Cursor JSON-round-trip normalization.** `LastPK` persists as JSON, so each
engine normalizes scanned PK values at the walk boundary: `[]byte` → string
(base64-through-JSON would re-bind as garbage — a silently misplaced cursor,
the worst failure class) and `time.Time` → the engine's native literal form
(MySQL cannot reliably compare RFC 3339's `T`/`Z` shape; PG keeps the offset
so timestamptz PKs re-bind correctly). The known shared wart inherited from
the migrate resume path: an integer PK value beyond 2^53 would lose precision
through JSON's float64 — unchanged here and noted rather than fixed, since
both paths share the store's wire shape.

### Everything else reused, nothing forked

Engine executors reuse each engine's `quoteIdent` and mirror the batched
reader's predicate builders; the CLI (`cmd/sluice/backfill.go`) reuses
`resolveEngine` + `applyEngineOptions` + `kongContext` + the ADR-0155
`runWithProgress` pretty view with a new two-phase `BackfillProgressSpec`;
error codes ride the `sluicecode` registry + doc-sync test.

### Phase 2: the verify post-pass (`--verify` / `--verify-only`)

The explicit "safe to ship the contract deploy request" signal that closes
the expand→migrate→contract loop. `--verify` runs one whole-table
`CountRemaining` on the `--where` guard AFTER the walk completes (or after
the completed-spec no-op — deliberately post-walk so it sees rows inserted
behind the cursor during an online run, not just the walked range).
`--verify-only` is the standalone, scriptable form of the same gate: no walk,
no control-table reads or writes, no UPDATEs — and therefore none of the
walk's PK requirements (a no-PK table is verifiable) and no `--set` needed
(any given is still schema-checked).

The exit contract: 0 remaining prints the safe-to-contract completion line
and exits 0; a nonzero count is the coded `SLUICE-E-BACKFILL-INCOMPLETE`
error — **runtime** class (exit 1, mirroring verify/diff's "ran cleanly,
found work" semantics), not a refusal, because the check ran truthfully and
found unfinished work: the online catch-up signal (re-run — a completed spec
needs `--restart` — then verify again), or on a quiesced database, a guard
that does not self-describe doneness. A failed verify does NOT mark the
migration state failed — the walk itself succeeded and its persisted work
stands.

Both verify modes **require `--where`**: without a self-describing guard the
remaining-count is the whole table and the signal is meaningless.
Contradictory combinations (`--verify-only` with `--dry-run`/`--restart`,
`--verify` with `--dry-run`) are refused at both the kong layer (xor groups)
and the orchestrator. Phase 2 also hoists the unsupported-engine refusal
ahead of table resolution, so an engine without the backfill surface always
gets the coded `SLUICE-E-BACKFILL-UNSUPPORTED-ENGINE` answer even when the
table is also missing.

## Consequences

- Operators get the missing expand-contract middle step as one command that
  is online-safe by construction, works unattended (resumable, idempotent
  re-run), and refuses the unsafe shapes loudly instead of degrading.
- Two new files per engine concern (`backfill.go` in each engine package)
  and one orchestrator; no orchestrator or reader/writer surface changed, so
  the blast radius on existing paths is zero.
- The `sluice_migrate_state` header table now also carries `backfill:*`
  migration ids with a `backfill` running phase; inspection tooling that
  assumed only migrate ids will see them (they are namespaced and terminal
  states reuse the migrate phase strings).
- A chunk re-executed after a crash double-applies when the operator's
  `--where` does NOT self-describe doneness (or is omitted). This is
  documented behavior (the guard is the idempotency contract, as in the
  research doc), not silent: the resume log line names the replayed cursor.

## Alternatives considered

- **Parallel chunk workers (rejected for Phase 1).** The ADR-0096 boundary
  sampler + N workers would multiply throughput, but a backfill's bottleneck
  is intentionally the write lock budget, not read fan-out; parallel UPDATE
  lanes multiply lock pressure on a live table — the opposite of the
  command's online-safety posture. Revisit on demand with the AIMD/grow-gate
  throttle (ADR-0110/0115) as the governor.
- **IR-translated expressions (rejected).** Translating `--set` through the
  IR expression translator (ADR-0016) buys nothing same-DB — there is no
  dialect boundary to cross — and would reject exactly the engine-specific
  functions operators reach for. Verbatim native SQL with a `--dry-run`
  preview is the `--expr-override` precedent.
- **SELECT-then-UPDATE row loop (rejected).** The Prisma shape (read PK +
  values, compute client-side, write per row) adds a round trip per row and
  a value-fidelity surface for zero gain when the transform is expressible
  in SQL; server-side `UPDATE ... SET col = expr` keeps values inside the
  engine.
- **A dedicated backfill state table (rejected).** The migrate-state store
  already has the exact shape (header phase + per-table cursor rows), both
  engines implement it, and its id namespace makes collisions impossible;
  a second bookkeeping table would be surface without new capability.
