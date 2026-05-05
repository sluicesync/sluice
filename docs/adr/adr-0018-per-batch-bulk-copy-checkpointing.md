# ADR-0018: Per-batch bulk-copy checkpointing for resume-mid-table

## Status

Accepted. Implemented in `internal/pipeline/migrate_bulk.go`,
`internal/engines/{mysql,postgres}/row_reader_batch.go`,
`internal/engines/{mysql,postgres}/row_writer_batch.go`,
`internal/ir/migration_state.go`. CLI surface in
`cmd/sluice/cli.go` (`--bulk-batch-size`); programmatic surface on
`pipeline.Migrator.BulkBatchSize`.

## Context

[ADR-0015](adr-0015-migration-resume.md) made `sluice migrate
--resume` recover from a mid-phase failure, but punted on per-batch
granularity for the bulk-copy phase: an `in_progress` table was
TRUNCATEd on resume entry and re-copied from row 0. The decision was
deliberate — per-batch state writes contend with bulk-load
throughput, and the COPY-protocol path commits all-or-nothing — but
v0.3.0's robustness testing exposed a corner the design doesn't
serve well.

A representative measurement: ~5 seconds for 20,000 rows on PG → PG
COPY-protocol bulk-load on the test harness. Production workloads
routinely have single tables in the 100M+ row range. Extrapolating,
that's tens of minutes to hours for a single table. A transient
network blip ten minutes into a four-hour copy of a single table
throws away ten minutes and earns the operator a fresh four-hour
copy. Operator pain compounds when several tables of that scale
exist — the failure mode shifts from "annoying" to "this is going
to take all weekend."

The v0.3.0 ADR called per-batch checkpointing "a future PR [that] can
layer per-batch checkpointing without changing the state-table shape
(the JSON column was chosen partly so this expansion is non-breaking)."
This ADR delivers that layering.

## Decision

Track a per-table cursor (last successfully-copied PK) in
`sluice_migrate_state.table_progress`. On resume, the bulk-copy phase
reads the cursor, asks the source for `WHERE pk > cursor ORDER BY pk
LIMIT batch_size`, applies the batch with idempotent INSERTs,
advances the cursor. Crash mid-batch → next attempt re-fetches the
un-checkpointed batch and the idempotent INSERT tolerates the dups.

Default batch size: 5000 rows. The flag exists to let operators tune
for their row-width and target-disk profile.

### Wire shape

The `table_progress` JSON column gains an object form alongside the
existing string form:

```json
{
  "users":      "complete",
  "orders":     {"state": "in_progress", "last_pk": [12345], "rows_copied": 12345},
  "products":   {"state": "in_progress", "last_pk": ["a", 7], "rows_copied": 8000},
  "events_log": "no_pk_truncate_and_redo"
}
```

`complete` and `no_pk_truncate_and_redo` use the bare-string form
because there's no cursor data to carry; `in_progress` uses the
object form for the cursor and row count.

`encoding/json`'s default unmarshal can't handle "string OR object"
on the same field, so the per-entry encode/decode lives on a custom
`MarshalJSON`/`UnmarshalJSON` for `ir.TableProgress`. The map
container is the standard `map[string]ir.TableProgress` and uses Go's
default container marshalling.

### Engine optional surfaces

Two new optional interfaces on `internal/ir/interfaces.go`, type-
asserted by the orchestrator at the per-table dispatch:

- **`BatchedRowReader`** extends `RowReader` with `ReadRowsBatch(ctx,
  table, after []any, limit int)`. Implementations emit a row-
  comparison predicate (`WHERE (pk1, pk2) > ($1, $2) ORDER BY pk1,
  pk2 LIMIT N`) which is the canonical form for descending into
  composite-PK ordering. Per-column boolean logic (`pk1 > ? OR (pk1
  = ? AND pk2 > ?)`) is incorrect for composite-PK descent and must
  not be used.
- **`IdempotentRowWriter`** extends `RowWriter` with
  `WriteRowsIdempotent(ctx, table, rows)`. Implementations emit
  upsert-form INSERT statements (`ON CONFLICT (pk) DO UPDATE` on PG;
  `INSERT ... AS new ON DUPLICATE KEY UPDATE` on MySQL 8.0.20+) so a
  re-applied batch tolerates the rows that already landed.

Both shipping engines (MySQL, Postgres) implement both. Engines that
don't implement them (none today) fall back to the v0.3.0
truncate-and-redo behaviour for in-progress tables — the orchestrator
type-asserts at dispatch time.

### Composite primary keys

Row-comparison (`WHERE (a, b) > ($1, $2) ORDER BY a, b`) is the
canonical form on both PG (since 8.2) and MySQL (since 4.1). The
predicate degrades cleanly to single-column ordering when the PK has
one column. Implementations emit it directly rather than synthesising
boolean logic.

The orchestrator's PK tracker captures the last row's PK column
values via a teeing wrapper around the row channel — same shape as
the existing `progressTicker` row counter — so no new return value is
threaded through the writer interface.

### Tables without a primary key

No PK = no cursor = no per-batch resumption. Detected at bulk-copy
classification; the table is marked
`"no_pk_truncate_and_redo"` on its first attempt and behaves exactly
as v0.3.0 does (TRUNCATE on resume entry, copy from scratch). The
sentinel is sticky across attempts so the orchestrator doesn't have
to re-detect on every resume.

A clear log line surfaces the fallback: operators expecting per-batch
resume on a no-PK table see the explicit message rather than
guessing.

### Position-and-data atomicity (deliberately weakened)

The CDC applier achieves position-and-data atomicity per
[ADR-0007](adr-0007-position-persistence.md) because the apply tx
writes both the data row and the position row inside the same target
transaction. The bulk-copy path can't compose with that shape: the
data write is a streamed multi-row INSERT (or COPY), and weaving a
side-table update into a streaming write is brittle.

The v1 design accepts a brief replay window: write the batch, then
write the cursor. A crash between the two re-applies the most-recent
batch on the next attempt. The idempotent INSERT (`ON CONFLICT DO
UPDATE` / `ON DUPLICATE KEY UPDATE`) makes that replay benign.

Sequential commit + idempotent re-deliver vs. atomic single-tx is the
kind of trade-off ADR-0007 made the opposite call on for streaming
CDC. The reasons are:

- The bulk-copy data path is structurally streaming: COPY in
  particular doesn't take a side-table update.
- The replay window is bounded by the per-batch commit cadence (one
  state write per batch, default 5000 rows), so the replay cost is
  capped.
- The per-batch upsert is the same SQL shape `change_applier` already
  uses for ADR-0010 idempotency. We're reusing well-understood
  semantics, not inventing new ones.

### Cold-start uses COPY; resume uses INSERT (PG)

The PG cold-start writer uses COPY FROM STDIN (3-5x faster than
batched INSERT). COPY can't be checkpointed mid-stream — pgx's
`CopyFrom` consumes the row channel in a single logical operation
that commits all-or-nothing. The resume path drops to batched
INSERTs to gain the per-batch cursor boundary.

The throughput cost is real but bounded: the resume path is the
recovery path, not the steady-state path. A cold-start migration
that runs to completion never pays it. A resume run pays slower-by-
INSERT throughput on the in-progress tables, but skips the previously-
completed tables entirely (the v0.3.0 win).

We could synthesise something like a chunked-COPY (COPY a batch's
worth, commit, COPY the next batch) to keep the resume path on the
COPY protocol, but that would force a breakdown of pgx's CopyFrom
into per-batch invocations. The complexity isn't worth the
throughput gain on the recovery path.

### Backward compatibility with v0.3.0 state rows

A row written by v0.3.0 has `table_progress[name] = "in_progress"` (a
bare string, no cursor). The v0.4.0 unmarshal path accepts this and
decodes it into `TableProgress{State: TableProgressInProgress, LastPK:
nil, RowsCopied: 0}`.

The orchestrator's classifier sees the nil LastPK and routes to
truncate-and-redo — the same behaviour the v0.3.0 binary would
produce. An in-flight migration that started on v0.3.0 and resumes
on v0.4.0 does not gain mid-table resume; only fresh migrations do.
This is documented in the operator-facing release notes when v0.4.0
ships.

## What this design deliberately does not do (yet)

- **Source-table modifications mid-resume.** If someone DELETEs a row
  on the source after the cursor has moved past it, the deleted row
  stays on the dest. Production migrations are expected to run
  against quiesced or read-only sources; this is a known caveat, not
  a regression. CDC-driven incremental sync is the supported answer
  for live sources.
- **Atomic position-and-data within a single tx for the bulk path.**
  Sequential commit + idempotent re-deliver is the v1 shape; an
  engine-specific extension that can wrap a side-table update inside
  the per-batch tx would close the replay window further but is not
  in scope.
- **Per-row checkpointing.** Cursor writes happen at batch
  boundaries, not row boundaries. Per-row checkpointing would
  collapse to "commit every row," which is exactly the
  pre-batched-CDC throughput cliff
  [ADR-0017](adr-0017-batched-cdc-apply.md) addressed for the
  apply path.
- **A flag to disable the new behaviour.** Resume default is fine:
  the plain-INSERT / COPY path wins on speed when the target is
  empty (no upsert overhead), and the upsert path is the correct
  shape when the target has data from a previous attempt. The
  orchestrator picks based on `--resume` already; there's no
  configuration to expose.
- **Deferred classification of no-PK tables on cold-start.** A
  cold-start cleanly completes a no-PK table's bulk-copy without
  cursor tracking; the `no_pk_truncate_and_redo` sentinel only gets
  written when a no-PK table is in flight at the moment of failure.
  Cold-start success → entry is `complete`, no fallback marker
  needed.

## Consequences

**Win.** A failed migration of a single huge table re-runs from the
last checkpoint (default: every 5000 rows) rather than from row 0.
For a 100M-row table that fails at the 50M mark, resume copies
50M rows, not 100M.

**Cost.** Slower bulk-copy throughput on the resume path because of
the upsert overhead and the dropped-to-INSERT shape on PG. For a
clean cold-start migration the path is unchanged: plain INSERT (PG
batched) or COPY (PG fast) with no upsert overhead and no per-batch
state writes.

**Wire-shape forward-compat.** The object form has named fields with
JSON tags; future fields can be added without breaking older readers
(v0.4.0 binaries reading a v0.5.0 row simply ignore the unknown
field). Older binaries will not understand new fields, but state-row
forward-compat is a one-way street; we explicitly support upgrade,
not downgrade.

**Operator surface.** `--bulk-batch-size N` lands on `sluice migrate`.
Help text covers tuning. Default 5000 was picked as a middle ground
between replay-window size and per-tx commit overhead; production
tuning experience may move it.

**No-PK regression mitigation.** v0.3.0's no-PK behaviour
(truncate-and-redo) is preserved exactly. The orchestrator detects
no-PK tables and routes them away from the cursor path. The no-PK
sentinel in `table_progress` makes the fallback explicit rather than
silent.
