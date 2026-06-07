// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"time"
)

// SchemaReader extracts an IR [Schema] from a live database.
type SchemaReader interface {
	ReadSchema(ctx context.Context) (*Schema, error)
}

// DDLStatement is a single emitted DDL statement, returned by
// [DDLPreviewer.PreviewDDL]. The Kind / Table fields are present for
// grouping in the operator-facing preview output (see ADR-0024); the
// pipeline that consumes them does not interpret them otherwise.
type DDLStatement struct {
	// Table is the unqualified table name the statement applies to.
	// For statements whose subject isn't a single table (e.g. PG
	// CREATE TYPE, which lives in the schema namespace), use the
	// table that owns the construct so output groups consistently.
	Table string

	// Kind is a short operator-friendly tag — "CREATE TABLE",
	// "CREATE INDEX", "CREATE TYPE", "ALTER TABLE", etc. Used only
	// for preview-output structure; engines may pick the spelling
	// that matches their own DDL idiom.
	Kind string

	// SQL is the statement itself, without a trailing newline. The
	// preview emitter joins statements with blank lines.
	SQL string
}

// DDLPreviewer is the optional engine surface for "produce the DDL you
// would emit, but don't execute it" — the read-side counterpart to the
// schema-write phase. Used by `sluice schema preview` (ADR-0024) so
// operators can inspect the target schema before any data moves.
//
// Engines implement this on the same type that satisfies
// [SchemaWriter] (today: SchemaWriter for both Postgres and MySQL).
// The pipeline preview orchestrator type-asserts on this surface;
// engines without a preview path surface a clear error rather than
// silently no-op.
//
// The returned slice is in the same logical order the writer would
// execute statements in: enum/type prerequisites first, then CREATE
// TABLE, then secondary indexes, then foreign-key constraints. The
// preview formatter groups by Table for output, so the emit order
// only matters insofar as cross-statement dependencies need to be
// readable in the printed result.
type DDLPreviewer interface {
	PreviewDDL(ctx context.Context, s *Schema) ([]DDLStatement, error)
}

// ColumnDDLPreviewer is the optional engine surface for emitting a
// single column's DDL fragment without writing it. Used by `sluice
// schema diff` (ADR-0029) to render `ADD COLUMN` suggestions for
// missing columns with the actual type, default, and generated-
// expression filled in — operators get a copy-paste-ready ALTER
// TABLE rather than a `-- TYPE` placeholder they have to fill in by
// hand.
//
// Engines implement this on the same type that satisfies
// [SchemaWriter] / [DDLPreviewer]. The column argument carries the
// type / default / generated metadata; table is needed only for IR
// types that require table context (PG enum names, today). table
// may be nil for callers that don't need that context — the
// implementation's contract is to either succeed or return a clear
// error explaining why table is required.
//
// The diff orchestrator type-asserts on this surface; engines that
// don't expose it fall back to the bare `-- TYPE` placeholder shape
// of the renderer rather than failing the whole diff.
type ColumnDDLPreviewer interface {
	EmitColumnDef(ctx context.Context, table *Table, col *Column) (string, error)
}

// SchemaWriter applies an IR [Schema] to a target database in three
// phases plus a small post-bulk-copy reconciliation step. Splitting
// schema creation from index/constraint creation is what enables
// fast bulk-loading: data is loaded into bare tables, then indexes
// and constraints are added once the data is in place.
type SchemaWriter interface {
	CreateTablesWithoutConstraints(ctx context.Context, s *Schema) error
	CreateIndexes(ctx context.Context, s *Schema) error
	CreateConstraints(ctx context.Context, s *Schema) error

	// SyncIdentitySequences advances each identity-column's sequence
	// past the maximum value present in the target table. Called
	// once after bulk-copy completes (between row-load and index
	// creation in the orchestrator). Without this, a target that
	// received explicit-id rows via bulk-copy would have its
	// sequence left at its default; the next user-initiated INSERT
	// would collide with bulk-copied IDs.
	//
	// Engines whose identity mechanism auto-bumps on direct INSERT
	// of explicit values (MySQL InnoDB) implement this as a no-op.
	SyncIdentitySequences(ctx context.Context, s *Schema) error

	// CreateViews emits CREATE VIEW / CREATE MATERIALIZED VIEW for
	// every entry in s.Views. Called as the final schema-apply phase
	// (after foreign keys) so all referenced tables exist on the
	// target. Implementations MUST be tolerant of view-to-view
	// dependencies: the orchestrator relies on a single-pass with
	// retries (see [pipeline.Migrator]) to converge on declaration
	// orders that don't sort topologically without a SQL parser.
	//
	// Idempotent: re-running on a target that already has matching
	// views is safe — both engines emit `CREATE OR REPLACE VIEW`
	// for regular views. Materialized views (Postgres only) are not
	// idempotent in the strict sense: a second apply will raise
	// "relation already exists"; the orchestrator's retry policy
	// treats this as success on the second pass.
	//
	// Engines without view support (none today) should implement
	// this as a no-op for an empty s.Views and return a clear error
	// on a non-empty list.
	CreateViews(ctx context.Context, s *Schema) error
}

// RowReader streams rows from a single table for the bulk-copy phase.
// Implementations should close the returned channel when the table is
// fully read, and return promptly when ctx is cancelled.
//
// Err is the load-bearing loud-failure surface (Bug 68). Streaming
// readers scan and decode rows on a background goroutine after
// ReadRows has already returned its channel; a per-row scan/decode
// failure there cannot be returned synchronously. The implementation
// MUST store such an error and surface it via Err so the orchestrator
// can distinguish "channel closed because the table was fully read"
// from "channel closed because a row failed". Callers MUST call Err
// after the channel has been fully drained (or ctx-cancelled) and
// treat a non-nil result as a hard migration failure. Without this
// check a mid-stream decode error silently truncates the table and
// the migrate exits 0 with missing rows — the worst failure class
// under the project's loud-failure tenet.
//
// Err returns nil before the first ReadRows call and after a clean,
// fully-drained read. It is concurrency-safe to call after the
// channel closes; concurrent calls during an in-flight stream are
// not required to be meaningful (the contract is "read it once the
// channel is drained").
type RowReader interface {
	ReadRows(ctx context.Context, table *Table) (<-chan Row, error)
	Err() error
}

// BatchedRowReader is an optional extension of [RowReader] for engines
// that support PK-ordered cursor-paginated reads. The bulk-copy
// orchestrator probes for this via type assertion when --resume is in
// flight; engines that don't implement it (or tables without a PK)
// fall back to the v0.3.0 truncate-and-redo behaviour for in-progress
// tables.
//
// The contract is "give me up to limit rows whose PK is strictly
// greater than after, in PK order". Pass nil after for the first
// batch. The returned channel closes when no more rows match (and the
// orchestrator interprets a zero-row close as "table fully read").
//
// For composite PKs, after is the slice of PK column values in PK
// declaration order. Implementations emit a row-comparison predicate:
//
//	WHERE (pk1, pk2, ...) > ($1, $2, ...) ORDER BY pk1, pk2, ...
//
// Both PG and MySQL natively support row-comparison; per-column
// boolean logic is incorrect for composite-PK descent and must not
// be used.
//
// For tables without a PK, implementations return an error; the
// orchestrator falls back to truncate-and-redo for that table. See
// ADR-0018 for the full design.
type BatchedRowReader interface {
	RowReader

	// ReadRowsBatch returns up to limit rows from table where the PK
	// is strictly greater than after (in PK column order), streamed
	// over the returned channel in PK ascending order. The channel
	// closes when limit is reached or no more matching rows exist.
	//
	// Returns a non-nil error for tables without a primary key; the
	// caller is expected to fall back to non-batched reads in that
	// case.
	ReadRowsBatch(ctx context.Context, table *Table, after []any, limit int) (<-chan Row, error)
}

// RowWriter performs bulk inserts using the target's native fast-load
// path (COPY, LOAD DATA INFILE, batched INSERTs, etc.). Implementations
// should consume rows until the channel is closed or ctx is cancelled.
type RowWriter interface {
	WriteRows(ctx context.Context, table *Table, rows <-chan Row) error
}

// MaxBufferBytesSetter is the optional surface a [RowWriter] or
// [ChangeApplier] can implement to accept a soft byte-size cap on
// per-batch buffered memory. The pipeline orchestrator threads
// [pipeline.Migrator.MaxBufferBytes] / [pipeline.Streamer.MaxBufferBytes]
// to every writer/applier that exposes this setter; engines that
// don't implement it use whatever batching they had before (row-count
// only).
//
// Zero or negative bytes means "no byte cap" (the engine's row-count
// cap remains the only flush trigger). Positive values are interpreted
// as a soft target — a single row larger than the cap still applies
// rather than wedging the writer; the cap bounds *accumulation*, not
// individual rows.
//
// See ADR-0028 for the design rationale.
type MaxBufferBytesSetter interface {
	SetMaxBufferBytes(bytes int64)
}

// CopyCheckpointFunc persists a snapshot-COPY resume position to the
// durable control table. The pipeline supplies an implementation that
// upserts the position row for the current stream (the same row the
// cold-start CDC anchor and the apply path write), so a fault mid-COPY
// resumes from the checkpoint rather than restarting the whole table.
// It is called from the engine's COPY-pump goroutine, so the
// implementation must be safe to call concurrently with the apply path
// (in practice the apply path isn't running yet during cold-start
// bulk-copy, but the position-row write must still be self-contained).
type CopyCheckpointFunc func(ctx context.Context, pos Position) error

// CopyCheckpointer is the optional surface a snapshot [RowReader] can
// implement to accept a periodic COPY-cursor checkpoint sink
// (ADR-0072 Phase B). The pipeline threads a [CopyCheckpointFunc] that
// persists the in-progress snapshot position to the control table on a
// bounded cadence (every N rows or T seconds, whichever first). Engines
// whose snapshot reader carries a resumable COPY cursor — today only the
// VStream cold-start reader, whose position round-trips Vitess's
// per-shard TablePKs — implement this; engines without a mid-COPY resume
// cursor (vanilla MySQL, Postgres) don't, and the checkpoint is simply
// not wired (their cold-start re-copies from the snapshot anchor on
// retry, unchanged).
//
// The sink must be set BEFORE bulk-copy drains the snapshot; the
// pipeline calls it on the cold-start path right after opening the
// stream. A nil func disables checkpointing (the pre-ADR-0072 behaviour:
// position persisted only at COPY_COMPLETED).
type CopyCheckpointer interface {
	SetCopyCheckpoint(fn CopyCheckpointFunc)
}

// CopyDurableProgressFunc reports that the cold-start bulk-copy writer
// has just DURABLY committed flushedRows more rows to the target
// (v0.99.9). It is a per-flush DELTA, not a running total: the sink sums
// the deltas into the global durable frontier, which sidesteps any
// per-table reset (the writer is invoked once per table and its internal
// counters restart each call, but the deltas accumulate cleanly across
// tables).
//
// The reader-side checkpointer uses the summed frontier to keep the
// persisted COPY checkpoint at-or-behind the durable-write frontier: a
// snapshot reader whose checkpoint position advances as rows are RECEIVED
// from the source (the VStream pump's TablePKs cursor) would, on a hard
// crash, leave the cursor AHEAD of the rows actually written to the
// target — and resume would silently skip the un-written gap.
//
// The reporter is the bulk-copy writer (the only component that knows
// when a batch is durable: after each successful flush). The sink is the
// snapshot reader, which gates its checkpoint on the running sum.
// flushedRows is always > 0 (an empty flush reports nothing).
type CopyDurableProgressFunc func(flushedRows int64)

// CopyDurableProgressReporter is the OPTIONAL surface a bulk-copy
// [RowWriter] implements to report its durable-write frontier to the
// snapshot reader's checkpoint (v0.99.9). After each successful flush the
// writer invokes the supplied [CopyDurableProgressFunc] with the
// cumulative count of rows it has durably committed for the table being
// copied.
//
// The pipeline wires the reporter to the reader's
// [CopyDurableProgressSink] on the cold-start COPY path so the persisted
// checkpoint never runs ahead of the durable rows (the invariant that
// makes a hard-crash resume gapless). A nil func disables reporting.
// Writers whose target has no resumable mid-COPY reader on the source
// (every non-VStream path) can implement it harmlessly — the sink is
// simply not wired when the reader doesn't carry a cursor.
type CopyDurableProgressReporter interface {
	SetCopyDurableProgress(report CopyDurableProgressFunc)
}

// CopyDurableProgressSink is the OPTIONAL surface a snapshot [RowReader]
// implements to RECEIVE the bulk-copy writer's durable-write frontier
// (v0.99.9). The reader EXPOSES [AdvanceDurableRows]; the pipeline hands
// that method to a [CopyDurableProgressReporter] writer so the reader's
// [CopyCheckpointer] persists a position no further ahead than the last
// durably-written row.
//
// Only a reader whose checkpoint position can outrun the durable frontier
// needs it — today the VStream cold-start reader, whose TablePKs cursor
// advances as rows are received into a bounded in-flight buffer ahead of
// the consumer. Readers without that lead (PG, vanilla MySQL) don't
// implement it.
//
// AdvanceDurableRows is the [CopyDurableProgressFunc] the writer calls;
// the data flows writer → reader, so the reader is the callback target,
// not a setter. flushedRows is the per-flush delta the sink adds to its
// running durable frontier.
type CopyDurableProgressSink interface {
	RowReader

	AdvanceDurableRows(flushedRows int64)
}

// ApplyExecTimeoutSetter is the optional surface a [ChangeApplier]
// can implement to accept a per-statement deadline for every
// tx.ExecContext on the apply path. The pipeline orchestrator
// threads [pipeline.Streamer.ApplyExecTimeout] to every applier
// that exposes this setter; engines that don't implement it inherit
// only the streamer's parent context (the pre-v0.52.0 behaviour).
//
// Zero or negative duration disables the per-exec timeout. Positive
// values are interpreted as a hard deadline per Exec; on expiry the
// driver's ctx-watcher closes the underlying connection and returns
// [context.DeadlineExceeded], which the applier's error classifier
// should treat as retriable so the runWithRetry loop activates and
// the next attempt acquires a fresh connection from the pool.
//
// Closes the GitHub issue #23 silent-stall failure mode (v0.52.0)
// where a half-closed destination connection blocked the apply
// goroutine indefinitely inside the driver's TLS read path.
type ApplyExecTimeoutSetter interface {
	SetExecTimeout(d time.Duration)
}

// RedactorSetter is the optional surface a [ChangeApplier] can
// implement to receive the operator-configured PII redaction
// policy. PII Phase 1.5 (roadmap item 15a follow-on, GitHub issue
// #24). The applier's Apply / ApplyBatch path consults the
// redactor before dispatching each change so PII columns get
// redacted on CDC events the same way Phase 1's bulk-copy path
// already redacts cold-start rows.
//
// The parameter type is `any` to avoid pulling the redact package
// into ir (would create a cycle: pipeline → redact → ir → redact).
// Implementations type-assert to *redact.Registry; a nil or empty
// registry is the no-op default (CDC events flow through
// unredacted, matching pre-Phase-1.5 behaviour).
type RedactorSetter interface {
	SetRedactor(registry any)
}

// StreamIDSetter is the optional surface a [ChangeApplier] can
// implement to receive the active stream's identifier. PII Phase 2.c
// (v0.59.0, GitHub issue #24): randomize:* strategies need
// streamID + table + column + PK values to derive a per-row
// replay-stable seed. The streamer calls SetStreamID after
// resolving the stream-id, before Apply / ApplyBatch begins.
//
// Engines that don't implement this interface inherit the empty-
// streamID behaviour: randomize:* still works (the seed remains
// stable per (table, column, PK) within the empty-streamID
// space), but operators wanting cross-stream determinism should
// pick an engine flavour that exposes the setter. MySQL and PG
// shipping engines both implement it.
type StreamIDSetter interface {
	SetStreamID(streamID string)
}

// RangeBoundsQuerier is the optional surface a [RowReader] can
// implement to expose MIN/MAX queries on a single PK column. Used by
// the parallel-bulk-copy phase (v0.5.0) to compute chunk boundaries
// before launching N parallel readers.
//
// minVal and maxVal are the raw driver-scanned values of MIN(col) and
// MAX(col) for the table, with NULL surfacing as a nil interface
// (empty table). The orchestrator coerces to int64 to compute equal
// slices; non-integer PKs are routed to the single-reader fallback
// before this method is called.
//
// Engines that don't implement this interface fall back to single-
// reader copy regardless of --bulk-parallelism. The shipping engines
// (MySQL, Postgres) both implement it.
type RangeBoundsQuerier interface {
	RangeBounds(ctx context.Context, table *Table, pkColumn string) (minVal, maxVal any, err error)
}

// RowCounter is the optional surface a [RowReader] can implement to
// expose a row-count estimate for ETA reporting in the bulk-copy
// progress lines (v0.5.0). The estimate may be exact (`SELECT
// COUNT(*)`) or approximate (`pg_class.reltuples`); engines should
// prefer fast estimates on huge tables since the count runs on a
// separate connection and feeds the ETA, not the data path.
//
// Returning (0, nil) means "no estimate available" — the orchestrator
// reports rate-only progress and omits ETA. Errors are non-fatal in
// the orchestrator; ETA is best-effort throughout.
type RowCounter interface {
	CountRows(ctx context.Context, table *Table) (int64, error)
}

// IdempotentRowWriter is an optional extension of [RowWriter] for the
// resume path. Indicates the writer's bulk INSERT path uses
// upsert-on-PK semantics (ON CONFLICT / ON DUPLICATE KEY UPDATE)
// instead of plain INSERT. Required for per-batch checkpointing — the
// brief replay window between batch commit and checkpoint write can
// re-deliver rows that already landed, and a plain INSERT would
// duplicate-key-error on those.
//
// The orchestrator calls WriteRowsIdempotent in resume mode; in
// non-resume (cold-start) mode it uses the faster plain WriteRows.
// Engines that don't implement this surface can still be used as
// targets — the orchestrator falls back to the v0.3.0 truncate-and-
// redo behaviour for in-progress tables. See ADR-0018.
type IdempotentRowWriter interface {
	RowWriter

	// WriteRowsIdempotent has the same shape as [RowWriter.WriteRows]
	// but generates upsert-form INSERT statements that tolerate PK
	// collisions (ON CONFLICT DO UPDATE / ON DUPLICATE KEY UPDATE).
	// Tables without a PK fall back to plain INSERT semantics — the
	// orchestrator never calls this method on no-PK tables (the
	// classify step routes those to truncate-and-redo) but
	// implementations should still degrade gracefully if it happens.
	WriteRowsIdempotent(ctx context.Context, table *Table, rows <-chan Row) error
}

// IdempotentCopyReader is the OPTIONAL surface a snapshot
// [RowReader] implements to declare that its COPY-phase row stream may
// re-deliver the same row, or deliver legitimate rows out of primary-
// key order, so the cold-start bulk-copy writer MUST be idempotent
// (upsert) rather than plain INSERT (Bug 125).
//
// The MySQL VStream snapshot reader implements it and returns true:
// Vitess's COPY mode re-emits rows already past the scan during binlog
// catchup, and can order the scan by a cheaper unique key than the
// table's PK — so a plain INSERT would collide on a unique key. The
// orchestrator type-asserts on this surface during cold-start and, when
// it reports true, routes the row stream through
// [IdempotentRowWriter.WriteRowsIdempotent] (and refuses a keyless
// table loudly, since the upsert needs a unique key to collide on).
//
// Readers that don't implement it (Postgres snapshot, MySQL binlog
// snapshot) keep the faster plain-INSERT / COPY cold-start path — their
// snapshot reads are gap-free and overlap-free by construction, so no
// idempotency is required.
type IdempotentCopyReader interface {
	RowReader

	// CopyNeedsIdempotentWriter reports whether the cold-start bulk
	// copy of this reader's rows must go through the idempotent writer.
	CopyNeedsIdempotentWriter() bool
}

// IdempotentCopyWriter is the writer-side capability the cold-start
// Bug-125 path requires before it will route a NO-PRIMARY-KEY table
// through [IdempotentRowWriter.WriteRowsIdempotent]. A writer implements
// it only when its idempotent path keys the upsert on a non-null UNIQUE
// index for PK-less tables (or refuses a truly keyless table loudly) —
// NEVER silently falling back to plain INSERT, which would duplicate
// Vitess's COPY catchup re-emissions now that the source-side dedup is
// gone.
//
// The MySQL target implements it. The Postgres target does not yet: its
// WriteRowsIdempotent plain-INSERTs no-PK tables, so a VStream→PG copy
// of a PK-less table is refused loudly (see the orchestrator's cold-
// start idempotent path) until PG gains the symmetric unique-key-upsert
// treatment. PK tables are unaffected on either engine — ON CONFLICT
// (pk) / ON DUPLICATE KEY UPDATE absorbs their re-emissions regardless.
type IdempotentCopyWriter interface {
	IdempotentRowWriter

	// HandlesNoPKIdempotentCopy reports whether WriteRowsIdempotent
	// upserts a no-PRIMARY-KEY table on a unique key (or refuses it
	// loudly) rather than plain-INSERTing it.
	HandlesNoPKIdempotentCopy() bool
}

// TableTruncator is the optional surface a [RowWriter] (or
// [SchemaWriter]) can implement to expose TRUNCATE TABLE for
// resume's truncate-and-redo path. The pipeline.Migrator type-asserts
// on this interface when re-entering an `in_progress` table during
// resume; engines that don't expose it fall back to "DELETE FROM"
// via the SchemaWriter or — last resort — refuse to resume the
// in-progress table cleanly.
//
// MySQL InnoDB and Postgres both support TRUNCATE TABLE. The
// per-engine wiring is on the [RowWriter] implementation so the
// orchestrator can find it without re-opening connections.
type TableTruncator interface {
	TruncateTable(ctx context.Context, table *Table) error
}

// TableDropper is the optional surface a [RowWriter] (or
// [SchemaWriter]) can implement to expose DROP TABLE for the
// `--reset-target-data` recovery path (ADR-0023). The pipeline
// type-asserts on this interface when the operator opts into the
// destructive-recovery flag; engines that don't expose it cause the
// flag to error clearly with "engine does not support
// --reset-target-data".
//
// Implementations must use IF EXISTS semantics so a partial-failure
// retry is idempotent. Postgres uses CASCADE to handle FK
// dependencies automatically; MySQL relies on InnoDB's referential
// cascade rules (the CASCADE keyword is invalid on MySQL DROP TABLE).
type TableDropper interface {
	DropTable(ctx context.Context, table *Table) error
}

// BulkTableDropper is the optional surface a [TableDropper] can layer
// on top of single-table DropTable to issue one DROP statement for a
// list of tables. The recovery flow on a database with hundreds of
// sluice-managed tables would otherwise pay a network round-trip per
// table; the bulk path collapses that to a single statement (PG and
// MySQL both accept comma-separated DROP TABLE).
//
// The pipeline's reset path probes for this surface and falls back to
// per-table [TableDropper.DropTable] when it's not implemented; an
// audit log line is emitted for each table either way.
//
// Implementations must use IF EXISTS semantics on every named table
// for the same idempotency reason as single-table DropTable.
type BulkTableDropper interface {
	DropTables(ctx context.Context, tables []*Table) error
}

// SchemaDeltaApplier is the optional surface a [SchemaWriter] can
// implement to apply a single ALTER TABLE delta against the target.
// Used by the Phase 3.2 chain-restore orchestrator to replay
// schema-evolution deltas captured on incremental manifests.
//
// Same-engine only: the source and target engine names must match.
// Cross-engine schema-delta translation is a Phase 5+ topic; the
// orchestrator refuses cross-engine chain restore upstream of this
// call.
//
// AddedColumns is the slice of columns that exist on the after-shape
// but not the before-shape. Implementations emit `ALTER TABLE
// <table> ADD COLUMN <col-def>` per entry (engines pick the dialect-
// specific column-def fragment via [ColumnDDLPreviewer]).
//
// Engines without an AlterAddColumn surface (none today; both PG and
// MySQL implement this) cause chain-restore to fall through with a
// clear log line and rely on the applier's column-list reconciliation
// — which works for some shapes but not all (PG strict-mode errors
// on unknown columns; MySQL's INSERT lists tolerate them via column
// list).
type SchemaDeltaApplier interface {
	// AlterAddColumn issues `ALTER TABLE <table> ADD COLUMN <col>`
	// for each column in cols, against the target. Idempotent on
	// columns that already exist (engine writers use IF NOT EXISTS
	// where the syntax allows; otherwise fall back to a probing
	// information_schema check before emit).
	AlterAddColumn(ctx context.Context, table *Table, cols []*Column) error
}

// ShapeDeltaApplier extends [SchemaDeltaApplier] with the additional
// per-shape delta methods required by ADR-0054 Shape A Phase 2's
// recognized-shape catalog (DP-E). Engines that implement live
// cross-shard DDL coordination must implement this interface; the
// lease-holder's apply path calls the matching method based on the
// pipeline's IR-delta classifier.
//
// Idempotency: each method is idempotent on the post-state (calling
// AlterDropColumn on a table whose column is already absent is a
// no-op; calling CreateShapeIndex on an index that already exists is
// a no-op). This lets the takeover-stream re-apply when the probe
// says NotApplied without worrying about partial-state — the engine's
// IF [NOT] EXISTS clauses handle the half-applied case.
//
// All methods are catalog-only DDL — they do NOT modify row data.
type ShapeDeltaApplier interface {
	SchemaDeltaApplier

	// AlterDropColumn issues `ALTER TABLE <table> DROP COLUMN <name>`
	// for each column in cols. Idempotent on columns that don't
	// exist (engines use IF EXISTS where supported).
	AlterDropColumn(ctx context.Context, table *Table, cols []*Column) error

	// CreateShapeIndex issues `CREATE INDEX <name> ON <table> (...)`
	// for each index in indexes. Idempotent on indexes that already
	// exist (engines use IF NOT EXISTS).
	CreateShapeIndex(ctx context.Context, table *Table, indexes []*Index) error

	// DropShapeIndex issues `DROP INDEX <name>` for each index in
	// indexes. Idempotent on indexes that don't exist (engines use
	// IF EXISTS).
	DropShapeIndex(ctx context.Context, table *Table, indexes []*Index) error

	// AlterColumnType issues `ALTER TABLE <table> ALTER COLUMN <name>
	// TYPE <new-type>` (PG) / `ALTER TABLE <table> MODIFY COLUMN
	// <name> <new-type>` (MySQL). want carries the post-DDL column
	// shape. Idempotent on columns already at the new type.
	AlterColumnType(ctx context.Context, table *Table, want *Column) error

	// AlterColumnNullability issues `ALTER TABLE <table> ALTER COLUMN
	// <name> SET NOT NULL` / `DROP NOT NULL` (PG) or `MODIFY COLUMN
	// <name> <type> [NOT] NULL` (MySQL). Idempotent on columns
	// already at the desired nullability.
	AlterColumnNullability(ctx context.Context, table *Table, want *Column) error

	// AlterRenameColumn issues `ALTER TABLE <table> RENAME COLUMN
	// <oldName> TO <newName>` (PG 9.2+, MySQL 8.0+ — both engines
	// preserve type, nullability, default, comment, identity, and
	// collation across a RENAME). Idempotent on the post-state: when
	// newName is already present (and oldName already absent), the
	// implementation no-ops; takeover-stream re-apply on a probe-
	// reported NotApplied is safe.
	//
	// v1 catalog: single-column rename only (one old → one new). The
	// upstream IR-delta classifier in pipeline.ClassifyShape refuses
	// multi-column rename loudly (added=N + dropped=N with N>1 is
	// ambiguous which old→new pair is which) — implementations need
	// only handle the single-rename shape.
	//
	// Inconsistent state recovery: when neither oldName nor newName
	// is present, the implementation returns a clear error; the
	// caller routes to the loud-failure path (drained model recovery
	// hint).
	AlterRenameColumn(ctx context.Context, table *Table, oldName, newName string) error

	// AlterAddCheck issues `ALTER TABLE <table> ADD CONSTRAINT
	// <name> CHECK (<expr>)` for each constraint in checks
	// (ADR-0065). Idempotent on the post-state via detect-then-emit
	// against the engine's CHECK catalog (pg_constraint /
	// information_schema.CHECK_CONSTRAINTS) — neither engine
	// reliably supports `ADD CONSTRAINT IF NOT EXISTS` in 8.0+ /
	// 16.x. Cross-dialect Expr values are routed through the
	// existing ADR-0016 / ADR-0045 translator at the writer
	// boundary; the implementation returns a refuse-loudly error
	// when the translation pass produces an expression containing
	// well-known untranslatable tokens (operator can opt in via
	// `--expr-override` per ADR-0016).
	AlterAddCheck(ctx context.Context, table *Table, checks []*CheckConstraint) error

	// AlterDropCheck issues `ALTER TABLE <table> DROP CONSTRAINT
	// [IF EXISTS] <name>` for each constraint in checks (ADR-0065).
	// Idempotent: PG supports `DROP CONSTRAINT IF EXISTS` natively;
	// MySQL implementations detect-then-DROP via
	// information_schema.CHECK_CONSTRAINTS.
	AlterDropCheck(ctx context.Context, table *Table, checks []*CheckConstraint) error

	// AlterModifyCheck issues DROP + ADD against the same target
	// for the modify-shape (ADR-0065). Neither engine supports an
	// in-place expression rewrite of an existing CHECK constraint
	// without dropping and re-adding; the v1 implementation emits
	// the DROP and ADD in sequence under the same applier method
	// so the takeover-stream's probe-and-record loop sees a single
	// logical step. The cross-dialect refuse-loudly pre-flight
	// described on AlterAddCheck applies to the ADD half here.
	AlterModifyCheck(ctx context.Context, table *Table, oldConstraint, newConstraint *CheckConstraint) error
}

// CDCSchemaSnapshotNormalizer is the optional engine surface for
// normalising a [Table] read by [SchemaReader] into the same shape
// the engine's [CDCReader] will later project from its wire protocol.
// ADR-0054 Bug 84 fix (v0.73.2).
//
// The motivation: live-coordination's [pipeline.ClassifyShape] compares
// two IR tables via `reflect.DeepEqual` on each column's IR Type. When
// the "pre" side comes from the cold-start [SchemaReader] (rich
// information_schema / pg_attribute view) and the "post" side comes
// from a CDC SchemaSnapshot (whatever the wire protocol carries), the
// SchemaReader's richer fields surface as a false `altered-col=true`
// on existing columns — which combines with a legitimate ADD COLUMN
// into a multi-shape combo refusal.
//
// Known PG asymmetries (the pgoutput RelationMessage carries only
// (name, OID, typmod, key-flag), so any IR field the SchemaReader
// populates from information_schema is missing on the CDC side):
//
//   - [Integer].AutoIncrement (SchemaReader sets true for IDENTITY /
//     SERIAL; pgoutput leaves false because the OID-to-type mapping
//     can't distinguish IDENTITY from a plain BIGINT).
//   - [Varchar].Collation, [Char].Collation, [Text].Collation
//     (SchemaReader reads pg_attribute.attcollation; pgoutput's
//     RelationMessage doesn't carry the collation OID).
//   - [Decimal].Unconstrained (SchemaReader sets true for bare
//     `numeric`; pgoutput emits typmod=-1 which the OID mapper
//     interprets as (0, 0) without flipping Unconstrained).
//
// MySQL does NOT implement this interface: the MySQL CDC reader's
// TableMapEvent decoder re-reads information_schema on schema-change
// boundaries so its projection already matches the SchemaReader's.
//
// Engines that don't implement the interface are a no-op fallback
// (the caller passes the table through unchanged). Implementations
// MUST return a NEW table struct (deep-enough copy that mutating the
// returned struct does not mutate the input); the caller treats the
// return value as the canonical cache entry from then on.
//
// Idempotent: NormalizeForCDCComparison(NormalizeForCDCComparison(t))
// equals NormalizeForCDCComparison(t).
type CDCSchemaSnapshotNormalizer interface {
	NormalizeForCDCComparison(t *Table) *Table
}

// SchemaTypeDropper is the optional surface a [RowWriter] (or
// [SchemaWriter]) can implement to drop user-defined database-level
// types created from the IR schema (e.g. Postgres `CREATE TYPE ...
// AS ENUM` types). Used by the `--reset-target-data` recovery path
// (ADR-0023) after the table drops complete: a partial cold-start
// can leave enum types behind that the next CREATE TYPE call would
// refuse with "type X already exists" (Bug 18).
//
// Engines whose enum representation doesn't outlive the table (MySQL
// embeds enum values inline on the column) don't need to implement
// this surface; the reset path no-ops when it isn't present.
//
// Implementations must use IF EXISTS / CASCADE semantics so the call
// stays idempotent across partial-failure retries. The schema passed
// in is the source-side IR so dropped types are scoped to ones sluice
// would have created — types belonging to other applications on a
// shared target database are untouched.
type SchemaTypeDropper interface {
	DropSchemaTypes(ctx context.Context, schema *Schema) error
}

// TableEmptyChecker is the optional surface a [RowWriter] (or
// [SchemaWriter]) can implement so the pipeline can detect a
// pre-existing populated dest table before starting a cold-start
// bulk-copy. Used by the cold-start pre-flight to refuse migrations
// that would otherwise INSERT into a non-empty target — typically a
// sign of a previously-killed cold-start run whose dest tables are
// still in place. See [pipeline] for the recovery flow.
//
// Implementations should treat a missing table as empty (return
// true, nil) so the pre-flight check doesn't double up with the
// schema-apply phase's CREATE TABLE IF NOT EXISTS.
//
// Engines that don't expose this surface are silently skipped by the
// pre-flight check — the pipeline keeps the v0.3.0 behaviour of
// trusting the operator that "cold-start" means "fresh tables".
type TableEmptyChecker interface {
	IsTableEmpty(ctx context.Context, table *Table) (bool, error)
}

// SnapshotImporter is the optional engine surface for importing a
// previously-exported snapshot onto N additional connections. Used by
// the parallel bulk-copy phase when N reader goroutines all need to
// see the same consistent source view.
//
// Postgres implements this via N `db.Conn(ctx)` acquires plus a
// `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION
// SNAPSHOT '<name>'` pair on each — the snapshot stays valid for as
// long as the exporting transaction (held open by the SnapshotStream)
// is alive. See ADR-0019 for the design.
//
// MySQL deliberately does not implement this interface: its REPEATABLE
// READ snapshot is per-session with no shareable name, so N parallel
// readers necessarily see N independent snapshots. The orchestrator
// falls back to opening N independent readers in that case; the
// transactional inconsistency window is bounded by the bulk-copy
// duration and is closed by the binlog catch-up in the snapshot+CDC
// handoff path. For non-CDC migrations (`sluice migrate`), the same
// per-connection-snapshot inconsistency applies — operators running
// against live MySQL sources should expect the small window or quiesce
// the source. Documented as a known engine difference rather than a
// regression.
type SnapshotImporter interface {
	// ImportSnapshot returns n RowReaders, each pinned to a separate
	// connection that has imported the named snapshot. Used by the
	// parallel bulk-copy phase; the caller closes each reader in turn
	// (or all together via a parent ctx cancel).
	//
	// The implementation owns the connection lifecycle for each
	// returned reader; callers should call Close on each to release
	// the pinned connection and roll back the snapshot tx. The
	// snapshotName must reference an exported snapshot whose owning
	// transaction is still open — typically the snapshot captured by
	// [Engine.OpenSnapshotStream].
	ImportSnapshot(ctx context.Context, snapshotName string, n int) ([]RowReader, error)
}

// SnapshotImporterOpener is the optional engine interface that exposes
// snapshot import. Engines without sharable snapshots (MySQL today) can
// omit the method; the orchestrator type-asserts at the call site and
// falls back to opening N independent readers.
type SnapshotImporterOpener interface {
	OpenSnapshotImporter(ctx context.Context, dsn string) (SnapshotImporter, error)
}

// CDCReader streams [Change] events from a source database starting at
// the given Position. Engines whose [Capabilities.CDC] is [CDCNone]
// return a non-nil error for any call to this interface.
type CDCReader interface {
	StreamChanges(ctx context.Context, from Position) (<-chan Change, error)
}

// PositionFromManifestPreflight is the optional engine-side surface
// for the Phase 3.3.C pre-flight checks fired before
// `sluice sync start --position-from-manifest` opens CDC. PG
// implements it on the engine's [SchemaReader] (parallel to
// [HealthReporter] / [BackupPositionCapturer]); engines without
// operator-attention surfaces simply omit the method.
//
// The contract: implementations inspect the source's slot/WAL state
// against the supplied chainTerminal position and the slotName the
// CDC reader will use, and return a [PreflightReport] capturing soft
// warnings + an optional refusal. The streamer surfaces refusals as
// run-aborting errors; warnings turn into refusals when
// `--strict-preflight` is set.
//
// Lives in the ir package (not pipeline) so engine packages can
// reference it without forming an import cycle through pipeline's
// integration tests.
type PositionFromManifestPreflight interface {
	PreflightPositionFromManifest(
		ctx context.Context,
		chainTerminal Position,
		slotName string,
	) (PreflightReport, error)
}

// PreflightReport bundles the result of a Phase 3.3.C pre-flight
// against the source. Warnings are operator-actionable advisories
// that don't block the run by default; Refusal is a fatal condition
// the operator must address before the run can proceed (slot lost,
// slot missing, WAL gap exceeds keep-size).
type PreflightReport struct {
	// Warnings is the slice of soft-warning messages emitted by the
	// preflight. Each is a single-sentence operator-facing string;
	// the streamer logs them via slog.WarnContext and (when
	// StrictPreflight is true) escalates to a refusal.
	Warnings []string

	// Refusal is non-empty when the preflight encountered a fatal
	// condition. The streamer surfaces it as a wrapped run error.
	// Empty means "no refusal" — warnings only.
	Refusal string
}

// BackupPositionCapturer is the optional engine surface for capturing
// the source's current CDC position from a one-shot query. Used by
// the full-backup orchestrator as a v0.18.0 fallback when the engine
// does NOT implement [BackupSnapshotOpener] — the snapshot-anchored
// path is the preferred shape because it closes the during-backup
// write-window gap that this fallback path leaves open.
//
// The captured position is the source-side cursor at the moment of
// capture. With the v0.17.x fallback shape, the full backup calls
// this at the end of the per-table row sweep so the recorded
// EndPosition reflects "the source has produced everything up to
// here at the moment the backup completes." Writes that landed on
// already-read tables during the backup window are read by neither
// the row sweep (no shared snapshot) nor the first incremental's
// `--since=<full>.EndPosition` window (those LSNs are before the
// captured EndPosition) — the v0.17.2 release notes called this out
// as a known caveat with the workaround "pair backups with
// continuous `sluice sync start`."
//
// In v0.18.0 the gap is closed via [BackupSnapshotOpener]: engines
// that implement it capture EndPosition at snapshot START (the
// source position at which a cross-table consistent read view is
// pinned) and the orchestrator never calls CaptureBackupPosition.
// Engines that DON'T implement BackupSnapshotOpener fall through to
// this surface with a WARN log line so operators know the chain
// rooted in this full will carry the v0.17.x during-backup write-
// window gap.
//
// Engines wire this on their [SchemaReader] (parallel to
// [HealthReporter]) so the full-backup orchestrator can type-assert
// on a value it already opens. The captured position's encoding is
// engine-specific:
//
//   - Postgres: a JSON-envelope `{slot,lsn}` shape using the slot
//     name supplied via slotName (or the engine's default when empty)
//     plus `pg_current_wal_lsn()`. The slot need not exist at capture
//     time — Phase 3.3's `--position-from-manifest` pre-flights the
//     slot state before resuming CDC from the recorded LSN.
//   - MySQL: a binlog-mode position recording `@@global.gtid_executed`
//     when GTID mode is on, or a `(file, pos)` pair otherwise.
//
// Engines without CDC support don't implement this surface; the
// orchestrator type-asserts and falls back to "no EndPosition recorded"
// (matches the v0.16.x shape; first incremental against such a manifest
// surfaces a clear "parent has no EndPosition; chain will start from
// CDC's current position" warning).
type BackupPositionCapturer interface {
	// CaptureBackupPosition returns the source's current CDC position.
	// The slotName argument is honoured by engines with a slot concept
	// (Postgres) and ignored by others (MySQL); empty falls back to the
	// engine's default. The returned position is suitable for storage
	// in [Manifest.EndPosition] and as a [Position] argument to the
	// engine's CDC reader.
	CaptureBackupPosition(ctx context.Context, slotName string) (Position, error)
}

// ChangeApplier applies [Change] events to a target database and
// persists progress alongside each applied change. Each Apply call
// commits the data write and the position update in the same target
// transaction (per ADR-0007), so progress and applied data can never
// diverge.
//
// Lifecycle: callers (typically pipeline.Streamer) first invoke
// EnsureControlTable once at startup to create the per-target state
// table, then ReadPosition to detect a previous run, then Apply with
// the resolved streamID.
type ChangeApplier interface {
	// EnsureControlTable creates the per-target sluice_cdc_state
	// table if it doesn't exist. Idempotent; safe to call on every
	// start.
	EnsureControlTable(ctx context.Context) error

	// ReadPosition returns the last persisted source position for
	// streamID, or ok=false when no row exists. ok=false signals
	// "first run" (cold start); ok=true signals "resume from this
	// position" (warm resume).
	ReadPosition(ctx context.Context, streamID string) (Position, bool, error)

	// ListStreams returns one [StreamStatus] per row in the
	// per-target control table. Used by `sluice sync status` for
	// operational visibility — operators want to see every stream
	// the target has ever been the destination for, not just one
	// specific ID. Order is unspecified; the CLI sorts.
	//
	// Returns an empty slice (not nil) when no streams have been
	// recorded yet. EnsureControlTable doesn't have to have been
	// called — the implementation should be tolerant of the table
	// being absent, treating that case as "no streams".
	ListStreams(ctx context.Context) ([]StreamStatus, error)

	// Apply consumes Change events from the channel and applies each
	// to the target. The position write happens inside the same
	// transaction as the data write — atomicity guarantees that
	// progress and data move together.
	Apply(ctx context.Context, streamID string, changes <-chan Change) error

	// RequestStop sets the stop flag on the named stream's row in the
	// per-target control table. A running [pipeline.Streamer] polls
	// this flag and, when it transitions to "set", finishes the
	// in-flight change, persists its final position, and exits
	// cleanly with a nil error. See `sluice sync stop` for the
	// operator-facing entry point.
	//
	// Idempotent: multiple calls land the same flag (the timestamp
	// gets bumped, but the running streamer treats any non-NULL
	// value as "stop requested" so a repeated request is harmless).
	//
	// When the stream row does not exist on the target, returns an
	// engine-specific sentinel error the caller can branch on (see
	// the `--if-exists`-style shape used by [SlotManager.Drop]).
	// This is not a fatal condition; users typo stream IDs.
	RequestStop(ctx context.Context, streamID string) error

	// ClearStopRequested resets the stop flag for the named stream.
	// Called by [pipeline.Streamer] in two places:
	//
	//   1. At startup, so a previous `sluice sync stop` doesn't
	//      leave a sticky signal that immediately exits the next
	//      `sluice sync start`.
	//   2. After a graceful drain triggered by an observed stop
	//      flag, so a CLI `sluice sync stop --wait` polling for
	//      completion sees the cleared flag and returns success.
	//
	// Idempotent and tolerant of a missing row (returns nil).
	//
	// Why clear at startup rather than on consumption: the polling
	// goroutine doesn't share a transaction with the applier's data
	// writes, so a clear-on-read could lose the signal if the data
	// write rolls back after seeing the flag. Clearing at startup
	// keeps the streamer's lifecycle as the flag's lifecycle. The
	// post-drain clear is a separate concern — it signals "drain
	// completed cleanly", not "flag consumed", and runs only after
	// the apply loop has fully returned.
	ClearStopRequested(ctx context.Context, streamID string) error
}

// BatchedChangeApplier is an optional extension of [ChangeApplier]
// for engines that can apply N changes in a single target
// transaction. The Streamer probes for this via type assertion;
// engines that don't implement it fall back to per-change
// [ChangeApplier.Apply].
//
// Idempotency is preserved per ADR-0010: replay of any prefix of
// the change stream still produces the same final state via the
// existing ON CONFLICT / ON DUPLICATE KEY UPDATE semantics on
// Insert and the zero-rows-affected tolerance on Update / Delete.
// Position-and-data atomicity is preserved per ADR-0007: the
// position of the last applied change in a batch is written in
// the same target transaction as the batch's data writes, so a
// crash mid-batch rolls back both.
//
// See ADR-0017 for the batched-commit design rationale.
type BatchedChangeApplier interface {
	ChangeApplier

	// ApplyBatch consumes Change events from the channel and applies
	// them in batches of up to maxBatchSize per target transaction.
	// The position write of the last applied change in each batch
	// happens inside the same transaction as the data writes.
	//
	// A batch flushes early when the channel closes (clean
	// shutdown), when ctx is cancelled, when a target write fails,
	// or when a [Truncate] event is encountered (schema-changing
	// events apply alone so any column-type cache invalidation is
	// scoped to that change).
	//
	// maxBatchSize <= 1 falls back to per-change semantics
	// (equivalent to [ChangeApplier.Apply]).
	ApplyBatch(ctx context.Context, streamID string, changes <-chan Change, maxBatchSize int) error
}

// StreamCleaner is the optional surface a [ChangeApplier] can
// implement to delete a stream's bookkeeping row from the per-target
// `sluice_cdc_state` table. Used by the `--reset-target-data`
// recovery path (ADR-0023): the pipeline clears the row before
// dropping dest tables so the next cold-start sees a fresh control
// table alongside an empty schema.
//
// Idempotent and tolerant of a missing row (returns nil) — re-running
// the reset on a target whose row was already cleared is not an
// error. Tolerant of the control table being absent for the same
// reason ReadPosition is.
//
// Engines that don't expose this surface cause `--reset-target-data`
// to error clearly with "engine does not support --reset-target-data
// for streams".
type StreamCleaner interface {
	ClearStream(ctx context.Context, streamID string) error
}

// PositionWriter is the optional surface a [ChangeApplier] can
// implement to write a position row WITHOUT a paired data write.
// Phase 4.5 (`sluice sync from-backup`) needs it for two reasons:
//
//  1. Cold-start --at-chain-id and --reset-target-data record an
//     initial broker position before any incremental has been
//     replayed; there's no Change stream to ride along with.
//  2. Schema-delta-only incrementals (no change chunks) still need
//     the broker's position to advance so the next tick doesn't
//     re-replay the schema delta on every poll.
//
// The implementation MUST upsert the same `(stream_id,
// source_position, updated_at)` row the apply path writes, with the
// same idempotency contract (re-calling for the same streamID lands
// the same row). It MAY use a single-statement transaction; it
// SHOULD NOT touch the data tables.
//
// EnsureControlTable must have been called first.
type PositionWriter interface {
	WritePosition(ctx context.Context, streamID string, pos Position) error
}

// SchemaHistoryCompactor is the optional surface a [ChangeApplier] can
// implement to delete ADR-0049 schema-history rows whose anchor_position
// is STRICTLY OLDER than the supplied retention floor. ADR-0049 Chunk D,
// DP-2: the floor is the combined `min(live ADR-0007 safe-point, oldest
// retained backup-chain resume position)` — the caller computes that
// combined floor (e.g. via [pipeline.SchemaHistoryRetentionFloor]) and
// passes it in.
//
// Strict-older semantics: rows AT the floor and AFTER the floor remain
// (locked design "leaves the at-floor and after-floor rows intact").
// The loud-floor sentinel is preserved by construction: a
// [ResolveSchemaVersion] for a position BELOW the oldest retained anchor
// wraps [ErrPositionInvalid] → ADR-0022 cold-start; compaction can only
// MAKE a resume fall below the floor, which is by construction loud,
// never silent.
//
// Engines without the surface can be ignored by the caller (the live
// resume path still works without compaction; growth is ∝ DDL count by
// the Chunk-B true-delta gate, so retention is a maintenance operation,
// not a runtime correctness requirement).
type SchemaHistoryCompactor interface {
	// CompactSchemaHistoryBelow deletes rows strictly older than floor
	// under the engine's [PositionOrderer]. Returns the count of rows
	// deleted (operator-facing diagnostic).
	CompactSchemaHistoryBelow(ctx context.Context, floor Position) (int, error)
}

// StreamStatus is the operational snapshot of one row in the
// per-target sluice_cdc_state control table. Returned by
// [ChangeApplier.ListStreams] for the `sluice sync status` command.
//
// UpdatedAt is the wall-clock instant the row last changed (i.e.,
// the most recent Apply commit). Operators use it to detect stuck
// streams: a stream that hasn't ticked in N minutes when the source
// is generating change traffic is the operator's problem to chase.
type StreamStatus struct {
	StreamID  string
	Position  Position
	UpdatedAt time.Time

	// SlotName is the replication-slot name the active stream is
	// consuming from on engines with a slot concept (Postgres).
	// Populated by the applier on each position-write so a later
	// `sluice schema add-table --no-drain` knows which slot's
	// confirmed_flush_lsn to read for the live-add LSN-floor check.
	// Empty for engines without slots (MySQL: binlog stream is the
	// slot; no slot_name to record) and for legacy rows that
	// pre-date the column (best-effort fallback to the engine
	// default `sluice_slot` is the caller's responsibility).
	SlotName string

	// SourceDSNFingerprint is the truncated SHA-256 hex of the
	// stream's source DSN host+port+database tuple, recorded by the
	// streamer on `sync start` (ADR-0031). Used for stream-id
	// collision detection: if the same stream-id is reused with a
	// different source DSN, the streamer refuses loudly. Empty for
	// engines that don't implement the fingerprint surface and for
	// legacy rows that pre-date the column (treated as "unknown —
	// allow" by the collision check).
	SourceDSNFingerprint string

	// TargetSchema is the operator-supplied `--target-schema NAME`
	// recorded on the per-target sluice_cdc_state row at every
	// position-write (ADR-0031, Bug 46). Powers the
	// `sluice schema add-table` resolve-and-refuse path: if the
	// operator passes `--target-schema` to add-table and it differs
	// from the recorded value, sluice refuses loudly rather than
	// landing the new table in a different namespace from where
	// CDC events are routed (the v0.25.0 silent-event-drop failure
	// mode). Empty for engines that don't implement the recorder
	// surface, legacy rows that pre-date the column, and streams
	// started without `--target-schema` (treated as "use the DSN
	// default schema").
	TargetSchema string
}

// SchemaSetter is the optional surface a [SchemaReader], [SchemaWriter],
// [RowReader], [RowWriter], or [ChangeApplier] can implement to accept
// an operator-supplied schema-name override (`--target-schema NAME`,
// ADR-0031). The pipeline orchestrator type-asserts on every Open*
// return when a non-empty target-schema is set; engines that don't
// implement the setter are expected to either expose [Capabilities].
// SchemaScope = [SchemaScopeFlat] (so the orchestrator refuses the flag
// upstream with a clear engine-not-supported error) or to no-op the
// override.
//
// Engines whose schema concept is taken from the DSN (Postgres' `schema`
// query parameter) implement this so the orchestrator can override
// without DSN rewriting. The override applies to subsequent reads and
// writes — implementations must not buffer schema-derived state before
// the Open* return.
//
// PG implements; MySQL does not (no schema-vs-database distinction; the
// orchestrator refuses `--target-schema` against MySQL targets).
type SchemaSetter interface {
	SetSchema(name string)
}

// IndexBuildTuner is the optional surface a [SchemaWriter] can implement
// to accept the operator's `--index-build-mem` value (a per-build
// maintenance_work_mem in bytes; 0 = auto). The pipeline orchestrator
// threads it to the writer before [SchemaWriter.CreateIndexes] so the
// dedicated index-build session can raise maintenance_work_mem above the
// provider's steady-state default — the dominant lever for the deferred
// secondary-index build, which runs against an idle target.
//
// PG implements; MySQL does not (its index-build tuning is a different
// contract — ALGORITHM=INPLACE / innodb_sort_buffer_size — out of scope
// for v1). Engines that don't implement it are silently passed through:
// the auto path is the default and the flag simply has no target.
//
// Zero or negative bytes is the auto sentinel — the writer derives
// maintenance_work_mem from a pg_settings probe instead of the override.
//
// SetIndexBuildParallelism accepts the operator's
// `--index-build-parallelism` value (the number of concurrent index
// builds; 0 = auto). Phase B: the PG writer builds the deferred
// secondary indexes with a bounded concurrent worker pool, each worker
// on its own connection with its own (divided) maintenance_work_mem. The
// auto count is bounded by the target's spare connection budget AND a
// memory budget (total build memory ≈ N × per-build mem — the memory ×
// concurrency trap), so it can't OOM a small node. Zero or negative is
// the auto sentinel.
//
// See docs/dev/notes/index-build-phase-tuning.md.
type IndexBuildTuner interface {
	SetIndexBuildMem(bytes int64)
	SetIndexBuildParallelism(n int)
}

// TableScoper is the optional surface a [SchemaReader] can implement
// to accept the operator's table filter *before* the schema scan, so
// per-column type validation is scoped to the tables that will
// actually be migrated (catalog Bug 76).
//
// Without this, a reader validates every column of every table in the
// database and aborts the whole run when an unsupported column type
// lives in a table the operator already excluded via
// `--include-table` / `--exclude-table` — the error even names the
// unrelated table, which is confusing. The pipeline still applies the
// authoritative post-read [TableFilter] regardless; this push-down is
// purely about not validating (and not failing on) out-of-scope
// tables.
//
// allow reports whether a given table name participates in the
// migration; it is the engine-neutral projection of the pipeline's
// TableFilter (path.Match semantics, engine defaults already merged).
// Implementations must treat a nil predicate as "no filtering" and
// apply allow only to user tables (engine bookkeeping tables are
// excluded by the reader independently). The predicate must be set
// before [SchemaReader.ReadSchema]; implementations must not buffer a
// table list before that call.
//
// PG implements; MySQL does not yet (the post-read filter still
// applies there — no silent loss, only the Bug-76 usability gap
// remains until MySQL grows the same push-down).
type TableScoper interface {
	SetTableScope(allow func(tableName string) bool)
}

// DatabaseLister is the OPTIONAL engine surface for enumerating the
// databases a server connection can see (ADR-0074, multi-database
// MySQL migration). The orchestrator calls it to resolve
// `--all-databases` / `--include-database` / `--exclude-database` into
// a concrete database list before iterating the per-database snapshot.
//
// Only engines whose namespacing primitive is a *database* on a shared
// server (MySQL: one binlog/server, N databases) implement it. MySQL
// lists `information_schema.schemata` minus the always-excluded system
// set (`information_schema`, `performance_schema`, `mysql`, `sys`).
// Postgres does NOT implement it in this phase: a PG "database" is a
// connection boundary, not a same-server namespace the orchestrator
// fans out across — PG multi-schema fan-out is the symmetric follow-on
// tracked in ADR-0074, not built here.
//
// The pipeline type-asserts on the source engine when any
// database-scope flag is set; an engine that doesn't implement the
// surface is refused loudly (the operator picked a multi-database run
// against a source that can't enumerate databases). Implementations
// open and close their own connection from the (database-optional)
// server DSN — the orchestrator hands the same source DSN it migrates
// from, whose database component may be empty in multi-database mode.
type DatabaseLister interface {
	// ListDatabases returns the non-system databases visible on the
	// server the dsn points at, in unspecified order. The orchestrator
	// applies the operator's include/exclude globs to this list. The
	// always-excluded system databases must never appear in the result.
	ListDatabases(ctx context.Context, dsn string) ([]string, error)
}

// DatabaseDSNDeriver is the OPTIONAL engine surface for deriving a
// single-database DSN from a (possibly database-less) server DSN
// (ADR-0074). The multi-database orchestrator uses it to re-open a
// single-database reader/writer per selected database without the
// pipeline having to know each engine's DSN syntax (the IR-first /
// engine-neutral-orchestrator tenets).
//
// MySQL implements it: WithDatabase swaps the DSN's DBName component;
// EnsureDatabase issues `CREATE DATABASE IF NOT EXISTS` against the
// server (the MySQL → MySQL auto-create-target-database behaviour
// resolved in ADR-0074). The PG engine does NOT implement it — PG
// multi-database fan-out is the symmetric follow-on, and a PG *target*
// in a MySQL-source fan-out routes via `--target-schema` (same DSN,
// per-database schema), not a DSN rewrite.
type DatabaseDSNDeriver interface {
	// WithDatabase returns a clone of dsn whose database component is
	// set to database. Used to re-open a single-database reader/writer
	// for one database of a multi-database run. Returns an error for a
	// malformed dsn.
	WithDatabase(dsn, database string) (string, error)

	// EnsureDatabase creates database on the server dsn points at if it
	// does not already exist (`CREATE DATABASE IF NOT EXISTS` semantics
	// — idempotent). Called for a MySQL → MySQL multi-database target
	// before the per-database writer opens. dsn may be a server DSN
	// (no database component) or name any database on the same server;
	// the implementation connects at the server level to run the DDL.
	EnsureDatabase(ctx context.Context, dsn, database string) error
}

// MultiDatabaseScoper is the OPTIONAL surface a [SchemaReader] implements
// to be told it is reading ONE database of a multi-database fan-out run
// (ADR-0074). It carries two pieces of state the reader needs that the
// single-database path leaves at their zero value:
//
//  1. The source database name, which the reader stamps onto every
//     [Table.Schema] / [View.Schema] it emits, so the orchestrator can
//     route each database to its same-named target namespace (PG schema
//     / MySQL database). In single-database mode these stay empty
//     (byte-identical back-compat).
//
//  2. An `inScope` predicate over database names, used for the
//     flat-scope foreign-key carve-out. MySQL's flat scope normally
//     drops a FK's referenced database to keep [ForeignKey.ReferencedSchema]
//     empty; in multi-database mode the reader instead POPULATES
//     ReferencedSchema with the referenced database — and refuses
//     LOUDLY at read time when a FK references a database OUTSIDE the
//     selected set (sluice can't guarantee the referent exists on the
//     target). `inScope` reports whether a referenced database name is
//     part of the migrated set; a nil predicate means single-database
//     mode (the carve-out stays disabled).
//
// Set before [SchemaReader.ReadSchema]; implementations must not buffer
// schema state before that call. PG does not implement it (this phase is
// MySQL-source fan-out); engines without the surface keep the flat,
// single-database behaviour.
type MultiDatabaseScoper interface {
	SetMultiDatabaseScope(database string, inScope func(database string) bool)
}

// CDCDatabaseScoper is the OPTIONAL surface a [CDCReader] implements to
// be told it is streaming a multi-database fan-out run (ADR-0074 Phase
// 1b). The MySQL binlog is server-wide — a single replication
// connection already carries every database's changes, each event
// tagged with its source database — so multi-database CDC is ONE
// stream with a wider event-allow set, not N streams.
//
// `inScope` reports whether an event's source database is part of the
// selected set. When set (non-nil):
//
//   - the reader emits row/truncate events from EVERY database the
//     predicate admits (not just the DSN's one database), and drops
//     events from databases outside the set — the SAME per-event drop
//     the single-database path uses, just with a wider allow set;
//   - each emitted [Change.Schema] carries the event's SOURCE database,
//     read from the binlog event's own metadata (the TABLE_MAP_EVENT /
//     QUERY-event schema field), so the [ChangeApplier]'s multi-database
//     routing can land it in the correct target namespace.
//
// A nil predicate (the single-database default) keeps today's behaviour
// EXACTLY: only the reader's DSN-bound database is emitted, everything
// else dropped — byte-identical back-compat. Set before
// [CDCReader.StreamChanges]; the predicate is consulted on the reader's
// single pump goroutine.
//
// PG does not implement it (this phase is MySQL-source fan-out); engines
// without the surface keep the single-database stream.
type CDCDatabaseScoper interface {
	SetCDCDatabaseScope(inScope func(database string) bool)
}

// MultiDatabaseRouter is the OPTIONAL surface a [ChangeApplier]
// implements to enable per-change target-namespace routing for a
// multi-database fan-out CDC run (ADR-0074 Phase 1b). The applier stays
// ONE instance with ONE position write per batch (the binlog position
// is server-wide per stream-id); routing is purely a per-change choice
// of which namespace a row write targets.
//
// When routing is ENABLED, the applier qualifies an
// Insert/Update/Delete/Truncate table reference with the change's
// [Change.Schema] (MySQL target → `db`.`table`; PG target →
// `schema.table`) ONLY when that source namespace is non-empty AND
// differs from the applier's own bound/default namespace — exactly the
// cross-namespace case, mirroring the Phase-1a foreign-key qualifier
// (qualify across DIFFERING namespaces only). A change whose Schema is
// empty OR equals the applier's bound namespace emits the SAME
// unqualified/bound SQL the single-database path emits.
//
// When routing is DISABLED (the default — every single-database run,
// for ALL engine pairs), the applier ignores [Change.Schema] for table
// qualification and always writes into its bound namespace, BYTE-
// IDENTICAL to the pre-ADR-0074 behaviour. This is the load-bearing
// back-compat guard: cross-engine single-database CDC (e.g. a PG source
// whose reader already populates Change.Schema with the source schema,
// streamed into a MySQL target with a differing bound database) must
// NOT start qualifying — the differing-namespace condition alone is
// insufficient, so routing is an explicit opt-in the multi-database
// streamer sets, never inferred from Change.Schema.
//
// The applier does NOT create the target namespace — the cold-start /
// snapshot phase owns namespace creation (ADR-0074 Phase 1b.2); the
// applier assumes the routed namespace already exists.
type MultiDatabaseRouter interface {
	SetMultiDatabaseRouting(enabled bool)
}

// MultiDatabaseSnapshotOpener is the OPTIONAL engine surface for opening
// the SINGLE consistent snapshot spanning N databases that a
// multi-database `sync start` cold-start needs (ADR-0074 Phase 1b.2).
//
// The crux of multi-database CDC is consistency: the server-wide binlog
// position handed to the CDC reader MUST be captured at ONE consistent
// cut across ALL selected databases, or the snapshot → CDC handoff loses
// or double-applies rows. Phase 1a's `migrate` re-opened a single-database
// snapshot per database (N independent positions); that is acceptable for
// a one-time copy but WRONG for the CDC handoff. This surface fixes it:
// the engine opens ONE snapshot transaction (`START TRANSACTION WITH
// CONSISTENT SNAPSHOT` on MySQL) and captures ONE binlog position inside
// it, then returns a [SnapshotStream] whose:
//
//   - Rows reads every selected database's tables FROM THAT ONE snapshot
//     transaction. The reader qualifies each SELECT by the table's
//     [Table.Schema] (the source database), so a single pinned connection
//     reads across all N databases at the same REPEATABLE-READ view. The
//     orchestrator stamps [Table.Schema] via [MultiDatabaseScoper] before
//     bulk-copy, so the reader knows which database each table lives in.
//   - Changes is the SERVER-WIDE CDC reader (the binlog is server-wide),
//     ready to be scoped to the selected database set via
//     [CDCDatabaseScoper.SetCDCDatabaseScope] and streamed from Position.
//   - Position is the single binlog coordinate captured inside the
//     spanning snapshot transaction — the gapless, idempotent handoff
//     point for every selected database's CDC.
//
// databases is the concrete, already-resolved selected database set (the
// orchestrator has applied the include/exclude globs and excluded the
// system databases). It must be non-empty. The dsn is a *server*
// connection — its database component may be empty (the operator drove a
// multi-database run without naming one).
//
// Only engines whose CDC stream is server-wide (MySQL binlog) implement
// it. The PlanetScale/Vitess VStream flavor is keyspace-scoped, so a
// spanning multi-keyspace snapshot is a distinct N-stream design
// (ADR-0074 §6 / Phase 1c) and is NOT implemented here — the engine
// returns [ErrNotImplemented]-shaped errors for the VStream flavors.
type MultiDatabaseSnapshotOpener interface {
	OpenMultiDatabaseSnapshotStream(ctx context.Context, dsn string, databases []string) (*SnapshotStream, error)
}

// ExtensionAware is the optional engine-side surface for engines that
// can pass through column types defined by extensions (ADR-0032). PG
// implements; MySQL does not (no extension concept in the same shape).
// The pipeline orchestrator type-asserts on every freshly-opened
// reader / writer and threads the operator-supplied
// `--enable-pg-extension` allowlist through; engines that don't
// implement default to "no extensions enabled" — the existing
// behaviour where unrecognised extension types surface as loud
// refusals.
//
// Implementations are responsible for:
//
//   - Validating each name against their own catalog of recognised
//     extensions (refusing unknown names with an operator-friendly
//     error listing the recognised set).
//   - Preflighting extension presence against the connected database
//     (`SELECT 1 FROM pg_extension WHERE extname = $1` on PG) so a
//     misspelled flag or wrong DSN surfaces loudly before any data
//     moves.
//
// Both checks fire at construction time, not mid-run.
type ExtensionAware interface {
	EnableExtensions(ctx context.Context, extensions []string) error
}

// VerbatimExtensionAware is the optional engine-side surface for
// engines that can carry an UNcatalogued extension column type
// verbatim (ADR-0047), as the passthrough tier *below* the ADR-0032
// catalog. PG implements; MySQL does not (no extension concept).
//
// The orchestrator enables the verbatim tier ONLY when it can prove
// the run does not need semantic type understanding:
//
//   - live PG → PG (source engine name == target engine name ==
//     "postgres"), or
//   - a PG backup (restore-target unknown at backup time → the
//     verbatim-typed columns are recorded on the lineage segment and
//     a loud restore-time engine gate refuses a non-PG target).
//
// Engines that don't implement the surface skip cleanly — the
// existing loud refusal for uncatalogued USER-DEFINED types is
// preserved (ADR-0047 determination tier (c)). The catalogued seven
// extensions are unaffected: they keep taking the rich ADR-0032 path
// regardless of this flag (the engine consults its catalog first).
//
// SetVerbatimExtensionPassthrough is idempotent and is called at
// reader/writer construction time, before any schema read or write.
type VerbatimExtensionAware interface {
	SetVerbatimExtensionPassthrough(enabled bool)
}

// CrossEngineExtensionTranslator is the optional engine-side surface
// for engines whose extensions have defensible default cross-engine
// translators (ADR-0032 § "Cross-engine policy"). The pipeline's
// engine-name gate in `validateEnabledPGExtensions` consults this
// to decide whether `--enable-pg-extension EXT` may be passed
// against a non-PG target: an extension with a default translator
// is allowed (the translator rewrites the column type at write-time
// or via [translate.RetargetForEngine]); one without falls back to
// the loud-failure default.
//
// Today's PG implementation declares `hstore` and `citext` as
// cross-engine-translatable (Tier 1 type-only with lossless MySQL
// mappings). `vector` / `pg_trgm` / `postgis` are not declared:
// their cross-engine semantics are ambiguous or absent and
// operators must supply `--type-override` explicitly.
//
// Engines implement this when at least one of their declared
// extensions has a built-in cross-engine translator. Engines without
// extensions skip the interface entirely; the pipeline treats the
// missing implementation as "no defaults — preserve the strict
// target-must-be-same-engine gate."
type CrossEngineExtensionTranslator interface {
	// HasCrossEngineDefaultTranslator reports whether the named
	// extension has a default cross-engine translator on this
	// engine. The name is the canonical pg_extension.extname value
	// (or whatever the engine's catalog keys on).
	HasCrossEngineDefaultTranslator(name string) bool
}

// SourceFingerprintRecorder is the optional surface a [ChangeApplier]
// can implement to accept the source-DSN fingerprint the streamer
// computes at startup. The fingerprint flows through to the per-target
// `sluice_cdc_state` row's `source_dsn_fingerprint` column on every
// position-write so a later `sync start` can detect a stream-id reused
// with a different source (ADR-0031).
//
// Idempotent — the streamer may call this on every Run; the value
// stays current across warm resumes. Empty input is allowed (no-op
// set) and reflects the legacy / engine-not-supported case where no
// fingerprint is recorded.
//
// PG implements; engines without fingerprint storage simply omit the
// method (the orchestrator's collision check then no-ops for that
// engine).
type SourceFingerprintRecorder interface {
	SetSourceDSNFingerprint(fingerprint string)
}

// SlotInfo describes one row of an engine's logical-replication slot
// inventory. The shape is engine-neutral but the underlying concept is
// Postgres-specific today (logical replication slots). Future engines
// with similar resume-state primitives — e.g. Vitess named tablet
// vstream cursors — could surface the same struct.
//
// WALStatus is the verbatim wal_status string from
// pg_replication_slots: "reserved", "extended", "unreserved", "lost",
// or empty (older PG releases). Operators glance at this column to
// spot slots about to be invalidated. ConfirmedFlushLSN is the slot's
// last-acknowledged consume position, useful for spotting stalled or
// abandoned slots whose consumer hasn't advanced.
type SlotInfo struct {
	Name              string
	Plugin            string
	Active            bool
	WALStatus         string
	RestartLSN        string
	ConfirmedFlushLSN string
}

// SlotManager is the engine-neutral surface for managing logical-
// replication slots from the operator-facing CLI (`sluice slot list`,
// `sluice slot drop`). Engines without a notion of replication slots
// don't implement it; the CLI checks for the optional
// `OpenSlotManager` method on the engine via type assertion and
// reports a clear error when the engine doesn't expose one.
//
// Slot management is destructive on the source side, so the CLI
// confirms before invoking [SlotManager.Drop] unless `--yes` is
// passed. Implementations should be idempotent on Drop: dropping a
// non-existent slot is a no-op rather than an error, mirroring
// Postgres' pg_drop_replication_slot semantics for the CLI's
// `--if-exists` mode.
type SlotManager interface {
	// List returns every replication slot visible to the connecting
	// role. The result is sorted by name for stable CLI output.
	List(ctx context.Context) ([]SlotInfo, error)

	// Drop removes the named slot. Returns an error wrapping
	// [sql.ErrNoRows] (or an engine-specific marker) when the slot
	// does not exist; callers can branch on that to honor a
	// `--if-exists` mode.
	//
	// Drop refuses to remove an active slot — an in-flight CDC
	// consumer is connected to it and yanking the slot would crash
	// that stream. Pass force=true to override (the operator has
	// confirmed they intend to disconnect the consumer).
	Drop(ctx context.Context, name string, force bool) error

	// Close releases the underlying connection pool.
	Close() error
}

// SlotManagerOpener is the optional interface engines implement to
// expose slot management to the CLI. The CLI checks for this method
// via type assertion and reports a clear error when the engine
// doesn't implement it (e.g. MySQL, where slot management isn't a
// concept on the source side).
type SlotManagerOpener interface {
	OpenSlotManager(ctx context.Context, dsn string) (SlotManager, error)
}

// MigrationPhase enumerates the phases a simple-mode migration can be
// in. Stored as a TEXT column so the wire shape is portable across
// engines and human-readable in ad-hoc psql/mysql sessions.
//
// The lifecycle is roughly:
//
//	pending -> tables -> bulk_copy -> identity_sync -> indexes -> constraints -> views -> complete
//
// Any phase can transition to `failed` on error; a subsequent
// `--resume` reads the stored phase and re-enters at that point.
type MigrationPhase string

const (
	// MigrationPhasePending is the initial state of a freshly-created
	// state row: the row exists but no phase has yet started. This is
	// transient — Migrator.Run flips to MigrationPhaseTables before
	// returning to the caller in normal operation. A row left in
	// `pending` indicates the orchestrator died between row-create and
	// the first phase, which `--resume` recovers from by re-running
	// every phase.
	MigrationPhasePending MigrationPhase = "pending"

	// MigrationPhaseTables covers schema phase 1 (CREATE TABLE without
	// constraints/indexes). The schema writers are idempotent on
	// re-run, so a partial-tables failure is recovered by simply
	// re-running this phase.
	MigrationPhaseTables MigrationPhase = "tables"

	// MigrationPhaseBulkCopy covers per-table bulk copy. Per-table
	// granularity lives in MigrationState.TableProgress; the phase
	// itself only flips to the next state when every table in the
	// schema is `complete`.
	MigrationPhaseBulkCopy MigrationPhase = "bulk_copy"

	// MigrationPhaseIdentitySync covers the post-bulk-copy
	// SyncIdentitySequences step (PG-only; no-op on MySQL). Idempotent.
	MigrationPhaseIdentitySync MigrationPhase = "identity_sync"

	// MigrationPhaseIndexes covers schema phase 2 (CREATE INDEX). The
	// engine schema writers' idempotence on re-run is best-effort here
	// — an existing index with a clashing name would error. A future
	// pass can pre-query INFORMATION_SCHEMA / pg_class and skip; v1
	// accepts the rough edge.
	MigrationPhaseIndexes MigrationPhase = "indexes"

	// MigrationPhaseConstraints covers schema phase 3 (foreign keys).
	// Same idempotence caveat as MigrationPhaseIndexes.
	MigrationPhaseConstraints MigrationPhase = "constraints"

	// MigrationPhaseViews covers schema phase 4 (CREATE VIEW /
	// CREATE MATERIALIZED VIEW). Re-runs are best-effort idempotent
	// for regular views (`CREATE OR REPLACE`); materialized views
	// emit a non-idempotent `CREATE MATERIALIZED VIEW`, which the
	// retry-on-failure orchestrator (see [pipeline.runViewsPhase])
	// treats as success on the second pass.
	MigrationPhaseViews MigrationPhase = "views"

	// MigrationPhaseComplete marks a clean finish. A row in this phase
	// blocks a re-run without --resume (operators must drop the target
	// or pick a fresh --migration-id) and surfaces "already complete"
	// on `--resume`.
	MigrationPhaseComplete MigrationPhase = "complete"

	// MigrationPhaseFailed marks the most recent attempt as errored.
	// MigrationState.LastError carries the wrapped error message,
	// truncated to 1KB. The stored Phase field also retains which
	// phase was running when the failure happened — that's what
	// drives resume's re-entry.
	MigrationPhaseFailed MigrationPhase = "failed"
)

// TableProgressState is the per-table tracking value within
// [TableProgress.State]. The state field maps directly onto the
// resume semantics: skip (`complete`), per-batch resume from cursor
// (`in_progress` with cursor data), or truncate-and-redo
// (`no_pk_truncate_and_redo`, or a v0.3.0-shape `in_progress` row).
type TableProgressState string

const (
	// TableProgressInProgress means the bulk-copy started writing the
	// table but did not complete. On --resume with v0.4.0 the cursor
	// fields on [TableProgress] drive a per-batch resume; on a row
	// written by v0.3.0 (no cursor) the orchestrator falls back to
	// truncate-and-redo.
	TableProgressInProgress TableProgressState = "in_progress"

	// TableProgressComplete means the bulk-copy finished cleanly. On
	// --resume, the table is skipped.
	TableProgressComplete TableProgressState = "complete"

	// TableProgressNoPKTruncateAndRedo marks a table without a primary
	// key. Per-batch checkpointing requires a PK-ordered cursor;
	// without one the table falls back to v0.3.0 truncate-and-redo on
	// every retry. The state is sticky across attempts — once a table
	// is classified as no-PK, every failure resumes via truncate-and-
	// redo regardless of how many rows the previous attempt landed.
	TableProgressNoPKTruncateAndRedo TableProgressState = "no_pk_truncate_and_redo"
)

// TableProgress is the per-table entry within
// [MigrationState.TableProgress]. The struct shape replaces the v0.3.0
// bare-string shape so per-batch resume can carry a cursor (last
// successfully-applied PK) and a row count alongside the lifecycle
// state.
//
// Wire shape on disk (within the table_progress JSON map):
//
//	"users":      "complete"                                       // v0.3.0 + v0.4.0
//	"orders":     {"state":"in_progress","last_pk":[12345],"rows_copied":12345}
//	"products":   {"state":"in_progress","last_pk":["a",7],"rows_copied":8000}
//	"events_log": "no_pk_truncate_and_redo"
//	"shipments":  {"state":"in_progress","chunks":[                // v0.5.0 parallel
//	    {"chunk_index":0,"upper_pk":[100],"last_pk":[42],"rows_copied":42,"state":"in_progress"},
//	    {"chunk_index":1,"lower_pk":[100],"upper_pk":[200],"state":"complete"}
//	]}
//
// The bare-string form is preserved for `complete` (compact, matches
// the v0.3.0 wire shape, easy to glance at in psql) and for the
// no-PK sentinel. Cursor-bearing rows use the object form. Custom
// JSON marshallers handle both shapes; see [TableProgress.UnmarshalJSON].
//
// Backward compatibility:
//
//   - v0.3.0 bare string `"in_progress"` decodes into
//     TableProgress{State: TableProgressInProgress} with a nil LastPK
//     and zero RowsCopied — the orchestrator treats that "no cursor"
//     case as truncate-and-redo on resume.
//   - v0.4.0 object form (LastPK + RowsCopied, no Chunks) decodes into
//     a single-chunk TableProgress; the orchestrator's classifier reads
//     it as the v0.4.0 single-cursor path. An old in-progress migration
//     resumed under v0.5.0 will continue on the single-reader path
//     (the parallel-copy decision is per-table, made at the start of
//     each table's copy on the v0.5.0 binary).
//   - v0.5.0 with Chunks populated is read as the parallel-copy form;
//     each chunk resumes from its own cursor. Operators upgrading
//     mid-migration should expect that in-flight tables retain their
//     original chunking; only freshly-started tables on the v0.5.0
//     binary use the new parallel layout.
type TableProgress struct {
	// State is the lifecycle phase of this table within the bulk-copy
	// stage of the migration.
	State TableProgressState

	// LastPK is the primary-key column values of the last successfully
	// committed row in the table on the single-chunk path. nil on
	// fresh start, on v0.3.0 rows, on no-PK tables, and on the
	// parallel-copy path (where per-chunk cursors live in [Chunks]).
	// For composite PKs the slice is in PK column declaration order.
	LastPK []any

	// RowsCopied is the count of rows committed so far on the single-
	// chunk path. The parallel-copy path reports its total via the sum
	// of [TableChunkProgress.RowsCopied] across [Chunks]; this field is
	// zero in that case.
	RowsCopied int64

	// Chunks holds the per-chunk progress entries when the table is
	// being copied via the parallel path (v0.5.0 and later). nil for
	// the single-chunk path (v0.4.0 and v0.5.0 below the parallelism
	// threshold). When non-nil, [LastPK] / [RowsCopied] are unused —
	// each chunk maintains its own cursor and row count, and the
	// orchestrator classifies each chunk independently on resume.
	Chunks []TableChunkProgress
}

// TableChunkProgress is the per-chunk entry within
// [TableProgress.Chunks] on the parallel-copy path. Each chunk owns a
// disjoint half-open PK range (`(LowerPK, UpperPK]` for chunks 1..N-1,
// `[min, UpperPK]` for chunk 0, `(LowerPK, max]` for chunk N-1) and a
// resume-cursor within that range.
//
// Range bounds are recorded once at parallel-copy launch and never
// change for the lifetime of a chunk. This matters for resume: the
// boundary computation (MIN/MAX/divide on the source PK column) runs
// only on the first attempt; subsequent --resume runs reuse the
// recorded bounds to keep chunks deterministic across restarts even
// if the source PK distribution shifts mid-migration.
//
// Wire shape on disk (object form within the chunks array):
//
//	{"chunk_index":2,"lower_pk":[200],"upper_pk":[300],"last_pk":[247],"rows_copied":47,"state":"in_progress"}
//
// LowerPK is exclusive (rows with PK > LowerPK), UpperPK is inclusive
// (rows with PK <= UpperPK). Chunk 0 has nil LowerPK to capture rows
// at the absolute minimum; chunk N-1 has nil UpperPK to capture rows
// at the absolute maximum. This matches the
// `WHERE pk > lower AND (upper IS NULL OR pk <= upper)` predicate
// shape the parallel reader emits.
type TableChunkProgress struct {
	// ChunkIndex is the 0..N-1 ordinal of this chunk within the
	// table's parallel layout. Stable across resume runs; the
	// orchestrator maps the same chunk to the same goroutine ordinal
	// on every attempt.
	ChunkIndex int

	// LowerPK is the exclusive lower bound of this chunk's PK range.
	// nil means "no lower bound" (chunk 0 captures rows at the
	// absolute minimum).
	LowerPK []any

	// UpperPK is the inclusive upper bound of this chunk's PK range.
	// nil means "no upper bound" (chunk N-1 captures rows at the
	// absolute maximum).
	UpperPK []any

	// LastPK is the cursor within this chunk — the PK of the last
	// successfully-committed row. nil before any rows commit. On
	// resume, the chunk re-reads from `pk > LastPK AND pk <= UpperPK`.
	LastPK []any

	// RowsCopied is the count of rows committed by this chunk so far.
	// Best-effort: a crash between batch commit and checkpoint write
	// may have landed more rows than the count reflects, but the
	// upsert path tolerates the drift.
	RowsCopied int64

	// State is the lifecycle phase of this chunk. The full
	// TableProgressState enum is overkill for chunks (no_pk doesn't
	// apply per-chunk), but reusing the type keeps the JSON wire shape
	// uniform; only `in_progress` and `complete` are meaningful here.
	State TableProgressState
}

// MigrationState is one row in the per-target sluice_migrate_state
// table. Returned by [MigrationStateStore.Read] and accepted by
// [MigrationStateStore.Write].
//
// Wire shape on disk (engine-neutral):
//
//	migration_id    TEXT PRIMARY KEY
//	phase           TEXT NOT NULL
//	table_progress  TEXT          -- JSON map[string]TableProgress
//	started_at      TIMESTAMP NOT NULL
//	updated_at      TIMESTAMP NOT NULL
//	last_error      TEXT          -- truncated to 1KB on write
//
// In-memory we de/serialize the JSON map into a Go map for convenient
// per-table updates. nil TableProgress is fine — first-run writes
// before any table starts use an empty map.
type MigrationState struct {
	MigrationID   string
	Phase         MigrationPhase
	TableProgress map[string]TableProgress
	StartedAt     time.Time
	UpdatedAt     time.Time
	LastError     string
}

// MigrationStateStore is the per-target persistence surface for
// resumable simple-mode migrations. Mirrors [ChangeApplier]'s
// EnsureControlTable / ReadPosition shape but with a different
// concept (one-shot migrations vs continuous-sync streams) and a
// different table (`sluice_migrate_state` vs `sluice_cdc_state`).
//
// Lifecycle:
//
//   - Migrator.Run calls EnsureControlTable once at startup.
//   - Read decides whether to start fresh, resume, or refuse.
//   - Write is called at every phase transition (one-row UPDATE) and
//     after each per-table bulk-copy boundary (table_progress JSON
//     refresh).
//
// Engines that don't support resumable migrations (none today) can
// simply not implement [MigrationStateStoreOpener]; the orchestrator
// falls back to non-resumable behaviour.
type MigrationStateStore interface {
	// EnsureControlTable creates the per-target sluice_migrate_state
	// table if it doesn't exist. Idempotent; safe to call on every
	// start. Includes any column-add migration for v0.2.x targets that
	// pre-date the table.
	EnsureControlTable(ctx context.Context) error

	// Read returns the row for migrationID, or ok=false when no row
	// exists. ok=false means "fresh migration"; ok=true means "row
	// found, decide resume vs refuse based on Phase".
	Read(ctx context.Context, migrationID string) (MigrationState, bool, error)

	// Write upserts the row. The store implementation is responsible
	// for setting updated_at to the current wall-clock time and for
	// preserving started_at across updates (only the first Write for
	// a given migration_id sets it). LastError is stored verbatim;
	// callers should truncate before passing.
	Write(ctx context.Context, state MigrationState) error

	// ClearMigration deletes the row for migrationID. Used by the
	// `--reset-target-data` recovery path (ADR-0023) before the dest
	// tables are dropped so the next cold-start sees a clean state
	// row alongside an empty schema. Idempotent and tolerant of a
	// missing row (returns nil) — re-running the reset on a target
	// whose row was already cleared is not an error.
	ClearMigration(ctx context.Context, migrationID string) error

	// Close releases the underlying connection pool.
	Close() error
}

// MigrationStateStoreOpener is the optional engine interface that
// exposes resumable-migration state persistence. Engines without a
// SQL surface for this (none today) can omit the method; the
// orchestrator type-asserts and falls back to non-resumable behaviour
// when the assertion fails.
//
// Same shape as [SlotManagerOpener]: optional, type-asserted at the
// call site, so adding a new engine doesn't force every existing
// engine to grow a stub.
type MigrationStateStoreOpener interface {
	OpenMigrationStateStore(ctx context.Context, dsn string) (MigrationStateStore, error)
}

// Engine is the bundle of operations a database engine implementation
// provides. Engine packages register themselves with the engines
// registry at init time; the orchestrator looks them up by name based
// on user configuration.
//
// An engine may return a non-nil error for OpenCDCReader and
// OpenChangeApplier if its [Capabilities.CDC] is [CDCNone]; callers
// must check capabilities before requesting these.
type Engine interface {
	// Name is the short identifier used in configuration files and on
	// the command line (e.g. "mysql", "postgres").
	Name() string
	// Capabilities reports what this engine supports.
	Capabilities() Capabilities

	OpenSchemaReader(ctx context.Context, dsn string) (SchemaReader, error)
	OpenSchemaWriter(ctx context.Context, dsn string) (SchemaWriter, error)
	OpenRowReader(ctx context.Context, dsn string) (RowReader, error)
	OpenRowWriter(ctx context.Context, dsn string) (RowWriter, error)
	OpenCDCReader(ctx context.Context, dsn string) (CDCReader, error)
	OpenChangeApplier(ctx context.Context, dsn string) (ChangeApplier, error)

	// OpenSnapshotStream captures a consistent snapshot of the source
	// and returns a paired RowReader (snapshot-pinned) and CDCReader
	// (positioned to start where the snapshot ended). Engines without
	// CDC support return an error wrapping the engine's
	// ErrNotImplemented.
	OpenSnapshotStream(ctx context.Context, dsn string) (*SnapshotStream, error)
}

// CDCReaderWithSlotOpener is the optional engine surface for engines
// whose CDC implementation can be configured with a non-default
// replication-slot name (today: Postgres). When the orchestrator has
// an operator-supplied slot name (`--slot-name` on `sync start`), it
// type-asserts on this interface and uses [OpenCDCReaderWithSlot]
// instead of the default [Engine.OpenCDCReader] — engines that don't
// implement this interface fall back to their built-in default name.
//
// Engines without a slot concept (MySQL: binlog stream is the slot)
// do not implement this surface; the orchestrator silently ignores
// the slot-name flag for those engines.
type CDCReaderWithSlotOpener interface {
	OpenCDCReaderWithSlot(ctx context.Context, dsn, slotName string) (CDCReader, error)
}

// SnapshotStreamWithSlotOpener is the corresponding optional surface
// for [Engine.OpenSnapshotStream]. The slot is created at snapshot-
// open time, so the slot name has to flow in at construction (not
// post-open via a setter). Same engine-set as [CDCReaderWithSlotOpener]
// — Postgres implements both.
type SnapshotStreamWithSlotOpener interface {
	OpenSnapshotStreamWithSlot(ctx context.Context, dsn, slotName string) (*SnapshotStream, error)
}

// SnapshotStreamResumer is the optional engine surface for resuming an
// INTERRUPTED cold-start COPY after a process restart (v0.99.8). Today
// only the PlanetScale (VStream) flavor implements it: its snapshot COPY
// checkpoints a mid-COPY resume cursor (Vitess per-shard TablePKs;
// ADR-0072) to the control table, so a process restart can continue the
// bulk COPY from the last-copied PK rather than restarting from row 0.
//
// The pipeline routes a process-restart resume here — instead of the
// plain CDC warm-resume path — when [PositionCarriesCopyCursor] reports
// that the persisted position carries such a cursor. Seeding the bulk
// snapshot stream from the cursor makes vtgate's re-emitted COPY-tail
// rows flow through the batched bulk-COPY writer (~4000 rows/sec) rather
// than the plain CDC reader's per-row apply path (~10 rows/sec), which is
// the silent-degrade this surface fixes: without it, the un-copied tail
// of a large table trickles indefinitely with no error.
//
// Engines without a mid-COPY cursor (Postgres, vanilla MySQL — their
// snapshots are single all-or-nothing transactions) do not implement
// this surface; the orchestrator keeps every cursor-less resume on the
// plain CDC warm-resume path.
type SnapshotStreamResumer interface {
	// PositionCarriesCopyCursor reports whether the persisted position
	// was written mid-COPY (carries a resume cursor) and therefore needs
	// the bulk resume path. A cursor-less position (completed cold-start,
	// or a position the engine can't decode) returns false and stays on
	// the plain CDC warm-resume path. This is a routing hint, not a
	// validation gate — it never returns an error.
	PositionCarriesCopyCursor(from Position) bool

	// OpenSnapshotStreamFromPosition resumes the bulk COPY from the
	// cursor carried by from, returning the same paired RowReader/CDCReader
	// shape as [Engine.OpenSnapshotStream]. The COPY continues from the
	// cursor (no full re-copy) and transitions to CDC on completion. Must
	// refuse loudly if from carries no cursor (seeding a bulk snapshot
	// from a cursor-less position would silently re-copy from row 0).
	//
	// tables scopes the resumed COPY filter, with the same semantics as
	// [TableScopedSnapshotOpener.OpenSnapshotStreamForTables]: empty/nil
	// means "all tables" (unchanged whole-keyspace behavior); a non-empty
	// allowlist restricts the resumed COPY to those unqualified table names.
	// Vitess's resume cursor is PER-TABLE, so passing the current allowlist
	// is correct in every case — tables with a cursor entry resume from it,
	// allowlisted tables with no cursor entry start fresh, and a table
	// dropped from the allowlist simply stops being copied.
	OpenSnapshotStreamFromPosition(ctx context.Context, dsn string, from Position, tables []string) (*SnapshotStream, error)
}

// TableScopedSnapshotOpener is the optional engine surface for engines
// whose snapshot streams the WHOLE source keyspace/database by default
// and can be told to scope the COPY to a specific table allowlist.
type TableScopedSnapshotOpener interface {
	// OpenSnapshotStreamForTables opens a snapshot stream whose COPY is
	// scoped to exactly `tables` (unqualified names within the source
	// keyspace/database), returning the same paired RowReader/CDCReader
	// shape as [Engine.OpenSnapshotStream].
	//
	// Engines whose snapshot streams the whole keyspace by default
	// (PlanetScale VStream: vtgate copies every table matched by the
	// filter rules) implement this so a large unrelated table in the
	// same keyspace is never streamed/buffered — avoiding the ADR-0071
	// multi-table-interleaving buffer overflow when only a subset of a
	// busy keyspace is in scope. An empty `tables` means "all tables"
	// (identical to [Engine.OpenSnapshotStream]).
	//
	// Engines whose snapshot is already per-table (Postgres per-table
	// COPY, vanilla MySQL per-table dump) gain nothing from scoping —
	// they never over-stream — so they need not implement this surface;
	// when they do, they may simply delegate to the default open.
	OpenSnapshotStreamForTables(ctx context.Context, dsn string, tables []string) (*SnapshotStream, error)
}

// DefaultTableExcluder is the optional engine surface for "tables
// the operator almost never wants to migrate against this engine".
// Implementing engines return a list of [path.Match]-style patterns
// that the orchestrator merges into the operator's
// [pipeline.TableFilter.Exclude] when the operator is in
// exclude-or-no-filter mode. Operator-supplied
// [pipeline.TableFilter.Include] short-circuits the merge — if the
// operator explicitly opts in to a table list, the engine doesn't
// override it.
//
// Used today for PlanetScale's Vitess table-lifecycle shadow tables
// (`_vt_*` — the tablet-internal HOLD/PURGE/EVAC/DROP staging
// tables); operators almost never want sluice to copy or stream
// those, and the symptom of accidentally including them is a quiet
// flood of internal-state churn that bears no relation to user data.
//
// The DSN is supplied so the engine can return DSN-derived defaults
// (e.g. v0.8.1's PlanetScale hostname auto-detect for the vanilla
// MySQL flavor: `*.connect.psdb.cloud` endpoints carry Vitess shadow
// tables even when the operator chose `--source-driver=mysql`). An
// empty DSN is acceptable — engines fall back to flag-keyed
// defaults.
type DefaultTableExcluder interface {
	// DefaultExcludePatterns returns a list of glob patterns to
	// merge into the operator's exclude list. Empty / nil disables
	// the default; equivalent to not implementing the interface.
	// The dsn parameter is the source DSN, supplied so the engine
	// can return DSN-keyed defaults in addition to flag-keyed ones.
	DefaultExcludePatterns(dsn string) []string
}
