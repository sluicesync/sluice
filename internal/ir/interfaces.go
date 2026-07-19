// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"errors"
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

// BoundedBatchedRowReader is an optional extension of [BatchedRowReader]
// that additionally bounds a batch by an INCLUSIVE upper PK. It is the
// load-bearing exactly-once surface for the parallel within-table chunk
// copy (ADR-0096): a chunk's read must be clipped at the chunk's upper
// boundary, and that clip MUST agree with the engine's ORDER BY total
// order — which is the column's NATIVE DB COLLATION, not a byte order.
//
// The earlier design clipped the upper bound in Go with a bytewise tuple
// comparator while the SQL ORDER BY used the column collation. For string
// / varchar / char PKs under a non-C collation (PG en_US.utf8, MySQL
// utf8mb4_0900_ai_ci) and for decimal-as-text PKs the two orders DIVERGE,
// so a boundary-straddling row could be excluded by BOTH the chunk above
// it (Go says "past upper") AND the chunk below it (SQL says "<= lower"),
// landing in NO chunk — a silent permanent row loss (the Bug-74 class).
// Pushing the upper bound into the SQL WHERE makes BOTH bounds use the
// same collation and the same PK index, so the partition is exactly-once
// for every orderable family by construction.
//
// ReadRowsBatchBounded returns up to limit rows where
// (pk) > after AND (pk) <= upTo, in PK ascending order. after may be nil
// (start of the chunk's range / table); upTo may be nil (the last chunk
// has no upper bound, identical to [BatchedRowReader.ReadRowsBatch]).
// When both are nil the behaviour is identical to ReadRowsBatch.
//
// The orchestrator REQUIRES this surface for the non-integer / composite
// keyset chunk strategy: an engine that does not implement it routes
// those tables to the single-reader path rather than risk the bytewise
// vs collation mismatch. The shipping engines (MySQL, Postgres) both
// implement it.
type BoundedBatchedRowReader interface {
	BatchedRowReader

	// ReadRowsBatchBounded returns up to limit rows from table where the
	// PK is strictly greater than after AND less than or equal to upTo
	// (both compared in the engine's native PK order, i.e. the column's
	// DB collation), streamed in PK ascending order. nil after means "no
	// lower bound"; nil upTo means "no upper bound".
	ReadRowsBatchBounded(ctx context.Context, table *Table, after, upTo []any, limit int) (<-chan Row, error)
}

// BatchedReadDisqualifier is the optional surface a [BatchedRowReader] can
// implement to VETO cursor-paginated reads for a SPECIFIC table whose PK
// shape its decoded-value cursor cannot round-trip — the engine-local escape
// hatch for the case where implementing [BatchedRowReader] would otherwise
// silently corrupt a table.
//
// The cursor loops ([copyTableWithCursor], [copyChunk]) advance the `after`
// tuple from the DECODED streamed row value (the same value the writer
// applies), then re-bind it as the `>` bound of the next page. That only
// works when the decoded value re-binds to the SAME ordered key the column
// stores. For most engine/type pairs it does. SQLite is the exception: a
// temporal column's value is decoded to a Go time.Time (or a formatted
// time-of-day string) and a NUMERIC column's to a decimal string, but the
// underlying storage may be INTEGER / REAL / TEXT — so the re-bound cursor
// compares in the WRONG class against the column's ORDER BY and the next
// page can come back EMPTY (silent truncation) or re-select copied rows
// (silent dup). Reconstructing the original stored form from the decoded
// value is not possible, so such a table must NOT drive the cursor at all.
//
// A reader returns (true, reason) to disqualify the table: the orchestrator
// then routes it to the whole-table single-reader copy ([copyTable]) for
// BOTH the within-table parallel-chunk path AND the per-batch resume path
// ([canResumePerBatch]), so neither ever binds a non-round-trippable cursor.
// (false, "") — or not implementing the interface — keeps the default
// cursor-eligible behaviour. The reason is logged so the routing is
// auditable.
type BatchedReadDisqualifier interface {
	BatchedRowReader

	// DisqualifiesBatchedRead reports whether cursor-paginated reads are
	// UNSAFE for this table (its PK can't round-trip a decoded cursor).
	DisqualifiesBatchedRead(table *Table) (disqualified bool, reason string)
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

// ApplyConcurrencySetter is the optional surface a [ChangeApplier] can
// implement to receive the ADR-0104 (item 23(c)) key-hash apply LANE count
// W: the merged CDC change stream is fanned across W in-order apply lanes
// by primary-key hash (same key → same lane → in-order, so the
// dependent-row hazard cannot occur), each committing concurrently on a
// dedicated backend, lifting aggregate apply throughput toward W× while the
// resume position advances only to a fully-durable source boundary (the
// seq-frontier). This is the LIVE successor to the ADR-0104 Phase-1 commit
// pipeline (proven ineffective — commit-only overlap); the streamer threads
// [pipeline.Streamer.ApplyConcurrency] to every applier that exposes it.
//
// Zero-value-safe (the v0.99.51 trap): W 0 and 1 BOTH mean serial —
// byte-identical to the pre-concurrency path. Concurrency engages ONLY when
// an operator explicitly sets W > 1 and a dedicated pool is available. Only
// the MySQL target implements it today (Postgres uses ADR-0092's
// within-transaction statement pipelining instead).
type ApplyConcurrencySetter interface {
	SetApplyConcurrency(lanes int)
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

// KeysetSampler is the optional surface a [RowReader] can implement to
// expose sampled-keyset chunk boundaries for a table whose primary key
// is NOT a single integer column — a single non-integer orderable PK
// (UUID, string, binary, decimal, temporal) or a composite PK
// (ADR-0096). It is the (c) NTILE/ROW_NUMBER() strategy ADR-0019
// deferred; [RangeBoundsQuerier] (MIN/MAX/divide) stays the path for
// single integer PKs.
//
// SampleKeysetBoundaries returns n-1 INTERIOR boundary tuples that split
// the table into n approximately equal ROW-COUNT slices, ordered by the
// PK columns. Each returned tuple has len(pkColumns) values, in
// pkColumns order, scanned in the engine's canonical [Row] value shape
// (so they round-trip through the same parameter binding the cursor
// predicate uses). The orchestrator assembles them into half-open
// (LowerPK, UpperPK] chunk ranges:
//
//   - boundary[k] is the INCLUSIVE upper bound of chunk k and the
//     EXCLUSIVE lower bound of chunk k+1, matching the engine's
//     WHERE (pk...) > (...) / <= (...) row-comparison total order.
//
// The split is by actual row count (ROW_NUMBER() over the PK index),
// so it is skew-free regardless of how clustered the keyspace is — the
// reason the keyset strategy exists for exactly the UUID/string keys
// MIN/MAX/divide would skew on.
//
// Returning fewer than n-1 distinct boundaries (a tiny or
// heavily-duplicate-keyed table, or an empty table) is NOT an error: the
// orchestrator drops zero-width interior chunks and, if too few remain,
// routes the table to the single-reader path. Returning an error makes
// the table fall back to single-reader too — keyset chunking is a
// performance optimisation whose absence is never a correctness problem.
//
// Like [RangeBoundsQuerier] this MUST run strictly pre-stream (the
// chunk-boundary decision is single-goroutine and precedes any per-chunk
// copy stream), so a snapshot-pinned reader either runs it on a conn
// that cannot race an in-flight stream or returns an error to fall back.
//
// Engines that don't implement this interface keep the ADR-0019
// behaviour: non-integer/composite-PK tables stay on the single-reader
// path. The shipping engines (MySQL, Postgres) both implement it.
type KeysetSampler interface {
	SampleKeysetBoundaries(ctx context.Context, table *Table, pkColumns []string, n int) ([][]any, error)
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

// RowCountEstimator is the optional surface a [RowReader] can implement
// to supply a row-count estimate used ONLY for the pre-stream within-
// table chunk DECISION ([shouldParallelChunk] in the parallel-bulk-copy
// orchestrator). It is deliberately SEPARATE from [RowCounter]:
//
//   - [RowCounter.CountRows] is also consumed by the throughput/ETA probe
//     ([kickOffRowCount]), which fires CONCURRENTLY with the in-flight
//     copy stream on the SAME reader. On a snapshot-pinned reader (a PG
//     stream/import reader whose every query runs on one pinned conn) a
//     count query racing the live row-stream would conflict on the
//     connection — so CountRows returns (0, nil) for pinned readers.
//   - EstimateRowCount is invoked ONLY pre-stream (single-goroutine,
//     before any copy stream opens), so an implementation MAY query the
//     catalog even when pinned — provided it does so on a connection that
//     CANNOT race an in-flight stream (e.g. a fresh off-snapshot conn for
//     snapshot-insensitive catalog metadata like pg_class.reltuples). It
//     must NEVER be wired into the ETA path.
//
// Returning (0, nil) means "no estimate → route to the single-stream
// path"; errors are non-fatal (the orchestrator falls back to single-
// reader). The orchestrator prefers this surface over [RowCounter] for
// the chunk decision when a reader implements it.
type RowCountEstimator interface {
	EstimateRowCount(ctx context.Context, table *Table) (int64, error)
}

// ExactCountEstimateOptIn is the optional surface a snapshot-pinned
// [RowCountEstimator] reader can implement to let an orchestrator OPT
// the reader into resolving the never-ANALYZEd catalog-estimate
// sentinel with an exact COUNT(*) on the reader's FRESH off-snapshot
// estimator connection — never the pinned conn (safety is identical
// either way; declining the fallback was always a COST decision, the
// ADR-0079 v1.1 disposition for sync cold-start import readers).
//
// The chunk DECISION is a size estimate with no consistency
// requirement, but a freshly-loaded, never-ANALYZEd source reports the
// catalog sentinel and would silently route every large table to the
// single-stream path (the 59c55e27 / TestRawCopy_ChunkedZeroLoss
// regression class). Paths whose contract is "a fresh source still
// chunks" — the migrate shared-snapshot primary (which the engine opts
// in itself at ExportSnapshot) and the backup within-table planning
// readers (ADR-0149, which the orchestrator opts in through this
// surface) — enable the exact-count fallback; sync cold-start import
// readers are deliberately never opted in (v1.1 unchanged).
type ExactCountEstimateOptIn interface {
	// EnableExactCountEstimate switches the reader's EstimateRowCount
	// never-ANALYZEd-sentinel resolution to an exact COUNT(*) on its
	// fresh estimator connection. Pre-stream only; idempotent.
	EnableExactCountEstimate()
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

// LossyFloatCopyReader is the OPTIONAL surface a snapshot [RowReader]
// implements to declare that its cold-start COPY DISPLAY-ROUNDS
// single-precision FLOAT columns — the value lands at mysqld's
// 6-significant-digit text precision rather than float32-exact (the
// VStream-COPY FLOAT display-rounding class, roadmap open-bug 2026-07-09).
//
// Only the MySQL VStream cold-start reader (PlanetScale/Vitess flavors)
// implements it and returns true: its rows arrive over vttablet's
// rowstreamer, whose bare-column SELECT is built inside vttablet and
// renders FLOAT through mysqld's float→text formatter (8388608 → 8388610).
// The projection ADR-0153's SQL reader uses to fix this (`(col * 1E0)`)
// cannot be injected into that server-side SELECT. Vanilla-MySQL binlog
// snapshot + Postgres snapshot readers do NOT implement it — their COPY
// is already float-exact.
//
// The pipeline uses this as the GATE for the post-COPY FLOAT re-read
// repair (sync cold-start) and the schema-triggered WARN / --strict-float
// refusal (backup): a reader that returns true triggers the mitigation;
// one that doesn't (or doesn't implement the surface) leaves the path
// byte-identical. Distinct from [IdempotentCopyReader]: a reader can be
// idempotent without display-rounding floats, and vice versa — coupling
// the two would be the wrong signal.
type LossyFloatCopyReader interface {
	RowReader

	// CopyDisplayRoundsFloats reports whether this reader's COPY phase
	// display-rounds single-precision FLOAT columns (so the cold-start
	// must re-read them exactly, or a backup must WARN/refuse).
	CopyDisplayRoundsFloats() bool
}

// FloatRepairWriter is the OPTIONAL target surface the post-COPY FLOAT
// re-read repair uses to correct single-precision FLOAT columns a
// [LossyFloatCopyReader]'s COPY landed display-rounded (roadmap open-bug
// 2026-07-09). After the cold-start COPY completes and BEFORE CDC apply
// begins, the pipeline re-reads the affected FLOAT columns EXACTLY over a
// separate SQL path (float32-exact projection) and hands the rows here to
// UPDATE the target rows by primary key.
//
// Each streamed [Row] carries the table's PRIMARY-KEY column values plus
// the single-precision FLOAT column values to set — nothing else.
// pkColumns names the PK columns (the remaining Row keys are the FLOAT
// columns). The writer issues one `UPDATE <table> SET <float...> WHERE
// <pk...>` per row, in bounded transactions. It is IDEMPOTENT and
// PK-keyed: re-running lands the same values, and a row absent on the
// target (deleted between COPY and re-read) is a zero-rows-affected no-op,
// not an error — the subsequent CDC replay from the copy anchor is the
// authority on such rows.
//
// The MySQL and Postgres targets implement it. A target that does not
// (SQLite/D1, or a future engine) leaves the repair un-runnable; the
// pipeline then falls back to the WARN-only backup posture for that run
// rather than silently proceeding as if repaired.
type FloatRepairWriter interface {
	// UpdateFloatColumnsByPK sets the FLOAT columns of each streamed row
	// on the row identified by its pkColumns values. Returns loudly on any
	// target error; a zero-rows-affected UPDATE (row gone) is not an error.
	UpdateFloatColumnsByPK(ctx context.Context, table *Table, pkColumns []string, rows <-chan Row) error
}

// ConcurrentCopyPartitioner is the OPTIONAL surface a snapshot
// [RowReader] implements to declare that its cold-start COPY may be
// drained CONCURRENTLY across disjoint table groups (ADR-0100, the
// WRITE-side companion to ADR-0099's K-independent-read-streams lever).
//
// ADR-0099 makes the VStream cold-copy READ side concurrent: K
// independent vtgate VStreams, each over a DISJOINT subset of the
// in-scope tables, all filling one shared per-table row buffer. But the
// orchestrator's serial bulk-copy loop drains one table at a time, so
// only one table is ever WRITTEN at a time — the measured ~1.4× ceiling
// (the target PROCESSLIST showed exactly one table receiving rows). This
// surface lets the engine hand the pipeline the EXACT disjoint partition
// it gave the producers, so the pipeline can run one read→write consumer
// pipeline per group concurrently (W = K), instead of one shared serial
// consumer. Each group's producer fills its tables' queues; each group's
// consumer drains+writes them — the 1:1 producer↔consumer-per-table
// coupling (and ADR-0099's per-stream byte sub-budget) is preserved.
//
// The MySQL VStream snapshot reader implements it: it returns the same
// groups [partitionTablesForStreams] produced at open (stored on the
// snapshot stream), so producer partition ≡ consumer partition by
// construction — the coverage + disjointness ADR-0099 unit-pins is
// inherited, not re-derived. The pipeline type-asserts on this surface
// during cold-start and, when it returns ≥2 groups, replaces the serial
// table loop with a W-goroutine errgroup (one consumer per group). nil /
// ≤1 group ⇒ the serial loop runs BYTE-IDENTICALLY (the zero-value-safe
// default — K = 1 / single-stream / a one-table scope all surface no
// groups).
//
// Readers that don't implement it (Postgres snapshot, MySQL binlog
// snapshot, single-stream VStream) keep the serial cold-start loop.
//
// CORRECTNESS (silent-loss class): every in-scope table must appear in
// EXACTLY ONE group — none dropped (a silently un-written table), none
// duplicated (two consumers draining one queue). The pipeline relies on
// this; the engine guarantees it via the disjoint partition it also gave
// the producers.
type ConcurrentCopyPartitioner interface {
	RowReader

	// ConcurrentCopyGroups returns the disjoint table groups the
	// cold-start bulk copy may write CONCURRENTLY — one consumer pipeline
	// per group, each group's tables drained serially within the group.
	// Returns nil (or a single group) when no cross-table write
	// concurrency is engaged, in which case the orchestrator runs the
	// serial table loop unchanged. Names are unqualified (matching the
	// COPY filter + ReadRows scope).
	ConcurrentCopyGroups() [][]string
}

// WorkStealingCopyReader is the OPTIONAL surface a concurrent snapshot
// [RowReader] implements when its N readers ALL observe the SAME consistent
// snapshot, so ANY reader can read ANY in-scope table and see identical data
// — which lets the pipeline replace the static disjoint partition (drained
// one group per pipeline) with WORK-STEALING: N pipelines pull tables from a
// shared queue, each reading its pulled table on its OWN reader. This keeps
// the cold-copy N-wide to the tail instead of tapering as the lighter
// [ConcurrentCopyPartitioner] groups finish early and idle (roadmap item 21a).
//
// The native-MySQL multi-snapshot reader implements it: its N pinned
// connections are each a `REPEATABLE READ` / `START TRANSACTION WITH
// CONSISTENT SNAPSHOT` opened under ONE FLUSH TABLES WITH READ LOCK (ADR-0101),
// so every connection is the SAME cut — reading table T on connection i is
// byte-identical for any i. The VStream reader does NOT implement it: each of
// its K streams is a separate vtgate session Match-scoped to its group at
// open, so a reader has rows ONLY for its own group — work-stealing there
// needs source-stream restructuring and stays on the static
// [ConcurrentCopyPartitioner] partition.
//
// CORRECTNESS (silent-loss class): the pipeline guarantees each in-scope
// table is claimed by EXACTLY ONE pipeline (atomic queue claim) and that no
// two pipelines ever issue a concurrent read on the same reader index — so
// each pinned connection still has at most one in-flight query, exactly as
// the static partition gave for free. The native single recorded FTWRL
// position is independent of WHICH connection read a table, so the
// snapshot→CDC seam is unaffected by stealing.
type WorkStealingCopyReader interface {
	ConcurrentCopyPartitioner

	// ConcurrentReaderCount returns the number of independent readers
	// (connections), each able to read ANY in-scope table from the same
	// consistent snapshot. >1 enables work-stealing.
	ConcurrentReaderCount() int

	// ReadRowsOn reads table on reader index i (0 <= i < ConcurrentReaderCount)
	// — the work-stealing analogue of [RowReader.ReadRows], which routes by the
	// table's statically-assigned reader. The caller guarantees at most one
	// in-flight ReadRowsOn per reader index i at a time.
	ReadRowsOn(ctx context.Context, table *Table, reader int) (<-chan Row, error)
}

// ChunkedWorkStealingCopyReader is the OPTIONAL surface a
// [WorkStealingCopyReader] implements when, in addition to letting any reader
// read any WHOLE table from the shared consistent snapshot, it can read an
// arbitrary PK-RANGE chunk of a table on a pinned reader. This makes the unit
// of stealable work a (table, PK-range) chunk rather than a whole table, so the
// cold-copy stays N-wide down to a chunk of the last large table instead of
// tapering to one whole big table at the tail (ADR-0119, roadmap item 21b — the
// intra-table refinement of [WorkStealingCopyReader]'s table-level stealing).
//
// The native-MySQL multi-snapshot reader implements it: every pinned connection
// is the SAME FTWRL cut (ADR-0101), so reading the half-open PK range
// (lowerPK, upperPK] of table T on connection i is byte-identical for any i —
// exactly what splitting one large table across several idle readers needs. The
// VStream reader does NOT (its per-stream Match-scoping is not range-splittable),
// so it stays on the whole-table [WorkStealingCopyReader] / static partition.
//
// The pipeline computes the chunk boundaries through the embedded
// [RangeBoundsQuerier] (single integer PK → MIN/MAX/divide) and [KeysetSampler]
// (non-integer / composite PK → sampled keyset), reusing the SAME boundary
// machinery the `sluice migrate` parallel copy pins (ADR-0019 / ADR-0096) — so
// it never imports engine internals to tile a table. Those boundary queries are
// a partition HINT (run on a side metadata pool, NOT a pinned snapshot conn);
// the chunk ranges tile (-inf, +inf] regardless of how fresh the sampled split
// points are, so coverage stays complete and disjoint even if the live table
// has drifted from the frozen cut (ADR-0119 Decision 2).
//
// CORRECTNESS (silent-loss class): the M chunks of a table partition its rows
// with NO gap and NO overlap — the bounds are half-open (lowerPK, upperPK] with
// nil end-caps (chunk 0 lowerPK==nil, last chunk upperPK==nil) from the shared
// boundary code, and the upper clip is pushed into SQL in the column's native
// collation (the Bug-74 contract). A table is read EITHER whole OR as chunks,
// never both, and each chunk is claimed by exactly one pipeline (the pipeline's
// atomic claim), so every source row reaches the target exactly once.
type ChunkedWorkStealingCopyReader interface {
	WorkStealingCopyReader

	// ReadRowsRangeOn reads the half-open PK range (lowerPK, upperPK] of table
	// on the pinned connection `reader`, paging with the collation-correct SQL
	// upper-bound clip ([BoundedBatchedRowReader.ReadRowsBatchBounded]). A nil
	// lowerPK means "no lower bound" (chunk 0); a nil upperPK means "no upper
	// bound" (the last chunk) — together they tile the keyspace. chunkIndex
	// disambiguates the per-(table,chunk) in-process resume cursor so concurrent
	// chunks of one table never alias on the reader's shared cursor map; a
	// negative chunkIndex denotes a whole-table read keyed on the table name
	// (equivalent to [WorkStealingCopyReader.ReadRowsOn]). The caller guarantees
	// at most one in-flight read per reader index at a time.
	ReadRowsRangeOn(ctx context.Context, table *Table, lowerPK, upperPK []any, chunkIndex, reader int) (<-chan Row, error)

	// RangeBoundsQuerier (single integer PK) and KeysetSampler (non-integer /
	// composite PK) are surfaced so the pipeline can compute chunk boundaries
	// without importing engine internals. They MUST run on a connection that
	// cannot race an in-flight snapshot read (a side metadata pool), since the
	// boundary decision is pre-read and the pinned conns are streaming.
	RangeBoundsQuerier
	KeysetSampler
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

// ParallelIdempotentCopyWriter is the OPTIONAL writer capability that
// enables WRITE-side fan-out on the VStream/CDC snapshot cold-start
// copy (ADR-0097). On a PlanetScale-MySQL target the snapshot writer
// falls back to a single cross-region-RTT-bound batched-INSERT
// connection (vtgate blocks LOAD DATA LOCAL INFILE); fanning the one
// incoming snapshot row stream out to N concurrent batched-INSERT
// workers — each on its own pinned connection — closes that gap. The
// READ side cannot be PK-range-chunked (vtgate streams the snapshot;
// no arbitrary range SELECT), so write-side fan-out is the only lever
// on this path. Contrast [ADR-0019]'s read-side chunking for the
// `sluice migrate` path, which can range-SELECT the source.
//
// The pipeline owns the reader goroutine + the PK-hash partition that
// routes every row to EXACTLY ONE of the supplied per-worker channels
// (no drop, no dup) — so the same PK can never be in two workers'
// in-flight batches (Bug-125 COPY re-emissions of a PK serialize on
// one worker; cross-worker interleaving is irrelevant because every
// write is an upsert, ADR-0010). The writer owns only the N-worker
// execution: it MUST run one goroutine per supplied channel, each
// pinning its own target connection and running the engine's existing
// idempotent batched-INSERT core, and MUST return only after EVERY
// worker has fully drained its channel and DURABLY committed its
// final batch (the ADR-0007 position-handoff guard — no position is
// advanced until this returns nil). Any worker error aborts the whole
// copy loudly (returns non-nil, the orchestrator advances no
// position); a ctx cancel unwinds every worker and leaks no
// goroutine or connection.
//
// The MySQL target implements it. Postgres does not (its eligible
// cold-start copy uses the fast raw-COPY / parallel snapshot path,
// ADR-0079; the VStream-source serial idempotent path that reaches
// this fan-out is the PS-MySQL gap). The pipeline type-asserts on this
// surface and, when present with a usable partition key and a degree
// > 1, fans out; otherwise it falls through to the serial
// WriteRowsIdempotent — never silently doing nothing.
type ParallelIdempotentCopyWriter interface {
	IdempotentRowWriter

	// WriteRowsIdempotentParallel runs the idempotent batched-copy over
	// len(workers) concurrent workers, each consuming one of the
	// supplied per-worker channels and pinning its own target
	// connection. Returns only after every worker has drained and
	// durably committed, or the first worker error (loudly), or the
	// ctx error on cancel. len(workers) == 1 is equivalent to
	// WriteRowsIdempotent on that one channel.
	WriteRowsIdempotentParallel(ctx context.Context, table *Table, workers []<-chan Row) error
}

// ParallelCopyWriter is the OPTIONAL writer capability that enables
// WRITE-side fan-out on the PLAIN-INSERT cold-start copy (ADR-0102) —
// the gap-free, fresh-target analogue of [ParallelIdempotentCopyWriter].
// It is the lever for the native-MySQL concurrent cold-copy (ADR-0101):
// each of the W concurrent table pipelines fans its active table's writes
// across D plain-INSERT workers → W × D, closing the remaining throughput
// gap to the VStream W × D ceiling on a cross-region PS-MySQL target where
// a single batched-INSERT connection is RTT-bound (vtgate blocks LOAD DATA
// LOCAL INFILE).
//
// It mirrors [ParallelIdempotentCopyWriter] exactly EXCEPT the write core:
// plain INSERT, not upsert. Plain INSERT is correct here because the native
// concurrent path is a gap-free snapshot (each row read exactly once from a
// frozen REPEATABLE-READ view, no re-emission) onto a FRESH target (the
// cold-start-from-scratch path resets a populated target first), and the
// disjoint partition + PK-hash routing means every row is written by
// exactly one worker — no overlap, nothing to absorb. (Contrast the
// idempotent surface, which exists precisely because the VStream COPY
// re-emits rows, Bug 125.)
//
// The pipeline owns the reader goroutine + the PK-hash partition that
// routes every row to EXACTLY ONE of the supplied per-worker channels (no
// drop, no dup — the same [partitionRowsByPK] the idempotent fan-out uses).
// The writer owns only the N-worker execution: one goroutine per channel,
// each pinning its own target connection and running the engine's existing
// PLAIN batched-INSERT core, returning only after EVERY worker has drained
// and DURABLY committed (the ADR-0007 position-handoff guard). Any worker
// error aborts the copy loudly; a ctx cancel unwinds every worker and leaks
// no goroutine or connection.
//
// The MySQL target implements it. Postgres does not (its eligible
// cold-start copy uses the fast raw-COPY / parallel snapshot path,
// ADR-0079). The pipeline type-asserts on this surface and, when present
// with a usable partition key and a degree > 1, fans out; otherwise it
// falls through to the serial single-writer [RowWriter.WriteRows] — never
// silently doing nothing.
type ParallelCopyWriter interface {
	RowWriter

	// WriteRowsParallel runs the PLAIN batched-INSERT copy over
	// len(workers) concurrent workers, each consuming one of the supplied
	// per-worker channels and pinning its own target connection. Returns
	// only after every worker has drained and durably committed, or the
	// first worker error (loudly), or the ctx error on cancel.
	// len(workers) == 1 is equivalent to WriteRows on that one channel.
	WriteRowsParallel(ctx context.Context, table *Table, workers []<-chan Row) error
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
// MySQL implements it too (ADR-0065 / ADR-0091 F7c): its binlog
// TableMapEvent decoder re-reads information_schema on schema-change
// boundaries so COLUMNS already match the SchemaReader's, but CHECK
// constraints are never re-read at the boundary, and the VStream
// flavor's FieldEvent projection additionally omits PrimaryKey /
// Indexes and per-column charset/collation.
//
// The normalization is a comparison LENS and the pipeline intercepts
// apply it to BOTH sides of every classifier comparison — the
// cold-start seed at synthesis AND each CDC-projected snapshot at
// intake (pipeline.normalizeSnapshotForComparison). Normalizing only
// the seed against a raw CDC post is the TRIAGE-#3 phantom-alter
// regression shape: any change to a projection's representation on
// either end silently breaks the one-sided equality.
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

// TableReadPreflighter is the optional surface a [SchemaReader] can
// implement when some tables it returned are KNOWN-DOOMED to refuse at
// read time (a flat-file reader whose dump carries a table in an
// unsupported encoding, say). The pipeline consults it AFTER the
// include/exclude table filter and BEFORE any DDL or data moves, so an
// --exclude-table'd doomed table never blocks the rest of the run
// (Bug 188), while an INCLUDED doomed table still refuses loudly up
// front — not mid-migration after other tables copied.
//
// PreflightTableRead returns nil for a table it can read (or does not
// know), and the loud, remedy-bearing refusal otherwise. Readers that
// can always read every table they returned simply don't implement
// this.
type TableReadPreflighter interface {
	PreflightTableRead(table string) error
}

// ShardDiscoverer is the optional surface a source [Engine] can
// implement to report the source's shard layout. A sharded source — a
// Vitess/PlanetScale keyspace fronted by vtgate, which transparently
// merges every shard into one logical stream — returns one entry per
// shard; a non-sharded source returns nil/empty.
//
// The orchestrator's cross-shard-collision preflight (Bug 152) consults
// it: a source with >1 shard merging into a single non-discriminated
// target table whose rows can collide on a key would silently overwrite
// per-shard rows that share a key value. The preflight refuses that
// configuration unless the operator adds a discriminator
// (--inject-shard-column, ADR-0048) or explicitly opts in
// (--allow-cross-shard-merge).
//
// Engines that don't implement it are treated as a single logical
// (non-sharded) source, so the guard is a no-op for vanilla MySQL and
// Postgres. Implementations connect to the source to enumerate shards;
// a non-sharded service variant (e.g. the vanilla MySQL flavor) should
// return (nil, nil) WITHOUT connecting, so the guard stays free for the
// common case.
type ShardDiscoverer interface {
	DiscoverShards(ctx context.Context, dsn string) ([]string, error)
}

// DSNValidator is the optional surface an [Engine] implements to reject
// a DSN it can tell — from the DSN string alone, with no connection —
// is misconfigured for this engine BEFORE any work begins. The
// orchestrator calls it for the source and the target at the very top
// of migrate and sync, so a driver/host mismatch fails loudly up front
// instead of obscurely mid-run.
//
// Engines that don't implement it are a silent no-op (the common case).
// The vanilla MySQL flavor uses it to catch a PlanetScale endpoint
// (*.connect.psdb.cloud / *.private-connect.psdb.cloud): the vanilla
// flavor's binlog CDC and LOAD DATA cold-copy are both blocked by
// Vitess/PlanetScale, so --source-driver / --target-driver mysql
// against a PlanetScale host fails obscurely partway through the copy —
// ValidateDSN turns that into an up-front refusal recommending the
// `planetscale` driver.
//
// The returned error is role-AGNOSTIC: ValidateDSN doesn't know whether
// the DSN it was handed is the source or the target, so it names the
// host and the reason without naming a flag. The orchestrator prefixes
// the role ("source"/"target") and the exact --source-driver /
// --target-driver flag when it surfaces the refusal.
type DSNValidator interface {
	ValidateDSN(dsn string) error
}

// CDCUnsupportedExplainer is the optional surface an [Engine] whose
// [Capabilities.CDC] is [CDCNone] implements to supply the operator-
// facing refusal for CDC-requiring modes (sync start, backup
// stream/incremental, mid-stream add-table) — typically a coded error
// naming WHY this engine flavor has no CDC yet and what the
// alternatives are — instead of the orchestrator's generic "declares
// CDC=None" message.
//
// ExplainCDCUnsupported returns nil when the engine has no flavor-
// specific story (the orchestrator falls back to its generic refusal),
// so a multi-flavor engine implements the method once and answers only
// for the flavors that need it. No engine currently implements it — the
// mysql `mariadb` flavor did in Phase 1, but MariaDB CDC shipped in
// Phase 3 (ADR-0170); the surface remains for a future CDCNone flavor.
//
// The orchestrator consults it ONLY after Capabilities().CDC == CDCNone
// — a non-nil return never overrides a real CDC declaration.
type CDCUnsupportedExplainer interface {
	ExplainCDCUnsupported() error
}

// CDCScopePreflighter is the optional source-engine surface consulted
// before a set of tables enters an ACTIVE CDC stream's scope mid-stream
// (the `schema add-table` flow). It returns a loud error when a table
// carries a column the engine's CDC decode path cannot faithfully stream
// — the mid-stream analogue of the per-engine preflight the CDC reader
// runs at stream start (which the live stream already passed and does not
// re-run when add-table extends its scope).
//
// The mysql engine's `mariadb` flavor is the current implementer: a
// native uuid/inet column reads correctly under bulk `migrate` but its
// binlog CDC value-decode is not yet implemented (ADR-0170), and a
// MySQL-family target would SILENTLY accept the mis-decoded value — so
// add-table refuses it with the same coded error the stream-start
// preflight uses. Engines whose CDC path handles every column they read
// need not implement it; nil error = every table is streamable.
type CDCScopePreflighter interface {
	PreflightCDCScope(ctx context.Context, tables []*Table) error
}

// SourceHostAdvisory is one operator advisory a [SourceHostAdvisor]
// derives from a source DSN's host pattern. Message is the full
// WARN-level line (host + hazard + remedy, self-contained); Hint is
// the concise remedy the orchestrator attaches as a structured `hint`
// attribute on the log record.
type SourceHostAdvisory struct {
	Message string
	Hint    string
}

// SourceHostAdvisor is the optional surface an [Engine] implements to
// emit operator advisories it can derive from the SOURCE DSN's host
// alone, with no connection — the WARN-level sibling of
// [DSNValidator]'s refusal. The orchestrator calls it for the source
// at the top of migrate, sync, and the backup CDC paths and logs each
// returned advisory at WARN; the run proceeds.
//
// It exists for managed-service host classes where the hazard is real
// but not certain enough (or not fatal enough) to refuse:
//
//   - The Postgres engine warns when the host matches a known
//     connection-pooler pattern (Neon `-pooler`, Supabase Supavisor,
//     pgbouncer): most poolers strip the replication startup
//     parameter (so CDC typically needs the direct endpoint), and
//     long-lived snapshot transactions can exhaust the pool mid-copy.
//   - The MySQL engine warns on DigitalOcean and Vultr Managed MySQL
//     hosts when cdc is true: both platforms purge binlogs out-of-band
//     minutes after creation regardless of what
//     @@binlog_expire_logs_seconds reports, so the host pattern is
//     the ONLY reliable preflight signal (the variable lies; Vultr
//     additionally exposes no retention knob at all).
//
// cdc reports whether the run will anchor or consume a CDC position
// (sync, backup full/incremental/stream) — advisories about
// change-stream retention are suppressed for a plain migrate, which
// never returns to the source's log. Engines that don't implement the
// surface are a silent no-op.
type SourceHostAdvisor interface {
	SourceHostAdvisories(dsn string, cdc bool) []SourceHostAdvisory
}

// SourceProbedAdvisor is the connection-probing sibling of
// [SourceHostAdvisor], for managed-host classes where the ground truth
// is QUERYABLE in-session — detection beats pattern-guessing: a host
// gate only decides WHETHER to probe (so non-matching hosts cost
// nothing), and the probe result decides WHAT to say, so a correctly
// configured host stays silent instead of collecting a blind WARN on
// every run. Contrast [SourceHostAdvisor]'s DigitalOcean advisory,
// which MUST warn unconditionally because DO's retention truth is not
// SQL-visible; AWS RDS MySQL exposes its real retention via
// mysql.rds_configuration, so its advisory probes first. The gate need
// not be a host PATTERN: Google Cloud SQL has no DNS suffix at all
// (bare IP, or the auth proxy at localhost), so the MySQL engine gates
// that probe on the host SHAPE and fingerprints the platform in-band
// (@@version).
//
// The orchestrator calls it from the same chokepoint as
// SourceHostAdvisories, with the same cdc gate. A probe failure must
// degrade to a conservative advisory (or silence), never block the
// run. Engines that don't implement the surface are a silent no-op.
type SourceProbedAdvisor interface {
	SourceProbedAdvisories(ctx context.Context, dsn string, cdc bool) []SourceHostAdvisory
}

// ReshardReopener is the optional surface a [CDCReader] implements to
// follow a source reshard (a Vitess shard split / merge / MoveTables)
// without losing or duplicating events across the seam (ADR-0094).
//
// A VStream reader detects a reshard as a vtgate JOURNAL event, stops the
// stream cleanly, and caches a terminal error carrying the NEW shard
// layout with journal-stamped GTIDs (no gap / no overlap at the cut).
// After the change channel closes, the orchestrator calls
// ReopenAfterReshard, which inspects the reader's own cached [Err]:
//
//   - If it is a reshard signal, the reader rebuilds the stream against
//     the new layout from the journal GTIDs and returns a fresh change
//     channel with ok=true, err=nil.
//   - If the terminal error is NOT a reshard, returns ok=false (the
//     caller handles it as a normal terminal/retriable error, unchanged).
//   - A reshard whose reopen itself fails returns ok=true with a non-nil
//     error (the caller surfaces it loudly; it must not be swallowed).
//
// Keeping detect-and-reopen behind one call lets the engine-specific
// typed reshard error stay private to the engine. Engines without it
// (binlog MySQL, Postgres) are a silent no-op — reshard is a Vitess-only
// concept. The contract is read-then-act on the reader's own cached
// state, so it takes no error argument.
type ReshardReopener interface {
	ReopenAfterReshard(ctx context.Context) (<-chan Change, bool, error)
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

// SnapshotExporter is the OPTIONAL engine interface for exporting a
// plain-SQL shareable snapshot with NO replication machinery — the
// migrate-path counterpart of the slot-anchored snapshot inside
// [Engine.OpenSnapshotStream]. The orchestrator type-asserts for it
// (together with [SnapshotImporterOpener]) when `sluice migrate` wants
// its parallel bulk-copy readers pinned to one consistent MVCC view;
// engines without a shareable snapshot (MySQL — per-session REPEATABLE
// READ with no exportable name; SQLite) omit it and migrate keeps its
// documented independent-per-connection readers (the ADR-0019 v1
// window). Postgres implements it via BEGIN REPEATABLE READ READ ONLY +
// pg_export_snapshot(), which needs no replication privilege and works
// on any primary (pg_export_snapshot() is unavailable during recovery,
// so a hot-standby source surfaces an error and the caller falls back
// loudly).
type SnapshotExporter interface {
	ExportSnapshot(ctx context.Context, dsn string) (*ExportedSnapshot, error)
}

// CDCReader streams [Change] events from a source database starting at
// the given Position. Engines whose [Capabilities.CDC] is [CDCNone]
// return a non-nil error for any call to this interface.
type CDCReader interface {
	StreamChanges(ctx context.Context, from Position) (<-chan Change, error)
}

// ChangeLogPruner is the OPTIONAL capability a trigger-CDC source
// (sqlite-trigger / d1-trigger / pgtrigger) implements so the streamer can
// AUTO-PRUNE its consumed `sluice_change_log` in-stream (ADR-0137 Phase B,
// Bug 165). The trigger engines capture every source change and never reap it,
// so the change-log grows unbounded for the life of a continuous sync; the
// streamer's auto-prune sidecar calls this on a cadence to bound that growth.
//
// The argument is the TARGET's durably-persisted CDC position TOKEN (read by
// the sidecar from the applier's cdc-state — the durably-applied frontier, the
// ONLY safe lower bound). The engine decodes it with its OWN position codec
// (reusing its `AppliedLastID`), computes `cut = appliedLastID - keep`, and
// DELETEs `sluice_change_log` rows with `id <= cut`. Keeping the decode inside
// the engine keeps the streamer engine-neutral — it never sees the position
// codec. `keep` is a belt-and-suspenders safety margin (rows kept below the
// frontier); a non-positive cut is a safe no-op (0 deleted, nil error). It
// returns the number of rows deleted.
//
// The load-bearing safety contract (ADR-0137): the token MUST be the target's
// durably-applied frontier, NEVER the source reader's read cursor (which runs
// ahead) — pruning above the durable frontier would delete not-yet-applied
// rows and cause silent loss on warm-resume. Passing a FOREIGN (non-trigger-CDC)
// token is refused loudly by the engine's `AppliedLastID` decode.
//
// A source that doesn't implement this (vanilla PG/MySQL/vitess — they have no
// change-log) is a typed-nil no-op: the sidecar type-asserts and simply does
// not run.
type ChangeLogPruner interface {
	PruneConsumedChangeLog(ctx context.Context, durablePositionToken string, keep int64) (deleted int64, err error)
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

	// RowsApplied is the LIFETIME cumulative count of row-level DML
	// changes (INSERT + UPDATE + DELETE — see [IsRowDMLChange]) the
	// applier has durably applied to the target for this stream since
	// the control row was created. It is incremented in the SAME
	// write that advances the stream position (ADR-0007), so a partial
	// apply can never inflate it: a batch (or lane) that fails or rolls
	// back before its commit contributes nothing (the increment rides
	// the same tx as the position and rolls back with it), and on a
	// crash between a data commit and its position write BOTH the
	// position and this counter stay at the last durable boundary.
	// TRUNCATE, SchemaSnapshot, and transaction markers are NOT
	// counted.
	//
	// The count is of row-changes APPLIED, at-least-once across a
	// warm-resume: CDC redelivers a bounded tail of already-applied
	// changes from the persisted position, and those idempotent
	// re-applies are counted again — so after a resume the lifetime
	// total can slightly exceed the number of DISTINCT source changes.
	// That is honest (the target genuinely received those applies) and
	// consistent with the apply path's at-least-once resume model; it
	// is never an UNDER-count, and it never counts a change the target
	// did not commit.
	//
	// Zero for engines that don't persist it, for legacy rows that
	// pre-date the additive rows_applied column (COALESCE(…, 0) — the
	// count starts from 0 at the first post-upgrade position write,
	// which is honest: pre-upgrade applies were never tracked), and
	// for a freshly-created stream that has not yet applied a row.
	RowsApplied int64
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

// RowFilterSetter is the optional surface a [RowReader] or a verify-side
// [SchemaReader] (an [ir.Verifier]) can implement to accept an operator-
// supplied per-table row-level filter (`--where TABLE=<predicate>`,
// ADR-0173 Phase 1). The predicate is native SOURCE-SQL, evaluated on
// the source — sluice pushes it into the read query as one more
// conjunct so only matching rows are copied/counted; there is no client-
// side row filtering.
//
// The map is keyed by SOURCE table name (a `--map-database`/`--map-schema`
// rename still matches on the original name, exactly like `--redact`).
// The value is the raw predicate; the engine ALWAYS wraps it in
// parentheses when composing it into the WHERE so a disjunctive predicate
// (`a OR b`) can't escape the chunk/keyset bounds it is ANDed with.
//
// The pipeline threads it onto every source reader a `migrate` run opens
// (the primary + each parallel chunk/table reader) via
// [migcore.ApplyRowFilters], and onto the SOURCE verifier for `verify`.
// Engines that don't implement it cause the pipeline to refuse `--where`
// loudly rather than silently copy every row — the loud-failure tenet.
//
// MySQL and Postgres implement it (on both RowReader and SchemaReader);
// SQLite/D1/flat-file sources do NOT (v1), so `--where` against them is
// refused. Phase 1 is migrate-only; `sync` does NOT thread it (a filtered
// snapshot with an unfiltered CDC leg would be inconsistent — Phase 2).
type RowFilterSetter interface {
	// SetRowFilters records the source-name-keyed predicate map. An empty
	// or nil map disables filtering (the default). Implementations store
	// it and consult filters[table.Name] at read/count time.
	SetRowFilters(filters map[string]string)
}

// FilteredCDCPreflighter is the optional source-engine surface that a
// continuous filtered sync (`sync --where`, ADR-0173 Phase 2) uses to
// verify — at sync-start — that the source is configured to deliver full
// row BEFORE-images for the filtered tables. Client-side row-move
// evaluation (an UPDATE that moves a row into / out of the filter's scope
// becomes a target INSERT / DELETE) requires the before-image, so a
// filtered CDC stream effectively requires MySQL binlog_row_image=FULL /
// PG REPLICA IDENTITY FULL. tables are the SOURCE table names carrying a
// `--where` predicate.
//
// Implementations return a coded refusal
// ([sluicecode.CodeWhereCDCBeforeImage]) naming the offending table + the
// exact remedy when the requirement is not met, and nil when it is (or is
// not applicable — a plain, empty-tables call is a no-op). MySQL and
// Postgres implement it; a source engine that does NOT is refused loudly
// by the pipeline (filtered continuous sync is v1-scoped to those two).
type FilteredCDCPreflighter interface {
	PreflightFilteredCDCBeforeImage(ctx context.Context, dsn string, tables []string) error
}

// FullBeforeImageSetter is the optional CDC-reader surface a filtered
// continuous sync (`sync --where`, ADR-0173 Phase 2) uses to request
// UN-narrowed before-images for the filtered tables. A CDC reader normally
// narrows the before-image to identity-key columns (Bug-8/88/92); for the
// named tables it must instead emit the FULL decoded old tuple so the
// client-side row-move evaluation can read every OLD column the `--where`
// predicate references, after which the pipeline re-narrows to the key
// columns before the applier builds its WHERE.
//
// The MySQL binlog + Postgres pgoutput CDCReaders (and the VStream readers)
// implement it; a reader that does NOT is refused loudly by the pipeline
// (silently accepting a PK-narrowed before-image would mis-classify a
// move-OUT and leak an out-of-scope row on the target).
//
// It lives in `ir` — not only in the pipeline — so the concrete engine
// readers can pin their conformance at compile time
// (`capabilities_assert.go`, audit 2026-07-18 M-A2): a rename or
// re-signature of SetFullBeforeImageTables would otherwise keep `go build`
// green while flipping every filtered sync on that engine to a runtime
// refuse. The pipeline's own optional-interface assertion is an alias of
// this type.
type FullBeforeImageSetter interface {
	SetFullBeforeImageTables(tables map[string]bool)
}

// ServerSideCDCFilterSetter is the optional CDC-reader surface a WARM-RESUME
// continuous filtered sync (`sync --where`, ADR-0174 Piece 2) uses to push the
// operator's `--where` predicates into the reader's SERVER-SIDE stream filter,
// so a resumed stream is reduced at the source instead of pulling the whole
// keyspace and discarding ~99% of it client-side after every restart/crash-
// resume (audit 2026-07-18 F-P1).
//
// It is a PERFORMANCE surface, not a correctness one: the pipeline's
// client-side row-move classification (the ADR-0173 route()) still runs on
// every delivered change regardless — the server-side filter only reduces what
// is delivered. So a reader that does NOT implement this interface (the MySQL
// binlog + Postgres pgoutput CDC readers, which have no server-side stream
// filter) is a silent no-op: the resume is correct, merely unfiltered at the
// source. Only the VStream (PlanetScale / Vitess) CDC reader implements it,
// mirroring the cold-start snapshot's server-side push-down
// ([FilteredSnapshotOpener]).
//
// filters maps SOURCE table name → the native-SQL `--where` predicate (the
// same map [RowFilterSetter] carries on the migrate leg). The reader wraps each
// in a `select * from <t> where (<pred>)` filter rule; tables with no entry
// keep streaming unfiltered. Like [FullBeforeImageSetter] it lives in `ir` so
// the concrete VStream reader can compile-assert conformance
// (capabilities_assert.go) — a rename/re-signature would otherwise silently
// drop the warm-resume server-side reduction (a perf regression) while
// `go build` stays green.
type ServerSideCDCFilterSetter interface {
	SetServerSideRowFilters(filters map[string]string)
}

// ClientCopyFilterSetter is the optional SNAPSHOT-reader surface the filtered-
// sync orchestrator uses to apply a CLIENT-side row filter to the cold-start
// COPY of specific tables — the ones whose `--where` predicate the reader's
// SERVER-side filter cannot reproduce faithfully, so the orchestrator streams
// them UNFILTERED server-side and filters them HERE instead (audit 2026-07-19
// A0: a PAD-SPACE-collation column under the VStream NO-PAD server-side filter
// would otherwise silently drop the trailing-space rows the PAD-faithful client
// keeps — so it is refused today until this fallback lands).
//
// It is the cold-start COPY analogue of the CDC leg's route(): keep is the same
// PAD-faithful client predicate, applied to each decoded COPY row as it leaves
// the reader; keep(table, row)==false drops the row. The orchestrator returns
// true for the tables it still filtered SERVER-side, so keep only ever narrows
// the unfiltered-server-side tables. Only the VStream snapshot reader implements
// it; other engines filter the snapshot faithfully at the source (native-SQL
// push-down) and never need it — the orchestrator refuses loudly if it has a
// client filter to install but the reader lacks this surface, rather than
// silently over-copying.
type ClientCopyFilterSetter interface {
	SetClientCopyFilter(keep func(table string, row Row) bool)
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

// IncrementalIndexBuilder is the OPTIONAL surface a [SchemaWriter] can
// implement to build each table's secondary indexes AS SOON AS its bulk
// copy finishes, concurrently with the still-copying tables, instead of
// in one whole-schema sweep after every table's copy completes (ADR-0077,
// roadmap item 3b(a)). pgcopydb hides its deferred-index phase this way;
// at scale (a 110 GB / 43-table corpus) sluice's separate post-copy index
// phase was a sequential ~457 s tail — 29% of the total wall.
//
// The orchestrator runs the copy pool and a consumer of this surface
// under ONE errgroup: the copy pool invokes a per-table callback as each
// table's copy returns nil, the orchestrator forwards the completed
// *Table onto completedTables, and BuildTableIndexesFromChannel drains
// that channel with the writer's OWN bounded, budget-aware build pool. A
// copy error cancels the build pool via the shared ctx and vice versa.
//
// Contract:
//
//   - The orchestrator closes completedTables exactly once, after the
//     copy pool finishes (and only on copy success — an error path cancels
//     the ctx instead). The implementation returns nil once the channel
//     is closed AND every queued build has finished, or the first build
//     error (which must cancel its peers).
//   - Each *Table arrives at most once. The implementation builds only
//     that table's secondary indexes (the same set the whole-schema
//     [SchemaWriter.CreateIndexes] would build for it), with
//     `CREATE INDEX IF NOT EXISTS` so a resume that re-feeds an
//     already-indexed table is a no-op.
//   - The build pool's connection budget is the value passed via
//     [IndexBuildBudgetSetter.SetIndexBuildBudget] (the slice the
//     combined copy+index split reserved for the index axis), NOT a fresh
//     self-probe — copy connections are open SIMULTANEOUSLY now, so a
//     self-probe would double-count the budget. When no budget was set
//     (0), the implementation may fall back to its self-probe (the
//     non-overlapped path).
//
// PG implements it (its index-build worker pool predates this). MySQL
// does NOT: the orchestrator detects the missing surface, drains
// completedTables into a no-op, and calls the whole-schema
// [SchemaWriter.CreateIndexes] AFTER the copy completes — exactly the
// pre-ADR-0077 behaviour.
type IncrementalIndexBuilder interface {
	BuildTableIndexesFromChannel(ctx context.Context, s *Schema, completedTables <-chan *Table) error
}

// TableIndexedNotifier is the OPTIONAL companion to
// [IncrementalIndexBuilder] (ADR-0077): the builder calls the registered
// callback ONCE per table, after that table's LAST secondary index has
// built (or immediately for a table with no secondary indexes). The
// pipeline orchestrator uses it to flip [TableProgress.IndexesBuilt] so a
// resume can fully skip an already-indexed table. The callback may run
// from any build worker goroutine, so the pipeline's implementation
// serialises its state write on the shared stateMu.
//
// Set BEFORE [IncrementalIndexBuilder.BuildTableIndexesFromChannel]; nil
// (the default) is a no-op (indexes still build, IndexesBuilt just isn't
// recorded — a crash then re-feeds every table on resume, harmless under
// IF NOT EXISTS). PG implements it.
type TableIndexedNotifier interface {
	SetTableIndexedCallback(fn func(table *Table))
}

// IndexBuildBudgetSetter is the OPTIONAL setter the orchestrator uses to
// hand an [IncrementalIndexBuilder] the connection budget reserved for
// the index axis when copy and index builds run SIMULTANEOUSLY (ADR-0077).
//
// The combined copy+index budget is split once at the single chokepoint
// (see pipeline.splitCopyAndIndexBudget): the index axis gets a small
// slice, the copy axis the rest, and their sum never exceeds the measured
// budget. The reserved slice is passed here so
// [IncrementalIndexBuilder.BuildTableIndexesFromChannel] sizes its pool
// from it instead of self-probing (which would double-count connections
// the copy pool already holds open).
//
// Zero is the sentinel for "no reserved budget" — the overlap path was
// not engaged (no measured ceiling / MySQL / degraded probe), so the
// implementation keeps its self-probe. Negative is treated as zero.
//
// PG implements it; MySQL does not (it has no [IncrementalIndexBuilder]).
type IndexBuildBudgetSetter interface {
	SetIndexBuildBudget(connBudget int)
}

// IndexVerifier is the OPTIONAL loud-failure safety net a [SchemaWriter]
// implements so the orchestrator can VERIFY, immediately after the index
// phase, that every secondary index the build was supposed to create
// actually exists on the target — and refuse loudly (naming the missing
// `table.index` list) if any is absent.
//
// This exists because a silent index-build no-op is the project's #1
// tenet violation (silent schema loss): a MySQL-target migrate/sync into a
// PlanetScale/Vitess endpoint once drained the completed-tables channel
// without building ANY secondary index, reporting success against a schema
// that was missing every non-primary index. VerifyIndexes turns that whole
// CLASS into a named refusal so no future refactor can re-break the build
// path unnoticed. It runs for ALL targets that implement it, on both the
// migrate and sync cold-start paths, and is cheap — one catalog read per
// expected index.
//
// The verified set MUST be the SAME eligible set the build targeted (the
// inline-emitted indexes a CREATE TABLE already carries are excluded, so
// they are never falsely flagged). A writer that does not implement the
// surface is a no-op — the net simply doesn't run for that engine.
type IndexVerifier interface {
	VerifyIndexes(ctx context.Context, s *Schema) error
}

// IndexBuildFallback is the OPTIONAL out-of-band channel a [SchemaWriter]
// routes a deferred secondary-index build through when the target refuses
// (or would refuse) the direct `ALTER … ADD INDEX` — today the PlanetScale
// deploy-request fallback for the errno-3024 statement-time wall and the
// errno-1105 safe-migrations direct-DDL block (ADR-0148, roadmap item 67).
//
// The contract is deliberately engine-vocabulary-only: table names the
// target table, ddls carries the writer's OWN emitted DDL statements for
// every still-pending index of that table (one call per table, so the
// implementation can batch them into one control-plane operation), and
// cause is the direct attempt's failure — nil when the writer routed the
// table preemptively (e.g. a size probe says the direct attempt is doomed).
// The implementation must build the indexes durably before returning nil;
// the orchestrator's [IndexVerifier] net re-checks them afterwards either
// way.
//
// An implementation that discovers it cannot engage (a control-plane
// prerequisite is missing — no safe migrations, bad credentials) returns
// an error wrapping [ErrIndexBuildFallbackUnavailable]; the writer then
// falls back to the pre-fallback behaviour — surfacing the ORIGINAL direct
// error (with its existing operator hint) for a failed attempt, or running
// the direct attempt after all for a preemptive route — so the fallback
// can never make a run fail in a way the status quo would not have.
type IndexBuildFallback interface {
	BuildIndexDDL(ctx context.Context, table string, ddls []string, cause error) error
}

// ErrIndexBuildFallbackUnavailable is the sentinel an [IndexBuildFallback]
// wraps when its prerequisites don't hold (safe migrations disabled on the
// production branch, an unusable service token, …). Writers match it with
// errors.Is and revert to the direct-attempt behaviour; the wrapping error's
// text carries the human-readable reason for the writer's WARN.
var ErrIndexBuildFallbackUnavailable = errors.New("index-build fallback unavailable")

// IndexBuildFallbackSetter is the OPTIONAL setter the orchestrator uses to
// thread a configured [IndexBuildFallback] onto a freshly-opened target
// [SchemaWriter] before any index phase runs. Engines without the surface
// skip cleanly; a nil fallback (the zero value every non-CLI construction
// gets) leaves the writer's behaviour byte-identical to before the
// fallback existed. MySQL implements it (the PlanetScale flavor is the
// only current consumer); PG has no statement-time-wall class to route.
type IndexBuildFallbackSetter interface {
	SetIndexBuildFallback(f IndexBuildFallback)
}

// TableAnalyzer is the OPTIONAL surface a [SchemaWriter] implements so
// the migrate orchestrator's opt-in `--analyze-after` phase can refresh
// the target's planner statistics per migrated table once constraints
// and views are in place (perf research delta 4: a freshly bulk-loaded
// table has stale/empty statistics, so the first post-cutover queries
// plan badly until an autovacuum/background ANALYZE catches up —
// pgcopydb runs a per-table VACUUM ANALYZE by default for the same
// reason).
//
// The statement is engine-dialect-owned, exactly like every other DDL
// surface: PG `ANALYZE <schema>.<table>`, MySQL `ANALYZE TABLE <table>`
// (which reports failures in its result SET, not as an exec error — the
// implementation must surface those loudly), SQLite `ANALYZE <table>`.
//
// The phase is ADVISORY: it runs after the migration's data and DDL are
// durably complete, so the orchestrator WARNs per failed table and never
// fails the run on an analyze error. Engines without the surface skip
// the phase with one loud WARN (the operator asked for it explicitly).
// All three shipping target engines implement it.
type TableAnalyzer interface {
	AnalyzeTable(ctx context.Context, table *Table) error
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
//
// rename (ADR-0142) is the OPTIONAL per-namespace source → target rename:
// when non-nil, the applier maps each change's source namespace through it
// to derive the TARGET namespace it qualifies the table reference with. nil
// is the identity default (target == source, byte-identical to the original
// same-named routing). Crucially the rename is applied INSIDE the routing
// step only — the change's own [Change.Schema] is NOT rewritten, so anything
// keyed on the source namespace (notably source-qualified --redact rules)
// keeps matching on the source name while only the write lands in the
// renamed target namespace.
type MultiDatabaseRouter interface {
	SetMultiDatabaseRouting(enabled bool, rename func(sourceNamespace string) string)
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

// ServerCDCReaderOpener is the OPTIONAL engine surface for opening a
// SERVER-WIDE CDC reader against a database-optional DSN — the
// snapshot-less counterpart of [MultiDatabaseSnapshotOpener] that a
// multi-database `sync start` WARM-RESUME needs (ADR-0074 Phase 1b.3).
//
// A multi-database cold-start opens its CDC reader as the [SnapshotStream]'s
// Changes (server-wide, paired with the spanning snapshot). A WARM-RESUME
// has a persisted server-wide binlog position and must NOT re-cold-start:
// it opens a bare server-wide CDC reader, scopes it to the selected
// database set via [CDCDatabaseScoper.SetCDCDatabaseScope], and resumes
// [CDCReader.StreamChanges] from the one persisted position — exactly the
// CDC wiring the cold-start does at handoff, minus the snapshot.
//
// The returned reader's bound database is empty (the binlog is
// server-wide); the selected-database set is supplied separately via
// [CDCDatabaseScoper]. dsn is a *server* connection — its database
// component may be empty (the operator drove a multi-database run without
// naming one). Only engines whose CDC stream is server-wide (MySQL binlog)
// implement it; the VStream flavors return an [ErrNotImplemented]-shaped
// error (keyspace-scoped CDC is the Phase 1c N-stream design).
type ServerCDCReaderOpener interface {
	OpenServerCDCReader(ctx context.Context, dsn string) (CDCReader, error)
}

// NamespaceFolder is the OPTIONAL surface a TARGET engine implements to
// report how it would FOLD a source namespace name into its own
// namespace identifier (ADR-0075 resolved decision #1). It exists for
// exactly one safety check: a PG → MySQL multi-schema fan-out routes each
// source schema to a same-NAMED MySQL database, but MySQL database names
// fold per the server's `lower_case_table_names` setting, whereas PG
// schema names are case-sensitive. Two distinct source schemas (e.g.
// `Sales` and `sales`) would therefore collide into ONE MySQL database —
// a silent merge of two namespaces' data. The orchestrator's
// multi-namespace pre-flight calls FoldNamespace on every selected source
// namespace and refuses LOUDLY when two distinct source names fold to the
// same target identifier (or a name is otherwise unsafe), naming both
// schemas (never a silent merge — the loud-failure tenet).
//
// FoldNamespace returns the target-side identifier `name` would land
// under. MySQL queries `@@lower_case_table_names` once and lowercases the
// name when the server folds (the common Linux default lct=0 is
// case-sensitive — identity — so on such servers the collision check is a
// no-op). Engines whose namespace identifiers are case-sensitive and
// never fold (Postgres) DON'T implement this surface; the orchestrator
// treats a missing surface as identity (no fold, no collision possible
// from folding). The dsn names the target server so the implementation
// can probe the live setting.
type NamespaceFolder interface {
	FoldNamespace(ctx context.Context, dsn, name string) (string, error)
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
// PlatformNote, when non-empty, marks the slot as PLATFORM-INTERNAL —
// a slot the managed provider itself owns (Neon's wal_proposer_slot,
// the Aiven-lineage pghoard_local) — and carries the short provider
// description for display. Enumeration surfaces label such slots
// instead of leaving them to read as leaked consumers, and Drop
// refuses them without --force.
type SlotInfo struct {
	Name              string
	Plugin            string
	Active            bool
	WALStatus         string
	RestartLSN        string
	ConfirmedFlushLSN string
	PlatformNote      string
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

	// IndexesBuilt records whether this table's secondary indexes have
	// finished building (ADR-0077, index-build overlap). It is set true
	// only after ALL of the table's secondary indexes have been built —
	// the index-overlap consumer flips it once the table's
	// [IncrementalIndexBuilder] queue drains. On resume the orchestrator
	// reads it alongside State:
	//
	//   - State=complete && !IndexesBuilt → copy is skipped (the data
	//     landed) but the table is re-fed to the index pool so its
	//     indexes finish; CREATE INDEX IF NOT EXISTS guards a crash that
	//     happened mid-index-build.
	//   - State=complete && IndexesBuilt   → the table is fully skipped.
	//
	// Additive on the JSON wire: it is omitempty, so an old state row
	// (which never wrote the key) decodes to false — read as "copy done,
	// indexes not yet built", which re-feeds the table to the index pool.
	// That is the safe interpretation: re-building an already-built index
	// is a no-op under IF NOT EXISTS, whereas the inverse (treating an
	// absent flag as "built") would silently skip a never-built index.
	// Engines without an [IncrementalIndexBuilder] (MySQL) never set this;
	// their indexes build in the post-copy whole-schema CreateIndexes
	// fallback, so the flag stays its zero value and is harmless.
	IndexesBuilt bool
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

// MigrationState is the in-memory view of one migration's persisted
// state. Returned by [MigrationStateStore.Read] and accepted by
// [MigrationStateStore.Write].
//
// Wire shape on disk (engine-neutral, ADR-0082): a HEADER row in
// sluice_migrate_state plus one PROGRESS row per table in
// sluice_migrate_table_progress:
//
//	-- sluice_migrate_state (header; one row per migration)
//	migration_id    TEXT PRIMARY KEY
//	phase           TEXT NOT NULL
//	table_progress  TEXT          -- ≤v0.99.x: JSON map blob (state_format 1);
//	                              -- now: a deliberately-invalid-JSON sentinel
//	state_format    INT NOT NULL DEFAULT 1
//	started_at      TIMESTAMP NOT NULL
//	updated_at      TIMESTAMP NOT NULL
//	last_error      TEXT          -- truncated to 1KB on write
//
//	-- sluice_migrate_table_progress (one row per table)
//	migration_id    TEXT NOT NULL
//	table_name      TEXT NOT NULL
//	progress        TEXT NOT NULL -- one TableProgress JSON value
//	updated_at      TIMESTAMP NOT NULL
//	PRIMARY KEY (migration_id, table_name)
//
// Read merges the two back into the Go map; UpdatedAt carries the
// most recent of the header's and the progress rows' timestamps. A
// legacy single-blob row (state_format 1) is detected on Read and
// upgraded to per-table rows once, on the first WRITE (Read itself
// never writes, so it works under a read-only target user) — see
// internal/migratestate. nil
// TableProgress is fine — first-run writes before any table starts
// use an empty map.
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
//   - Read decides whether to start fresh, resume, or refuse — and
//     performs the one-time legacy-blob → per-table-rows upgrade
//     (ADR-0082) when it finds a ≤v0.99.x row.
//   - Write is called at every phase transition (a header-only
//     upsert).
//   - WriteTableProgress is called at every per-table bulk-copy
//     boundary and per-batch resume checkpoint (one progress-row
//     upsert — O(1) in table count).
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

	// Write upserts the header row plus any per-table entries present
	// in state.TableProgress. It never deletes progress rows absent
	// from the map — entries only accrue over a migration's life, so
	// a header-only Write (nil map; the phase-transition shape)
	// leaves previously-persisted per-table progress untouched. The
	// store sets updated_at to the current wall-clock time and
	// preserves started_at across updates (only the first Write for a
	// given migration_id sets it). LastError is stored verbatim;
	// callers should truncate before passing.
	Write(ctx context.Context, state MigrationState) error

	// WriteTableProgress upserts ONE table's progress row without
	// touching the header or any peer table's row — the O(1)
	// per-checkpoint write (ADR-0082). Callers must have called Read
	// or Write for migrationID first (the pipeline's loadOrInitState
	// guarantees it): Read performs the legacy-row upgrade that makes
	// per-table rows the authoritative progress source.
	WriteTableProgress(ctx context.Context, migrationID, tableName string, progress TableProgress) error

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

// ConnectionLabeler is the optional engine surface for engines that can
// stamp every connection they open with an operator-visible label
// carrying the run's stream-/migration-id, so operators can find a
// specific run's sessions in the server's activity views. The CLI
// type-asserts on it right after engine lookup and, when implemented,
// swaps in the labeled copy for the rest of the run; engines without a
// per-connection label concept simply omit the method.
//
// The label's wire form is engine-specific. Postgres carries it in
// application_name (`sluice/<role>/<id>`, visible in pg_stat_activity
// and matched by the stale-backend probe); a future engine may use its
// own connection attribute (e.g. MySQL's performance_schema
// session_connect_attrs).
//
// WithConnectionLabel returns a configured copy rather than mutating —
// engines are registered once and shared, so the registry's value must
// stay label-free for the next caller. An empty id is normalised by the
// engine to a stable fallback (Postgres: `sluice/<role>/-`) so the
// label format stays well-formed and greppable.
type ConnectionLabeler interface {
	WithConnectionLabel(id string) Engine
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

// FilteredSnapshotOpener is the optional engine surface that pushes a
// per-table `--where` predicate into the snapshot COPY at OPEN time
// (ADR-0174 Piece 2, continuous filtered `sync --where` on VStream
// sources). It is distinct from [RowFilterSetter] — which the pipeline
// applies to an already-open RowReader that filters lazily per read —
// because the VStream COPY sends its filter rules to vtgate when the
// stream is constructed: a post-open SetRowFilters cannot retroactively
// re-scope the in-flight COPY, so the predicate MUST be known before the
// stream opens. Engines whose snapshot RowReader filters lazily (Postgres,
// vanilla MySQL: `SELECT ... WHERE` per table) do NOT implement this — the
// pipeline's post-open [RowFilterSetter] gate covers them; only an
// eager-COPY source (VStream) needs the predicate at open.
//
// The pipeline prefers this surface whenever `--where` is set and the
// source implements it; it still runs the [RowFilterSetter] gate afterward
// (the loud-failure authority for a source that supports neither).
type FilteredSnapshotOpener interface {
	// OpenSnapshotStreamForTablesFiltered opens a table-scoped snapshot
	// (as [TableScopedSnapshotOpener.OpenSnapshotStreamForTables]) whose
	// COPY — and the streaming phase that follows it on the same stream —
	// evaluates rowFilters server-side. rowFilters maps SOURCE table name
	// to the operator's native-SQL predicate (already validated by the
	// restricted `--where` grammar); a table with no entry copies unfiltered.
	// The source engine evaluates the predicate with the SOURCE's own
	// collation, so it agrees with the pipeline's client-side CDC evaluation
	// by construction. An empty rowFilters is identical to the unfiltered
	// table-scoped open.
	OpenSnapshotStreamForTablesFiltered(ctx context.Context, dsn string, tables []string, rowFilters map[string]string) (*SnapshotStream, error)
}

// FilteredSnapshotResumer is the [FilteredSnapshotOpener] counterpart for
// resuming an INTERRUPTED filtered cold-start COPY (the ADR-0174 Piece 2
// intersection of [SnapshotStreamResumer] and the filter push-down). The
// resumed COPY must carry the SAME server-side predicate the interrupted
// run did — otherwise the resumed scan would copy out-of-scope rows, a
// silent leak — so, like [FilteredSnapshotOpener], the predicate is threaded
// at open time rather than via a post-open setter.
type FilteredSnapshotResumer interface {
	// OpenSnapshotStreamFromPositionFiltered resumes the bulk COPY from the
	// cursor carried by from (as
	// [SnapshotStreamResumer.OpenSnapshotStreamFromPosition]) with rowFilters
	// pushed server-side, same keying/semantics as
	// [FilteredSnapshotOpener.OpenSnapshotStreamForTablesFiltered].
	OpenSnapshotStreamFromPositionFiltered(ctx context.Context, dsn string, from Position, tables []string, rowFilters map[string]string) (*SnapshotStream, error)
}

// DefaultTableExcluder is the optional engine surface for "tables
// the operator almost never wants to migrate against this engine".
// Implementing engines return a list of [path.Match]-style patterns
// that the orchestrator merges into the operator's
// [migcore.TableFilter.Exclude] when the operator is in
// exclude-or-no-filter mode. Operator-supplied
// [migcore.TableFilter.Include] short-circuits the merge — if the
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

// InferredTypeValidator is the OPTIONAL surface a [SchemaReader] implements
// to support the opt-in, data-validated rich-type inference (`--infer-types`,
// ADR-0144). It is the engine-neutral seam the migrate orchestrator calls to
// ask "does EVERY non-NULL value in this column conform to this richer target
// type?" — the engine owns the SQL (one cheap aggregate pushed to the source,
// no row transfer); the pipeline owns the name-hint candidate selection and
// the override injection.
//
// Implemented today by SQLite's file [SchemaReader] and the live-D1 reader,
// the only dynamically-typed sources where the conservative INTEGER/TEXT
// mapping leaves a rich-type gap worth closing. A source that does not
// implement it is refused loudly when `--infer-types` is set (inference is
// SQLite/D1-only) rather than silently ignored.
//
// The inference adds NO new value-conversion code: a conforming column is
// promoted by injecting a validated `--type-override`-equivalent entry that
// rides the engine's existing override decode, which itself loud-refuses on
// any value it cannot faithfully interpret. So the up-front exhaustive
// validation and the decode's loud-refuse are two independent nets against
// silent corruption (ADR-0144 §"two independent nets").
type InferredTypeValidator interface {
	// ValidateInferredType reports whether every non-NULL value in
	// table.column conforms to target — exhaustively (a single stray
	// non-conformer fails it; a one-shot migration cannot tolerate one).
	//
	// target names the candidate FAMILY the pipeline selected by name-hint
	// ([Boolean], [Timestamp], [JSON]{Binary:true}, [UUID]). For the temporal
	// family the engine RESOLVES the concrete shape from the data — resolved
	// is [Timestamp]{WithTimeZone:true} iff EVERY value carries an explicit
	// offset/`Z`, else {WithTimeZone:false} (tz-aware, never inventing a
	// zone); for the other families resolved == target.
	//
	// validated is the count of non-NULL values checked. conforms is true
	// only when no value contradicted target AND validated > 0 (an all-NULL
	// or empty column validates nothing, so it is NOT promoted — there is no
	// evidence the rich type is correct). A query failure returns a non-nil
	// err and the column is left at its safe type.
	ValidateInferredType(ctx context.Context, table, column string, target Type) (conforms bool, resolved Type, validated int64, err error)
}

// BackfillSet is one `--set` clause of `sluice backfill` (ADR-0159):
// assign Column the value of Expr, evaluated per row by the database
// itself. Column is an identifier the engine quotes; Expr is a native
// SQL expression over the table's existing columns, emitted VERBATIM
// — a backfill runs inside ONE database, so there is no cross-dialect
// translation to do (the same posture `--expr-override` takes).
type BackfillSet struct {
	Column string
	Expr   string
}

// BackfillExecutor is the optional engine surface behind `sluice
// backfill` (ADR-0159): a same-database, keyset-chunked, online-safe
// in-place UPDATE. The orchestrator ([pipeline.Backfiller]) walks the
// table's primary key in bounded batches — NextChunkUpperBound
// discovers each batch's inclusive upper PK, ExecBackfillChunk issues
// one UPDATE clipped to that (after, upper] range — so every
// statement holds locks for at most one batch and stays under vendor
// statement-time walls (the PlanetScale errno-3024 class, ADR-0148).
//
// Both bound predicates are row-comparisons on the PK tuple
// (`(pk...) > (...)` / `<= (...)`), compared by the engine in the
// column's NATIVE collation — the same exactly-once contract
// [BoundedBatchedRowReader] pins (ADR-0096): the chunk walk and the
// chunk UPDATE must agree on one total order or a boundary-straddling
// row lands in no chunk.
//
// The operator's `--where` predicate scopes WHICH rows inside a chunk
// are updated (and, when it self-describes doneness — e.g.
// `new_col IS NULL` — makes re-runs and crash-replays idempotent);
// the PK range bounds the statement regardless. where is verbatim
// native SQL, like [BackfillSet.Expr].
//
// Engines without an in-place UPDATE surface (SQLite/D1 today) simply
// don't implement [BackfillExecutorOpener]; the orchestrator refuses
// them loudly (SLUICE-E-BACKFILL-UNSUPPORTED-ENGINE) rather than
// silently doing nothing.
type BackfillExecutor interface {
	// NextChunkUpperBound returns the PK tuple of the LAST row in the
	// next batch of up to limit rows whose PK is strictly greater than
	// after (nil after = start of table), in PK order, and ok=false
	// when no rows remain past after. The returned tuple is in PK
	// column declaration order, with each value normalized to a form
	// that re-binds into the engine's comparison predicates AND
	// round-trips the resume store's JSON encoding (e.g. []byte →
	// string, time.Time → the engine's native literal form).
	NextChunkUpperBound(ctx context.Context, table *Table, after []any, limit int) (upper []any, ok bool, err error)

	// ExecBackfillChunk issues ONE bounded UPDATE applying sets to the
	// rows in (after, upper] that also match where (empty where = the
	// whole range), and returns the driver-reported affected-row count.
	// after may be nil (first chunk: no lower bound); upper is required
	// — an unbounded UPDATE is exactly what this surface exists to
	// avoid.
	ExecBackfillChunk(ctx context.Context, table *Table, sets []BackfillSet, where string, after, upper []any) (int64, error)

	// BackfillStatement returns the chunk UPDATE exactly as
	// ExecBackfillChunk executes it mid-walk (both PK bounds present,
	// placeholders shown symbolically), for `--dry-run` preview. The
	// first chunk omits the lower-bound predicate.
	BackfillStatement(table *Table, sets []BackfillSet, where string) (string, error)

	// CountRemaining counts rows still matching where (the `--dry-run`
	// estimate and the progress/ETA total). Empty where counts all
	// rows. It also serves as the where-predicate preflight: an
	// unparsable predicate fails HERE, before any UPDATE runs.
	CountRemaining(ctx context.Context, table *Table, where string) (int64, error)

	// Close releases the underlying connection pool.
	Close() error
}

// BackfillExecutorOpener is the optional engine surface that exposes
// the in-place backfill executor. Same shape as
// [MigrationStateStoreOpener]: optional, type-asserted at the call
// site, so adding a new engine doesn't force every existing engine to
// grow a stub. MySQL (all flavors — PlanetScale/Vitess ride the same
// SQL path) and Postgres implement it.
type BackfillExecutorOpener interface {
	OpenBackfillExecutor(ctx context.Context, dsn string) (BackfillExecutor, error)
}

// ControlTableStatement names one sluice control table together with
// the exact CREATE statement the engine executes to create it.
type ControlTableStatement struct {
	// Table is the control table's unqualified name (e.g.
	// "sluice_cdc_state").
	Table string

	// DDL is the CREATE statement, byte-identical to what the engine's
	// own Ensure* path executes — single-sourced so the printed
	// bootstrap DDL can never drift from what sluice would create.
	DDL string
}

// ControlTableDDLProvider is the optional engine surface behind
// `sluice control-tables ddl`: it renders the CREATE statements for
// sluice's own control tables (migrate-state + cdc-state) so an
// operator can pre-create them through a governed channel when the
// target refuses direct DDL — the PlanetScale safe-migrations
// bootstrap (ship each statement via `sluice deploy-ddl`). Same shape
// as [MigrationStateStoreOpener]: optional, type-asserted at the call
// site. The MySQL family (all flavors) implements it.
type ControlTableDDLProvider interface {
	ControlTableDDL() []ControlTableStatement
}
