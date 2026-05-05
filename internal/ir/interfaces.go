package ir

import (
	"context"
	"time"
)

// SchemaReader extracts an IR [Schema] from a live database.
type SchemaReader interface {
	ReadSchema(ctx context.Context) (*Schema, error)
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
}

// RowReader streams rows from a single table for the bulk-copy phase.
// Implementations should close the returned channel when the table is
// fully read, and return promptly when ctx is cancelled.
type RowReader interface {
	ReadRows(ctx context.Context, table *Table) (<-chan Row, error)
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

// CDCReader streams [Change] events from a source database starting at
// the given Position. Engines whose [Capabilities.CDC] is [CDCNone]
// return a non-nil error for any call to this interface.
type CDCReader interface {
	StreamChanges(ctx context.Context, from Position) (<-chan Change, error)
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
//	pending -> tables -> bulk_copy -> identity_sync -> indexes -> constraints -> complete
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
//
// The bare-string form is preserved for `complete` (compact, matches
// the v0.3.0 wire shape, easy to glance at in psql) and for the
// no-PK sentinel. Cursor-bearing rows use the object form. Custom
// JSON marshallers handle both shapes; see [TableProgress.UnmarshalJSON].
//
// Backward compatibility: a v0.3.0 row with the bare string
// `"in_progress"` decodes into TableProgress{State:
// TableProgressInProgress} with a nil LastPK and zero RowsCopied — the
// orchestrator treats that "no cursor" case as truncate-and-redo on
// resume. Operators upgrading mid-migration should expect that
// in-flight tables will not gain mid-table resume on the v0.4.0
// binary; only fresh migrations do.
type TableProgress struct {
	// State is the lifecycle phase of this table within the bulk-copy
	// stage of the migration.
	State TableProgressState

	// LastPK is the primary-key column values of the last successfully
	// committed row in the table. nil on fresh start, on v0.3.0 rows,
	// and on no-PK tables. For composite PKs the slice is in PK column
	// declaration order. JSON elements pass through encoding/json's
	// default marshalling — integers as numbers, strings as strings,
	// timestamps as RFC3339 strings — which is sufficient for every PK
	// column type the orchestrator currently supports.
	LastPK []any

	// RowsCopied is the count of rows committed so far. Reported in
	// progress logs on resume so an operator can see how far the
	// previous attempt got. Best-effort: a crash between batch commit
	// and checkpoint write may have landed more rows than the count
	// reflects, but the upsert path on resume is tolerant of that drift.
	RowsCopied int64
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
