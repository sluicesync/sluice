package ir

import "context"

// SchemaReader extracts an IR [Schema] from a live database.
type SchemaReader interface {
	ReadSchema(ctx context.Context) (*Schema, error)
}

// SchemaWriter applies an IR [Schema] to a target database in three
// phases. Splitting schema creation from index/constraint creation is
// what enables fast bulk-loading: data is loaded into bare tables,
// then indexes and constraints are added once the data is in place.
type SchemaWriter interface {
	CreateTablesWithoutConstraints(ctx context.Context, s *Schema) error
	CreateIndexes(ctx context.Context, s *Schema) error
	CreateConstraints(ctx context.Context, s *Schema) error
}

// RowReader streams rows from a single table for the bulk-copy phase.
// Implementations should close the returned channel when the table is
// fully read, and return promptly when ctx is cancelled.
type RowReader interface {
	ReadRows(ctx context.Context, table *Table) (<-chan Row, error)
}

// RowWriter performs bulk inserts using the target's native fast-load
// path (COPY, LOAD DATA INFILE, batched INSERTs, etc.). Implementations
// should consume rows until the channel is closed or ctx is cancelled.
type RowWriter interface {
	WriteRows(ctx context.Context, table *Table, rows <-chan Row) error
}

// CDCReader streams [Change] events from a source database starting at
// the given Position. Engines whose [Capabilities.CDC] is [CDCNone]
// return a non-nil error for any call to this interface.
type CDCReader interface {
	StreamChanges(ctx context.Context, from Position) (<-chan Change, error)
}

// ChangeApplier applies [Change] events to a target database.
// Implementations should respect transactional boundaries supplied by
// the source (when present) and update durable position tracking as
// changes are committed.
type ChangeApplier interface {
	Apply(ctx context.Context, changes <-chan Change) error
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
