// Package pipeline implements the simple-mode orchestrator: a one-shot
// schema-and-data migration from a source database to a target. It is
// the layer that wires the IR's reader and writer interfaces into an
// end-to-end migration, given two engines.
//
// The simple-mode flow:
//
//  1. Read the source schema.
//  2. Translate (currently identity; the dedicated translator layer
//     lands in a future commit when cross-engine type rewriting needs
//     to be policy-driven rather than rejected with a clear error).
//  3. Apply schema phase 1: tables without indexes or constraints.
//  4. Bulk-copy data, table by table.
//  5. Apply schema phase 2: indexes.
//  6. Apply schema phase 3: foreign keys.
//
// The package does not depend on any specific engine package; engines
// are passed in as [ir.Engine] values, typically resolved by the CLI
// from the engines registry.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/orware/sluice/internal/ir"
)

// Migrator runs a single simple-mode migration from Source/SourceDSN to
// Target/TargetDSN. Construct the value, then call Run with a context.
//
// Migrator does not retain state between Run calls — call it once per
// migration. Concurrent calls on the same value are not supported; if
// you want to run two migrations in parallel, instantiate two values.
type Migrator struct {
	// Source is the engine the source DSN belongs to (e.g. mysql,
	// postgres). Required.
	Source ir.Engine

	// Target is the engine the target DSN belongs to. May be the
	// same as Source for same-engine migrations. Required.
	Target ir.Engine

	// SourceDSN is the source-engine-native connection string.
	// Required.
	SourceDSN string

	// TargetDSN is the target-engine-native connection string.
	// Required.
	TargetDSN string

	// DryRun, when true, reads the source schema and prints what
	// would be applied without actually writing anything to the
	// target. Useful for verifying connectivity and previewing the
	// migration plan.
	DryRun bool

	// Stdout is where dry-run plan output goes. Defaults to os.Stdout
	// when nil; tests can supply a buffer.
	Stdout io.Writer
}

// Run executes the migration. Returns nil on success or a wrapped
// error pointing at the phase that failed.
//
// Run honours ctx cancellation: if ctx is cancelled mid-migration,
// the underlying database operations return ctx.Err() and Run
// surfaces it. Partially-applied state on the target is the user's
// responsibility — for v1 there is no automatic rollback, since DDL
// in MySQL is implicit-commit and rollback after partial application
// is engine-dependent.
func (m *Migrator) Run(ctx context.Context) error {
	if err := m.validate(); err != nil {
		return err
	}

	// ---- 1. Open and read source schema ----
	sr, err := m.Source.OpenSchemaReader(ctx, m.SourceDSN)
	if err != nil {
		return fmt.Errorf("pipeline: open source schema reader: %w", err)
	}
	defer closeIf(sr)

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return fmt.Errorf("pipeline: read source schema: %w", err)
	}
	if len(schema.Tables) == 0 {
		m.printf("pipeline: source schema has no tables; nothing to migrate\n")
		return nil
	}

	if m.DryRun {
		return m.printPlan(schema)
	}

	// ---- 2. Open target writers ----
	sw, err := m.Target.OpenSchemaWriter(ctx, m.TargetDSN)
	if err != nil {
		return fmt.Errorf("pipeline: open target schema writer: %w", err)
	}
	defer closeIf(sw)

	rw, err := m.Target.OpenRowWriter(ctx, m.TargetDSN)
	if err != nil {
		return fmt.Errorf("pipeline: open target row writer: %w", err)
	}
	defer closeIf(rw)

	// ---- 3-6. Schema apply (phase 1) → bulk copy → indexes → constraints.
	rr, err := m.Source.OpenRowReader(ctx, m.SourceDSN)
	if err != nil {
		return fmt.Errorf("pipeline: open source row reader: %w", err)
	}
	defer closeIf(rr)

	if err := runBulkCopy(ctx, schema, rr, sw, rw); err != nil {
		return err
	}

	m.printf("pipeline: migrated %d tables\n", len(schema.Tables))
	return nil
}

// runBulkCopy applies the four shared phases that follow target-writer
// open: schema phase 1 (tables without constraints) → bulk-copy of
// every table → schema phase 2 (indexes) → schema phase 3 (foreign
// keys). Used by both [Migrator] (one-shot mode) and [Streamer]
// (long-running mode) — the only difference between them is where
// `rows` comes from (engine-pool RowReader vs [ir.SnapshotStream].Rows).
//
// Errors from any phase are wrapped with the phase name so the caller
// can pinpoint which step failed without parsing strings.
func runBulkCopy(
	ctx context.Context,
	schema *ir.Schema,
	rows ir.RowReader,
	sw ir.SchemaWriter,
	rw ir.RowWriter,
) error {
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		return fmt.Errorf("pipeline: create tables: %w", err)
	}
	for _, table := range schema.Tables {
		if err := copyTable(ctx, rows, rw, table); err != nil {
			return fmt.Errorf("pipeline: copy table %q: %w", table.Name, err)
		}
	}
	if err := sw.CreateIndexes(ctx, schema); err != nil {
		return fmt.Errorf("pipeline: create indexes: %w", err)
	}
	if err := sw.CreateConstraints(ctx, schema); err != nil {
		return fmt.Errorf("pipeline: create constraints: %w", err)
	}
	return nil
}

// validate checks that all required fields are populated. Errors here
// indicate caller bugs; surface them clearly before any I/O happens.
func (m *Migrator) validate() error {
	switch {
	case m.Source == nil:
		return errors.New("pipeline: Source engine is nil")
	case m.Target == nil:
		return errors.New("pipeline: Target engine is nil")
	case m.SourceDSN == "":
		return errors.New("pipeline: SourceDSN is empty")
	case m.TargetDSN == "":
		return errors.New("pipeline: TargetDSN is empty")
	}
	return nil
}

// copyTable opens the source-side row stream, hands it off to the
// target writer, and waits for completion. The reader's lifetime
// covers exactly one table; the writer is reused across tables.
func copyTable(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table) error {
	rows, err := rr.ReadRows(ctx, table)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	if err := rw.WriteRows(ctx, table, rows); err != nil {
		return fmt.Errorf("write rows: %w", err)
	}
	return nil
}

// printPlan writes a human-readable summary of what Run would do,
// without performing any writes. Used when DryRun is true.
func (m *Migrator) printPlan(schema *ir.Schema) error {
	m.printf("DRY RUN — would migrate %s → %s\n", m.Source.Name(), m.Target.Name())
	m.printf("  %d tables to create, populate, and constrain:\n", len(schema.Tables))
	for _, t := range schema.Tables {
		m.printf("    - %s  (%d columns, %d indexes, %d foreign keys)\n",
			t.Name, len(t.Columns), len(t.Indexes), len(t.ForeignKeys))
	}
	return nil
}

// printf writes formatted output to m.Stdout, defaulting to discarding
// when no writer is configured.
func (m *Migrator) printf(format string, args ...any) {
	if m.Stdout == nil {
		return
	}
	fmt.Fprintf(m.Stdout, format, args...)
}

// closeIf calls Close on v if it implements io.Closer. Used to clean
// up the *sql.DB handles the engine readers/writers wrap.
func closeIf(v any) {
	if c, ok := v.(io.Closer); ok {
		_ = c.Close()
	}
}
