// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// querier is the slice of database/sql RowReader needs from its query
// source. Both *sql.DB (the simple-mode path, owns its own pool) and
// *sql.Conn (the snapshot-mode path, holds a single pinned connection
// running a long REPEATABLE-READ transaction with SET TRANSACTION
// SNAPSHOT) satisfy it.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// RowReader streams rows from PostgreSQL tables for the bulk-copy
// phase. It implements [ir.RowReader].
//
// Errors during streaming are stored on the reader and accessible via
// Err after the row channel has closed (mirrors database/sql.Rows.Err).
type RowReader struct {
	q      querier
	schema string

	// qualifyBySchema makes ReadRows qualify the table reference by the
	// table's OWN [ir.Table.Schema] (the source schema) rather than the
	// reader's bound r.schema (ADR-0075 Phase 2b multi-schema spanning
	// snapshot). It is set ONLY on the spanning snapshot RowReader, whose
	// single pinned connection reads across N schemas from the one
	// exported-snapshot transaction. In every single-schema path this
	// stays false and the emitted SQL is BYTE-IDENTICAL to the pre-ADR-0075
	// shape — r.schema."table" qualified, exactly as today. (The PG reader
	// already schema-qualifies its SELECT, unlike MySQL's bare table; here
	// the only change is WHICH schema qualifies it.)
	qualifyBySchema bool

	// closer owns the underlying connection resources for this reader.
	// In simple mode it's the *sql.DB; in snapshot mode it's nil and
	// the SnapshotStream owns the lifecycle. Close is a no-op when nil.
	closer io.Closer

	// snapshotPinned marks a reader whose queries all run on ONE pinned
	// *sql.Conn inside a single consistent-read transaction (the
	// slot/export snapshot stream's reader, and the parallel
	// SnapshotImporter readers). Such a reader CANNOT run two overlapping
	// queries on its connection — database/sql serialises a *sql.Conn
	// through closemu, so e.g. CountRows's exact-COUNT fallback firing a
	// second QueryContext while its first Rows is still open self-deadlocks
	// (and during a copy, a probe racing the in-flight row-stream would
	// too). Methods that would issue such an overlapping/concurrent query
	// (CountRows) short-circuit when this is set. It is the EXPLICIT signal
	// for "single pinned conn"; the older `closer == nil` test caught only
	// the externally-owned snapshot reader and MISSED the self-closing
	// importer readers — which have a non-nil closer yet are equally
	// pinned (the bug that wedged the ADR-0079 sync fast path).
	snapshotPinned bool

	// estimatorDSN is the driver-ready DSN (schema already stripped) used
	// by [RowReader.EstimateRowCount] to open a FRESH off-snapshot
	// connection for the pre-stream within-table chunk DECISION on a
	// pinned reader. pg_class.reltuples is snapshot-insensitive catalog
	// metadata, so reading it off a separate conn is correct AND cannot
	// race the pinned reader's in-flight stream (the connection conflict
	// that the chunk-DECISION-only EstimateRowCount surface exists to
	// avoid — see [ir.RowCountEstimator]). Empty on the non-pinned migrate
	// reader, which estimates on its own *sql.DB pool instead. Threaded in
	// at the snapshot stream / importer mint sites (cdc_snapshot.go,
	// snapshot_importer.go).
	estimatorDSN string

	mu  sync.Mutex
	err error // sticky error from the most recent ReadRows call
}

// NewSnapshotRowReader builds an [ir.RowReader] over a caller-pinned
// *sql.Conn that is ALREADY inside a consistent-read transaction
// (typically `BEGIN ISOLATION LEVEL REPEATABLE READ`). The returned
// reader runs its SELECTs on that single connection so every table is
// read within the same MVCC snapshot — the same shape OpenSnapshotStream
// / OpenBackupSnapshot use internally, but exposed so a sibling engine
// that composes postgres by delegation (engines/pgtrigger) can reuse
// the value-decode + buildSelect machinery on its OWN snapshot
// transaction without re-implementing it.
//
// The reader does NOT own the connection lifecycle: its Close is a
// no-op (closer==nil), exactly like the snapshot-mode readers built in
// cdc_snapshot.go / backup_snapshot.go. The caller is responsible for
// COMMIT/ROLLBACK-ing the transaction and returning the conn to its
// pool. schema is the namespace SELECTs are qualified against (the
// DSN's `schema`, default "public").
//
// All the optional RowReader surfaces ([ir.BatchedRowReader],
// [ir.RangeBoundsQuerier], [ir.RowCounter], [ir.SchemaSetter]) are
// available on the returned value because it is a *RowReader — callers
// type-asserting on those interfaces (the bulk-copy orchestrator's
// parallel/cursor paths) get the same behaviour as the slot-based
// snapshot stream's Rows reader.
func NewSnapshotRowReader(conn *sql.Conn, schema string) ir.RowReader {
	return &RowReader{q: conn, schema: schema, closer: nil}
}

// Close releases the underlying connection resources. Safe to call
// multiple times. In snapshot mode (closer==nil) this is a no-op —
// the SnapshotStream's Close is the operative cleanup.
func (r *RowReader) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
}

// SetSchema implements [ir.SchemaSetter]. Called by the pipeline
// orchestrator when `--target-schema NAME` is set (ADR-0031). The
// row reader queries SELECT against the named schema rather than
// the DSN's default. Empty input is a no-op.
func (r *RowReader) SetSchema(name string) {
	if name == "" {
		return
	}
	r.schema = name
}

// effectiveSchema returns the schema a query should qualify table by. In
// the multi-schema spanning snapshot (ADR-0075 Phase 2b) the one pinned
// connection reads across N schemas, so qualify by the table's own Schema
// rather than the reader's single bound schema; the defensive fall-back to
// r.schema covers a single-schema table threaded through the spanning
// reader (empty Table.Schema). In every single-schema path qualifyBySchema
// stays false and this returns r.schema — byte-identical to the pre-ADR-0075
// shape. Shared by ReadRows, CountRows, RangeBounds, and EstimateRowCount so
// they all qualify identically.
func (r *RowReader) effectiveSchema(table *ir.Table) string {
	if r.qualifyBySchema && table.Schema != "" {
		return table.Schema
	}
	return r.schema
}

// Err returns the error, if any, that terminated the most recently
// returned channel. It is only valid to call after the channel has
// been fully drained.
func (r *RowReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// ReadRows streams the rows of table over the returned channel. The
// channel closes when the table is fully read, when ctx is cancelled,
// or when an error occurs (in which case [Err] returns the cause).
//
// Callers must either fully drain the channel or cancel ctx — leaving
// a partially-read channel without cancellation will leak the
// streaming goroutine.
func (r *RowReader) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	if table == nil {
		return nil, errors.New("postgres: ReadRows: table is nil")
	}
	if len(table.Columns) == 0 {
		return nil, fmt.Errorf("postgres: ReadRows: table %q has no columns", table.Name)
	}

	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()

	query := buildSelect(r.effectiveSchema(table), table)
	// rowserrcheck and sqlclosecheck can't follow rows into the
	// goroutine; both rows.Err() and rows.Close() are handled inside
	// stream() (Close via defer, Err checked once iteration ends).
	rows, err := r.q.QueryContext(ctx, query) //nolint:rowserrcheck,sqlclosecheck
	if err != nil {
		return nil, fmt.Errorf("postgres: ReadRows: query failed: %w", err)
	}

	out := make(chan ir.Row, rowChanBuffer)
	go r.stream(ctx, rows, table, out)
	return out, nil
}

// rowChanBuffer is the bounded buffer on the reader's output channel:
// it lets row decode overlap the downstream write instead of
// rendezvous-alternating with it, while staying small enough that
// back-pressure (and worst-case buffered bytes on wide rows) is
// preserved. Mirrors the same-named constant in internal/pipeline
// (see its doc comment for the checkpoint-correctness argument).
const rowChanBuffer = 64

// stream is the goroutine that scans rows from the database and pushes
// them onto out as IR Rows. It owns rows and is responsible for
// closing it; out is owned by stream and is closed before stream exits.
//
// Generated columns are filtered out of the SELECT (see buildSelect)
// and therefore out of the iterated columns here too — the database
// recomputes them on the target's INSERT, so the source value is
// never carried.
func (r *RowReader) stream(ctx context.Context, rows *sql.Rows, table *ir.Table, out chan<- ir.Row) {
	defer close(out)
	defer func() { _ = rows.Close() }()

	cols := sourceReadableColumns(table.Columns)
	scanBuf := make([]any, len(cols))
	scanPtrs := make([]any, len(cols))
	for i := range scanBuf {
		scanPtrs[i] = &scanBuf[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			r.setErr(fmt.Errorf("postgres: scan: %w", err))
			return
		}

		row := make(ir.Row, len(cols))
		for i, col := range cols {
			v, err := decodeValue(scanBuf[i], col.Type)
			if err != nil {
				r.setErr(fmt.Errorf("postgres: column %q: %w", col.Name, err))
				return
			}
			row[col.Name] = v
		}

		select {
		case out <- row:
		case <-ctx.Done():
			r.setErr(ctx.Err())
			return
		}
	}

	if err := rows.Err(); err != nil {
		r.setErr(fmt.Errorf("postgres: rows iteration: %w", err))
	}
}

func (r *RowReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

// buildSelect produces a SELECT statement that fetches every non-
// generated column of table in declaration order. Generated columns
// are excluded so the bulk-copy path doesn't carry source-side
// computed values into the target — the target's own GENERATED
// clause will recompute them on INSERT, preserving the invariant
// rather than freezing it. The table is schema-qualified (Postgres
// has namespaced schemas, unlike MySQL). Identifiers are double-
// quoted with internal quotes escaped.
func buildSelect(schema string, table *ir.Table) string {
	src := sourceReadableColumns(table.Columns)
	cols := make([]string, len(src))
	for i, c := range src {
		cols[i] = quoteIdent(c.Name)
	}
	tableRef := quoteIdent(schema) + "." + quoteIdent(table.Name)
	return fmt.Sprintf(
		"SELECT %s FROM %s",
		strings.Join(cols, ", "),
		tableRef,
	)
}

// nonGeneratedColumns returns the columns of in that are NOT
// generated columns. The slice has the same declaration order as
// the input (callers depend on it for positional row decoding).
//
// File-level note: this engine's translation policy on generated
// columns is verbatim passthrough — the schema reader records the
// expression text on the IR Column, the DDL writer emits a GENERATED
// clause that recreates the same invariant on the target, and the
// row read/write paths skip the column entirely so the database
// recomputes the value rather than freezing the source-side result.
func nonGeneratedColumns(cols []*ir.Column) []*ir.Column {
	out := make([]*ir.Column, 0, len(cols))
	for _, c := range cols {
		if c.IsGenerated() {
			continue
		}
		out = append(out, c)
	}
	return out
}

// sourceReadableColumns returns the columns the reader's SELECT
// projection should fetch from the source. Filters out both generated
// columns (the database recomputes them on the target) AND
// SluiceInjected columns (added by sluice's own ADR-0048 Shape A IR
// pass; they exist on the mutated schema sluice plans against, but do
// NOT exist on the source — selecting them surfaces as SQLSTATE 42703
// "column does not exist", catalog Bug 80). The WRITER path
// deliberately does NOT use this helper because the injected column
// MUST land on the target; the orchestrator's [shardStampRows] wrap
// stamps the discriminator value onto each row between read and
// write, and the writer's nonGeneratedColumns projection picks it up.
func sourceReadableColumns(cols []*ir.Column) []*ir.Column {
	out := make([]*ir.Column, 0, len(cols))
	for _, c := range cols {
		if c.IsGenerated() || c.SluiceInjected {
			continue
		}
		out = append(out, c)
	}
	return out
}

// quoteIdent double-quotes a Postgres identifier. Internal double
// quotes are escaped by doubling — the canonical form for identifier
// quoting in standard SQL.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
