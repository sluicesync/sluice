// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Multi-database analogue of the single-database restart-from-scratch
// gate pins (see restart_from_scratch_reset_test.go). Per the "pin the
// class, not the representative" (Bug 74) discipline, the multi-database
// fan-out is a DISTINCT dispatch site — coldStartCopyOneDatabase's
// per-database cold-start gate (streamer_multidb.go, the
//
//	case s.RestartFromScratch && !copyReaderIsIdempotent(stream.Rows):
//
// branch that calls resetTargetTablesForRestart on the per-DB FILTERED
// schema) — so it needs its own pin even though it reuses the same
// copyReaderIsIdempotent predicate and resetTargetTablesForRestart helper
// the single-database gate uses.
//
// These tests drive coldStartCopyOneDatabase directly through that gate
// and assert the three halves of the decision:
//
//	(a) non-idempotent reader  → the per-DB in-scope target tables ARE dropped
//	(b) idempotent reader      → the target is NOT dropped (absorb-the-overlap)
//	(c) the drop set is scoped to THIS database's filtered tables only
//	    (the schema coldStartCopyOneDatabase read for `database`), never the
//	    whole multi-DB table universe.
//
// COVERAGE BOUNDARY: a unit test cannot ground-truth the SOURCE-side
// per-database scoping itself (the MySQL reader's MultiDatabaseScoper that
// stamps Table.Schema and filters information_schema to one database) —
// that needs a real server and is exercised by the multi-database
// integration suite. What this pin DOES lock is the gate's contract: the
// drop is applied to whatever per-database schema coldStartCopyOneDatabase
// resolved for `database`, and to nothing wider. The stub source's schema
// reader returns a DIFFERENT table set per database name, so assertion (c)
// is real: driving the gate for one database must drop exactly that
// database's tables.

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// recordingDropper is a RowWriter + TableDropper that records each drop
// and, unlike emptyCheckingDropper, ALSO accepts WriteRows / the
// idempotent variant by draining the (closed/empty) channel — so the
// bulk-copy that runs after the gate completes harmlessly and the test
// asserts purely on the recorded drop set. It implements
// IdempotentRowWriter so the idempotent-reader case takes its upsert path
// without erroring on a missing surface.
type recordingDropper struct {
	dropped []string
}

func (w *recordingDropper) WriteRows(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	for range rows { //nolint:revive // drain the (empty) channel so the gate's post-drop bulk-copy completes
	}
	return nil
}

func (w *recordingDropper) WriteRowsIdempotent(ctx context.Context, t *ir.Table, rows <-chan ir.Row) error {
	return w.WriteRows(ctx, t, rows)
}

func (w *recordingDropper) DropTable(_ context.Context, table *ir.Table) error {
	w.dropped = append(w.dropped, table.Name)
	return nil
}

// --- multi-database stub engines, scoped per database name ---

// multiDBGateSource is a minimal ir.Engine + ir.DatabaseDSNDeriver whose
// per-database SchemaReader returns the table set registered for the
// derived database. WithDatabase records the requested database so the
// schema reader can return that database's tables (mirroring the real
// per-database information_schema read), which is what makes assertion
// (c) — drop-set scoping — a genuine check rather than a tautology.
type multiDBGateSource struct {
	name string
	// tablesByDB maps a source database name to the unqualified table
	// names coldStartCopyOneDatabase should see when it reads that
	// database's schema.
	tablesByDB map[string][]string
}

func (e *multiDBGateSource) Name() string                  { return e.name }
func (e *multiDBGateSource) Capabilities() ir.Capabilities { return ir.Capabilities{} }

// WithDatabase encodes the database into the derived DSN so the schema
// reader opened against it knows which database's tables to return.
func (e *multiDBGateSource) WithDatabase(_, database string) (string, error) {
	return "db=" + database, nil
}

func (e *multiDBGateSource) EnsureDatabase(context.Context, string, string) error { return nil }

func (e *multiDBGateSource) OpenSchemaReader(_ context.Context, dsn string) (ir.SchemaReader, error) {
	const prefix = "db="
	if len(dsn) <= len(prefix) || dsn[:len(prefix)] != prefix {
		return nil, errors.New("multiDBGateSource: unexpected dsn " + dsn)
	}
	database := dsn[len(prefix):]
	names := e.tablesByDB[database]
	tables := make([]*ir.Table, 0, len(names))
	for _, n := range names {
		// A single-column PK so the idempotent cold-copy path (Bug 125
		// upsert) has a usable conflict key; the non-idempotent path
		// ignores it.
		tables = append(tables, &ir.Table{
			Name:    n,
			Schema:  database,
			Columns: []*ir.Column{{Name: "id"}},
			PrimaryKey: &ir.Index{
				Columns: []ir.IndexColumn{{Column: "id"}},
				Unique:  true,
			},
		})
	}
	return &fixedSchemaReader{schema: &ir.Schema{Tables: tables}}, nil
}

func (e *multiDBGateSource) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return nil, errors.New("multiDBGateSource: OpenSchemaWriter not used")
}

func (e *multiDBGateSource) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errors.New("multiDBGateSource: OpenRowReader not used")
}

func (e *multiDBGateSource) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return nil, errors.New("multiDBGateSource: OpenRowWriter not used")
}

func (e *multiDBGateSource) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("multiDBGateSource: OpenCDCReader not used")
}

func (e *multiDBGateSource) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("multiDBGateSource: OpenChangeApplier not used")
}

func (e *multiDBGateSource) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("multiDBGateSource: OpenSnapshotStream not used")
}

type fixedSchemaReader struct{ schema *ir.Schema }

func (r *fixedSchemaReader) ReadSchema(context.Context) (*ir.Schema, error) { return r.schema, nil }

// multiDBGateTarget returns a recording, dropping RowWriter (the gate's
// drop machinery records into it) and a no-op SchemaWriter (the bulk-copy
// CreateTablesWithoutConstraints lands here harmlessly). It does NOT
// implement ir.DatabaseDSNDeriver, so coldStartCopyOneDatabase routes via
// target-schema (targetCanDeriveDB=false) — the MySQL→PG fan-out shape.
type multiDBGateTarget struct {
	rw *recordingDropper
}

func (e *multiDBGateTarget) Name() string                  { return "multidb-gate-target" }
func (e *multiDBGateTarget) Capabilities() ir.Capabilities { return ir.Capabilities{} }

func (e *multiDBGateTarget) OpenSchemaWriter(context.Context, string) (ir.SchemaWriter, error) {
	return noopSchemaWriter{}, nil
}

func (e *multiDBGateTarget) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return e.rw, nil
}

func (e *multiDBGateTarget) OpenSchemaReader(context.Context, string) (ir.SchemaReader, error) {
	return nil, errors.New("multiDBGateTarget: OpenSchemaReader not used")
}

func (e *multiDBGateTarget) OpenRowReader(context.Context, string) (ir.RowReader, error) {
	return nil, errors.New("multiDBGateTarget: OpenRowReader not used")
}

func (e *multiDBGateTarget) OpenCDCReader(context.Context, string) (ir.CDCReader, error) {
	return nil, errors.New("multiDBGateTarget: OpenCDCReader not used")
}

func (e *multiDBGateTarget) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return nil, errors.New("multiDBGateTarget: OpenChangeApplier not used")
}

func (e *multiDBGateTarget) OpenSnapshotStream(context.Context, string) (*ir.SnapshotStream, error) {
	return nil, errors.New("multiDBGateTarget: OpenSnapshotStream not used")
}

// (noopSchemaWriter — the no-op SchemaWriter that absorbs the bulk-copy's
// DDL phase — is defined in copy_concurrent_tables_test.go and reused here.)

// runMultiDBGate drives coldStartCopyOneDatabase for one database through
// the per-database cold-start gate and returns the recording target.
func runMultiDBGate(t *testing.T, s *Streamer, rows ir.RowReader, database string, inScope func(string) bool) *recordingDropper {
	t.Helper()
	rw := &recordingDropper{}
	src := s.Source.(*multiDBGateSource)
	tgt := &multiDBGateTarget{rw: rw}
	s.Source = src
	s.Target = tgt

	stream := &ir.SnapshotStream{Rows: rows, CloseFn: func() error { return nil }}
	err := s.coldStartCopyOneDatabase(
		context.Background(), stream, &stubChangeApplier{},
		"stream-1", database, inScope,
		nil /*targetDeriver*/, false, /*targetCanDeriveDB → target-schema routing*/
	)
	if err != nil {
		t.Fatalf("coldStartCopyOneDatabase(%q): %v", database, err)
	}
	return rw
}

// (a) non-idempotent reader → the per-database in-scope target tables are
// dropped before the fresh cold-start (mirrors the single-DB
// TestColdStartGate_RestartFromScratch_NonIdempotent_DropsTarget).
func TestColdStartCopyOneDatabase_RestartFromScratch_NonIdempotent_DropsTarget(t *testing.T) {
	captureSlog(t)
	src := &multiDBGateSource{
		name:       "mysql",
		tablesByDB: map[string][]string{"shop": {"users", "orders"}},
	}
	s := &Streamer{Source: src, RestartFromScratch: true}

	rw := runMultiDBGate(t, s, nonIdempotentReader{}, "shop", func(string) bool { return true })

	if got := rw.dropped; len(got) != 2 || got[0] != "users" || got[1] != "orders" {
		t.Errorf("non-idempotent restart must DROP the per-DB in-scope tables; dropped = %v", got)
	}
}

// (b) idempotent reader → the target is NOT dropped (the
// absorb-the-overlap path; the gate falls through to the force-skipped
// preflight). Mirrors TestColdStartGate_RestartFromScratch_Idempotent_DoesNotDrop.
func TestColdStartCopyOneDatabase_RestartFromScratch_Idempotent_DoesNotDrop(t *testing.T) {
	captureSlog(t)
	src := &multiDBGateSource{
		name:       "planetscale",
		tablesByDB: map[string][]string{"shop": {"users", "orders"}},
	}
	s := &Streamer{Source: src, RestartFromScratch: true}

	rw := runMultiDBGate(t, s, idempotentReader{}, "shop", func(string) bool { return true })

	if len(rw.dropped) != 0 {
		t.Errorf("idempotent restart must NOT drop (absorb-the-overlap path); dropped = %v", rw.dropped)
	}
}

// (c) the drop set is scoped to THIS database's filtered tables — driving
// the gate for one database of a multi-DB universe drops exactly that
// database's tables, never another database's. The stub source returns a
// distinct table set per database, so this is a genuine scoping check.
func TestColdStartCopyOneDatabase_RestartFromScratch_DropScopedToOneDatabase(t *testing.T) {
	captureSlog(t)
	src := &multiDBGateSource{
		name: "mysql",
		tablesByDB: map[string][]string{
			"shop":      {"users", "orders"},
			"analytics": {"events", "sessions", "rollups"},
		},
	}
	inScope := func(db string) bool { return db == "shop" || db == "analytics" }
	s := &Streamer{Source: src, RestartFromScratch: true}

	// Drive only the "shop" database.
	rw := runMultiDBGate(t, s, nonIdempotentReader{}, "shop", inScope)

	if got := rw.dropped; len(got) != 2 || got[0] != "users" || got[1] != "orders" {
		t.Fatalf("expected only shop's tables dropped; dropped = %v", got)
	}
	for _, name := range rw.dropped {
		if name == "events" || name == "sessions" || name == "rollups" {
			t.Errorf("drop set leaked another database's table %q; dropped = %v", name, rw.dropped)
		}
	}
}
