// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Unit tests for Phase 5's cross-engine routing in chain restore.
// Cross-engine schema-delta translation runs through
// [translate.RetargetForEngine]: PG-source `ADD COLUMN UUID` arrives
// at the MySQL target's [ir.SchemaDeltaApplier.AlterAddColumn] as
// `ADD COLUMN CHAR(36)`. These tests use a recording schema writer
// that captures the column definitions handed to AlterAddColumn so
// the test can assert on the post-translation type.

import (
	"context"
	"errors"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// schemaDeltaRecorderEngine: an [ir.Engine] whose schema writer
// implements [ir.SchemaDeltaApplier] and records each AlterAddColumn
// call's arguments. Used to assert cross-engine retargeting of
// schema deltas.
type schemaDeltaRecorderEngine struct {
	*chainRestoreRecorderEngine

	mu          sync.Mutex
	addedColumn []*ir.Column // last AlterAddColumn call's added cols
	alterTable  *ir.Table    // last AlterAddColumn call's table arg
	addedTables []*ir.Table  // every CreateTablesWithoutConstraints input
}

func newSchemaDeltaRecorderEngine(name string) *schemaDeltaRecorderEngine {
	inner := &chainRestoreRecorderEngine{
		restoreRecorderEngine: newRestoreRecorderEngine(name),
	}
	return &schemaDeltaRecorderEngine{chainRestoreRecorderEngine: inner}
}

func (e *schemaDeltaRecorderEngine) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	return &schemaDeltaRecorderWriter{owner: e}, nil
}

type schemaDeltaRecorderWriter struct {
	owner *schemaDeltaRecorderEngine
}

func (w *schemaDeltaRecorderWriter) CreateTablesWithoutConstraints(_ context.Context, s *ir.Schema) error {
	w.owner.mu.Lock()
	defer w.owner.mu.Unlock()
	w.owner.recordPhase("CreateTablesWithoutConstraints")
	w.owner.addedTables = append(w.owner.addedTables, s.Tables...)
	return nil
}

func (w *schemaDeltaRecorderWriter) CreateIndexes(_ context.Context, _ *ir.Schema) error {
	w.owner.recordPhase("CreateIndexes")
	return nil
}

func (w *schemaDeltaRecorderWriter) CreateConstraints(_ context.Context, _ *ir.Schema) error {
	w.owner.recordPhase("CreateConstraints")
	return nil
}

func (w *schemaDeltaRecorderWriter) SyncIdentitySequences(_ context.Context, _ *ir.Schema) error {
	w.owner.recordPhase("SyncIdentitySequences")
	return nil
}

func (w *schemaDeltaRecorderWriter) CreateViews(_ context.Context, _ *ir.Schema) error {
	w.owner.recordPhase("CreateViews")
	return nil
}

func (w *schemaDeltaRecorderWriter) AlterAddColumn(_ context.Context, table *ir.Table, cols []*ir.Column) error {
	w.owner.mu.Lock()
	defer w.owner.mu.Unlock()
	w.owner.recordPhase("AlterAddColumn:" + table.Name)
	w.owner.alterTable = table
	w.owner.addedColumn = cols
	return nil
}

// schemaErroringDeltaApplier returns a known error from
// AlterAddColumn so tests can assert error propagation.
type schemaErroringDeltaApplier struct {
	*schemaDeltaRecorderEngine
	err error
}

func (e *schemaErroringDeltaApplier) OpenSchemaWriter(_ context.Context, _ string) (ir.SchemaWriter, error) {
	return &erroringSchemaWriter{owner: e}, nil
}

type erroringSchemaWriter struct {
	owner *schemaErroringDeltaApplier
}

func (w *erroringSchemaWriter) CreateTablesWithoutConstraints(_ context.Context, s *ir.Schema) error {
	w.owner.mu.Lock()
	defer w.owner.mu.Unlock()
	w.owner.recordPhase("CreateTablesWithoutConstraints")
	w.owner.addedTables = append(w.owner.addedTables, s.Tables...)
	return nil
}

func (w *erroringSchemaWriter) CreateIndexes(_ context.Context, _ *ir.Schema) error { return nil }

func (w *erroringSchemaWriter) CreateConstraints(_ context.Context, _ *ir.Schema) error { return nil }

func (w *erroringSchemaWriter) SyncIdentitySequences(_ context.Context, _ *ir.Schema) error {
	return nil
}
func (w *erroringSchemaWriter) CreateViews(_ context.Context, _ *ir.Schema) error { return nil }

func (w *erroringSchemaWriter) AlterAddColumn(_ context.Context, _ *ir.Table, _ []*ir.Column) error {
	return w.owner.err
}

// TestApplySchemaDeltas_CrossEngine_AddColumnUUIDtoChar36 verifies
// that an ALTER TABLE ADD COLUMN UUID delta on a PG-source chain
// arrives at the MySQL target's AlterAddColumn as a CHAR(36) column —
// the canonical PG → MySQL UUID retarget defined in
// [translate.RetargetForEngine].
func TestApplySchemaDeltas_CrossEngine_AddColumnUUIDtoChar36(t *testing.T) {
	tgt := newSchemaDeltaRecorderEngine("mysql")
	cr := &ChainRestore{
		Target: tgt, TargetDSN: "tgt", Store: &LocalStore{},
	}
	link := &segmentRecord{segment: &LineageSegment{Codec: CodecGzip}, manifestRecord: manifestRecord{
		path: "manifests/incr-0001.json",
		manifest: &ir.Manifest{
			BackupID:     "incr-0001",
			SourceEngine: "postgres",
			Kind:         ir.BackupKindIncremental,
			SchemaDelta: []*ir.SchemaDeltaEntry{{
				Kind:  ir.SchemaDeltaAlterTable,
				Table: "users",
				Before: &ir.Table{
					Name: "users",
					Columns: []*ir.Column{
						{Name: "id", Type: ir.Integer{Width: 64}},
					},
				},
				After: &ir.Table{
					Name: "users",
					Columns: []*ir.Column{
						{Name: "id", Type: ir.Integer{Width: 64}},
						{Name: "external_id", Type: ir.UUID{}},
					},
				},
			}},
		},
	}}
	if err := cr.applySchemaDeltas(context.Background(), link); err != nil {
		t.Fatalf("applySchemaDeltas: %v", err)
	}
	if len(tgt.addedColumn) != 1 {
		t.Fatalf("addedColumn len = %d; want 1", len(tgt.addedColumn))
	}
	col := tgt.addedColumn[0]
	if col.Name != "external_id" {
		t.Errorf("col.Name = %q; want external_id", col.Name)
	}
	// PG's UUID retargets to MySQL's CHAR(36).
	if c, ok := col.Type.(ir.Char); !ok || c.Length != 36 {
		t.Errorf("col.Type = %T %+v; want ir.Char{Length:36}", col.Type, col.Type)
	}
}

// TestApplySchemaDeltas_SameEngine_NoRetarget_TINYINTpassthrough
// asserts the same-engine path leaves columns untouched: a MySQL
// chain on a MySQL target sees the source's TINYINT(1) column verbatim
// at the target's applier.
func TestApplySchemaDeltas_SameEngine_NoRetarget_TINYINTpassthrough(t *testing.T) {
	tgt := newSchemaDeltaRecorderEngine("mysql")
	cr := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: &LocalStore{}}
	link := &segmentRecord{segment: &LineageSegment{Codec: CodecGzip}, manifestRecord: manifestRecord{
		path: "manifests/incr-0001.json",
		manifest: &ir.Manifest{
			BackupID:     "incr-0001",
			SourceEngine: "mysql",
			Kind:         ir.BackupKindIncremental,
			SchemaDelta: []*ir.SchemaDeltaEntry{{
				Kind:  ir.SchemaDeltaAlterTable,
				Table: "users",
				Before: &ir.Table{
					Name: "users",
					Columns: []*ir.Column{
						{Name: "id", Type: ir.Integer{Width: 64}},
					},
				},
				After: &ir.Table{
					Name: "users",
					Columns: []*ir.Column{
						{Name: "id", Type: ir.Integer{Width: 64}},
						{Name: "active", Type: ir.Boolean{}},
					},
				},
			}},
		},
	}}
	if err := cr.applySchemaDeltas(context.Background(), link); err != nil {
		t.Fatalf("applySchemaDeltas: %v", err)
	}
	if len(tgt.addedColumn) != 1 {
		t.Fatalf("addedColumn len = %d; want 1", len(tgt.addedColumn))
	}
	col := tgt.addedColumn[0]
	if col.Name != "active" {
		t.Errorf("col.Name = %q; want active", col.Name)
	}
	// Same-engine: Boolean stays Boolean (the MySQL writer maps it to
	// TINYINT(1) at emit-time, not in the IR).
	if _, ok := col.Type.(ir.Boolean); !ok {
		t.Errorf("col.Type = %T; want ir.Boolean (same-engine, no retarget)", col.Type)
	}
}

// TestApplySchemaDeltas_CrossEngine_AddTableUUID_RetargetsCol verifies
// AddTable through the cross-engine path. A PG-source chain creating a
// new table with a UUID column arrives at the MySQL target's
// CreateTablesWithoutConstraints with the column already retargeted
// to CHAR(36).
func TestApplySchemaDeltas_CrossEngine_AddTableUUID_RetargetsCol(t *testing.T) {
	tgt := newSchemaDeltaRecorderEngine("mysql")
	cr := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: &LocalStore{}}
	link := &segmentRecord{segment: &LineageSegment{Codec: CodecGzip}, manifestRecord: manifestRecord{
		path: "manifests/incr-0001.json",
		manifest: &ir.Manifest{
			BackupID:     "incr-0001",
			SourceEngine: "postgres",
			Kind:         ir.BackupKindIncremental,
			SchemaDelta: []*ir.SchemaDeltaEntry{{
				Kind:  ir.SchemaDeltaAddTable,
				Table: "tokens",
				After: &ir.Table{
					Name: "tokens",
					Columns: []*ir.Column{
						{Name: "id", Type: ir.UUID{}},
						{Name: "label", Type: ir.Varchar{Length: 64}},
					},
				},
			}},
		},
	}}
	if err := cr.applySchemaDeltas(context.Background(), link); err != nil {
		t.Fatalf("applySchemaDeltas: %v", err)
	}
	if len(tgt.addedTables) != 1 {
		t.Fatalf("addedTables = %d; want 1", len(tgt.addedTables))
	}
	tbl := tgt.addedTables[0]
	if tbl.Name != "tokens" {
		t.Errorf("table = %q; want tokens", tbl.Name)
	}
	if c, ok := tbl.Columns[0].Type.(ir.Char); !ok || c.Length != 36 {
		t.Errorf("col[0].Type = %T %+v; want ir.Char{Length:36} (UUID → CHAR(36))",
			tbl.Columns[0].Type, tbl.Columns[0].Type)
	}
}

// TestApplySchemaDeltas_AlterAddColumnError_Propagates verifies that
// when the underlying schema-delta applier returns an error, the
// chain restore surfaces it (and does not silently swallow).
func TestApplySchemaDeltas_AlterAddColumnError_Propagates(t *testing.T) {
	want := errors.New("alter add column: target connection refused")
	tgt := &schemaErroringDeltaApplier{
		schemaDeltaRecorderEngine: newSchemaDeltaRecorderEngine("mysql"),
		err:                       want,
	}
	cr := &ChainRestore{Target: tgt, TargetDSN: "tgt", Store: &LocalStore{}}
	link := &segmentRecord{segment: &LineageSegment{Codec: CodecGzip}, manifestRecord: manifestRecord{
		path: "manifests/incr-0001.json",
		manifest: &ir.Manifest{
			BackupID:     "incr-0001",
			SourceEngine: "postgres",
			Kind:         ir.BackupKindIncremental,
			SchemaDelta: []*ir.SchemaDeltaEntry{{
				Kind:   ir.SchemaDeltaAlterTable,
				Table:  "users",
				Before: &ir.Table{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
				After: &ir.Table{Name: "users", Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{Name: "external_id", Type: ir.UUID{}},
				}},
			}},
		},
	}}
	err := cr.applySchemaDeltas(context.Background(), link)
	if err == nil {
		t.Fatal("err = nil; want propagated AlterAddColumn err")
	}
	if !errors.Is(err, want) {
		t.Errorf("err = %v; want wrap of %v", err, want)
	}
}
