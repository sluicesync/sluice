// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Unit pins for restore-parity TRIAGE #4: a source UNIQUE CONSTRAINT
// (ir.Index.ConstraintBacked) re-emits as ALTER TABLE ... ADD
// CONSTRAINT in the constraints phase, while a plain unique INDEX
// stays a CREATE UNIQUE INDEX in the index phase. Both object shapes
// are pinned in every dispatch surface that splits on the flag (the
// emit helper, the index-phase job builder, the preview path, and the
// Bug 125 keyless inline promotion).

func TestEmitAddUniqueConstraint(t *testing.T) {
	t.Run("single column", func(t *testing.T) {
		idx := &ir.Index{
			Name:             "customers_email_unique",
			Unique:           true,
			ConstraintBacked: true,
			Columns:          []ir.IndexColumn{{Column: "email"}},
		}
		got, err := emitAddUniqueConstraint("public", "customers", idx)
		if err != nil {
			t.Fatalf("emitAddUniqueConstraint: %v", err)
		}
		want := `ALTER TABLE "public"."customers" ADD CONSTRAINT "customers_email_unique" UNIQUE ("email");`
		if got != want {
			t.Errorf("stmt mismatch\n got: %q\nwant: %q", got, want)
		}
	})

	t.Run("multi column preserves order", func(t *testing.T) {
		idx := &ir.Index{
			Name:             "t_a_b_unique",
			Unique:           true,
			ConstraintBacked: true,
			Columns:          []ir.IndexColumn{{Column: "b"}, {Column: "a"}},
		}
		got, err := emitAddUniqueConstraint("public", "t", idx)
		if err != nil {
			t.Fatalf("emitAddUniqueConstraint: %v", err)
		}
		want := `ALTER TABLE "public"."t" ADD CONSTRAINT "t_a_b_unique" UNIQUE ("b", "a");`
		if got != want {
			t.Errorf("stmt mismatch\n got: %q\nwant: %q", got, want)
		}
	})

	t.Run("include payload carries", func(t *testing.T) {
		idx := &ir.Index{
			Name:             "t_key_unique",
			Unique:           true,
			ConstraintBacked: true,
			Columns:          []ir.IndexColumn{{Column: "key"}},
			IncludeColumns:   []string{"payload"},
		}
		got, err := emitAddUniqueConstraint("public", "t", idx)
		if err != nil {
			t.Fatalf("emitAddUniqueConstraint: %v", err)
		}
		want := `ALTER TABLE "public"."t" ADD CONSTRAINT "t_key_unique" UNIQUE ("key") INCLUDE ("payload");`
		if got != want {
			t.Errorf("stmt mismatch\n got: %q\nwant: %q", got, want)
		}
	})

	t.Run("expression entry refuses loudly", func(t *testing.T) {
		// PG forbids expressions in UNIQUE constraints, so a
		// ConstraintBacked index carrying one is a sluice-bug condition
		// that must refuse, not emit invalid DDL.
		idx := &ir.Index{
			Name:             "t_expr_unique",
			Unique:           true,
			ConstraintBacked: true,
			Columns:          []ir.IndexColumn{{Expression: "lower(email)", ExpressionDialect: "postgres"}},
		}
		if _, err := emitAddUniqueConstraint("public", "t", idx); err == nil {
			t.Fatal("expression entry emitted; want loud refusal")
		}
	})

	t.Run("nameless or columnless refuses", func(t *testing.T) {
		if _, err := emitAddUniqueConstraint("public", "t", &ir.Index{Unique: true, ConstraintBacked: true}); err == nil {
			t.Fatal("nameless/columnless constraint emitted; want error")
		}
		if _, err := emitAddUniqueConstraint("public", "t", nil); err == nil {
			t.Fatal("nil index emitted; want error")
		}
	})
}

// TestIndexBuildJobs_SkipConstraintBacked pins the index-phase split:
// constraint-backed unique indexes are NOT built as CREATE UNIQUE
// INDEX (they land via CreateConstraints), while plain unique and
// non-unique indexes still are.
func TestIndexBuildJobs_SkipConstraintBacked(t *testing.T) {
	w := &SchemaWriter{schema: "public"}
	table := &ir.Table{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "number", Type: ir.Integer{Width: 64}},
			{Name: "state", Type: ir.Text{Size: ir.TextLong}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Name: "orders_pkey", Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "orders_number_unique", Unique: true, ConstraintBacked: true, Columns: []ir.IndexColumn{{Column: "number"}}},
			{Name: "orders_number_uidx", Unique: true, Columns: []ir.IndexColumn{{Column: "number"}}},
			{Name: "orders_state_idx", Columns: []ir.IndexColumn{{Column: "state"}}},
		},
	}
	jobs := w.indexBuildJobsForTables([]*ir.Table{table})
	names := make([]string, 0, len(jobs))
	for _, j := range jobs {
		names = append(names, j.idx.Name)
	}
	if len(names) != 2 || names[0] != "orders_number_uidx" || names[1] != "orders_state_idx" {
		t.Errorf("index jobs = %v; want [orders_number_uidx orders_state_idx] (constraint-backed skipped)", names)
	}
}

// TestPreviewDDL_UniqueConstraintSplit pins the preview mirror of the
// live split — the constraint form previews as ALTER TABLE ... ADD
// CONSTRAINT ... UNIQUE, ordered BEFORE any FK (an FK may reference
// it), and never as CREATE UNIQUE INDEX; the plain unique index keeps
// its CREATE UNIQUE INDEX shape.
func TestPreviewDDL_UniqueConstraintSplit(t *testing.T) {
	w := &SchemaWriter{schema: "public"}
	orders := &ir.Table{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "number", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Name: "orders_pkey", Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "orders_number_unique", Unique: true, ConstraintBacked: true, Columns: []ir.IndexColumn{{Column: "number"}}},
			{Name: "orders_number_uidx", Unique: true, Columns: []ir.IndexColumn{{Column: "number"}}},
		},
	}
	lines := &ir.Table{
		Name: "lines",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "order_number", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Name: "lines_pkey", Columns: []ir.IndexColumn{{Column: "id"}}},
		ForeignKeys: []*ir.ForeignKey{{
			Name:              "lines_order_fk",
			Columns:           []string{"order_number"},
			ReferencedTable:   "orders",
			ReferencedColumns: []string{"number"},
		}},
	}
	stmts, err := w.PreviewDDL(context.Background(), &ir.Schema{Tables: []*ir.Table{orders, lines}})
	if err != nil {
		t.Fatalf("PreviewDDL: %v", err)
	}
	addConstraint, addFK, plainUnique := -1, -1, -1
	for i, st := range stmts {
		switch {
		case strings.Contains(st.SQL, `ADD CONSTRAINT "orders_number_unique" UNIQUE`):
			addConstraint = i
		case strings.Contains(st.SQL, `FOREIGN KEY`):
			addFK = i
		case strings.Contains(st.SQL, `CREATE UNIQUE INDEX "orders_number_uidx"`):
			plainUnique = i
		}
		if strings.Contains(st.SQL, `CREATE UNIQUE INDEX "orders_number_unique"`) {
			t.Errorf("constraint-backed index previewed as CREATE UNIQUE INDEX: %q", st.SQL)
		}
	}
	if addConstraint < 0 {
		t.Fatalf("preview missing ADD CONSTRAINT orders_number_unique UNIQUE; stmts: %+v", stmts)
	}
	if plainUnique < 0 {
		t.Errorf("preview missing CREATE UNIQUE INDEX orders_number_uidx; stmts: %+v", stmts)
	}
	if addFK < 0 {
		t.Fatalf("preview missing the FK statement; stmts: %+v", stmts)
	}
	if addConstraint > addFK {
		t.Errorf("ADD CONSTRAINT UNIQUE (idx %d) previewed after the FK (idx %d); unique constraints must precede FKs", addConstraint, addFK)
	}
}

// TestPreviewDDL_KeylessInlinePromotedConstraintNotDuplicated pins the
// Bug 125 interaction: on a PK-less table whose promoted COPY unique
// key IS the constraint-backed index, the CREATE TABLE inline
// promotion (CONSTRAINT ... UNIQUE) already carries the object, and
// the constraints phase must NOT list a second ADD CONSTRAINT for it.
func TestPreviewDDL_KeylessInlinePromotedConstraintNotDuplicated(t *testing.T) {
	w := &SchemaWriter{schema: "public"}
	keyless := &ir.Table{
		Name: "audit",
		Columns: []*ir.Column{
			{Name: "entry_id", Type: ir.Integer{Width: 64}},
		},
		Indexes: []*ir.Index{
			{Name: "audit_entry_id_unique", Unique: true, ConstraintBacked: true, Columns: []ir.IndexColumn{{Column: "entry_id"}}},
		},
	}
	stmts, err := w.PreviewDDL(context.Background(), &ir.Schema{Tables: []*ir.Table{keyless}})
	if err != nil {
		t.Fatalf("PreviewDDL: %v", err)
	}
	inline, alterAdds := false, 0
	for _, st := range stmts {
		if st.Kind == "CREATE TABLE" && strings.Contains(st.SQL, `CONSTRAINT "audit_entry_id_unique" UNIQUE`) {
			inline = true
		}
		if strings.Contains(st.SQL, `ADD CONSTRAINT "audit_entry_id_unique"`) {
			alterAdds++
		}
	}
	if !inline {
		t.Errorf("keyless table did not inline-promote the unique key as a CONSTRAINT; stmts: %+v", stmts)
	}
	if alterAdds != 0 {
		t.Errorf("inline-promoted constraint also previewed %d ADD CONSTRAINT statement(s); want 0", alterAdds)
	}
}
