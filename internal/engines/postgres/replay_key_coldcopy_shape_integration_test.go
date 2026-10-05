//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestColdCopyCreate_PKLessNotNullUnique_IsKeyedWhileItCopies measures the
// premise behind the "shape (b)" availability concern on the target sluice
// itself creates: a source table with no PRIMARY KEY and a NOT NULL UNIQUE
// index. The concern was that the cold copy creates the table with its
// PRIMARY KEY only and adds UNIQUE indexes after the data, so the cold-copy
// retry gate would find no target key and refuse every transient. That is not
// what the schema writer does: for a PK-less table it promotes the chosen
// NOT NULL UNIQUE inline at CREATE TABLE as a UNIQUE CONSTRAINT (Bug 125's
// cross-engine symmetry in emitTableDef, so ON CONFLICT has an index to
// infer against), so the key exists while the rows copy and the gate's probe
// answers keyed. This pins it on a real server, through the real
// CreateTablesWithoutConstraints and the real ProbeReplayKey, for the shapes
// that matter:
//
//   - one NOT NULL UNIQUE: promoted, keyed;
//   - two NOT NULL UNIQUEs: one promoted (the picker's choice), keyed — the
//     other arrives with the index phase; the probe judges the ON CONFLICT
//     arbiter, which is the promoted key;
//   - a NOT NULL UNIQUE over a GENERATED column: promoted, but a generated
//     key part never qualifies (no writer binds it), so keyless — the gate
//     still refuses there, conservatively, which this records.
func TestColdCopyCreate_PKLessNotNullUnique_IsKeyedWhileItCopies(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	vc := func(name string) *ir.Column { return &ir.Column{Name: name, Type: ir.Varchar{Length: 64}} }
	uq := func(name string, cols ...string) *ir.Index {
		idx := &ir.Index{Name: name, Unique: true}
		for _, c := range cols {
			idx.Columns = append(idx.Columns, ir.IndexColumn{Column: c})
		}
		return idx
	}
	gen := &ir.Column{Name: "g", Type: ir.Varchar{Length: 64}, GeneratedExpr: "upper(email)", GeneratedStored: true}
	cases := []struct {
		table     *ir.Table
		wantKeyed bool
	}{
		{&ir.Table{
			Name: "shape_b_one_unique", Columns: []*ir.Column{vc("email"), {Name: "v", Type: ir.Text{}, Nullable: true}},
			Indexes: []*ir.Index{uq("uq_shape_b_one_email", "email")},
		}, true},
		{&ir.Table{
			Name: "shape_b_two_uniques", Columns: []*ir.Column{vc("email"), vc("handle"), {Name: "v", Type: ir.Text{}, Nullable: true}},
			Indexes: []*ir.Index{uq("uq_shape_b_two_email", "email"), uq("uq_shape_b_two_handle", "handle")},
		}, true},
		{&ir.Table{
			Name: "shape_b_generated_unique", Columns: []*ir.Column{vc("email"), gen},
			Indexes: []*ir.Index{uq("uq_shape_b_gen_g", "g")},
		}, false},
	}
	schema := &ir.Schema{}
	for _, c := range cases {
		schema.Tables = append(schema.Tables, c.table)
	}

	swAny, err := (Engine{}).OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() {
		if c, ok := swAny.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := swAny.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	rwAny, err := (Engine{}).OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer func() {
		if c, ok := rwAny.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	prober := rwAny.(ir.ReplayKeyProber)
	for _, c := range cases {
		exists, keyed, err := prober.ProbeReplayKey(ctx, c.table)
		if err != nil || !exists || keyed != c.wantKeyed {
			t.Errorf("%s right after CREATE (before any index phase): ProbeReplayKey = (%v, %v, %v), want (true, %v, nil)",
				c.table.Name, exists, keyed, err, c.wantKeyed)
		}
	}
}
