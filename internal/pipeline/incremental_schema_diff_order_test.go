// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func diffOrderTestTable() *ir.Table {
	return &ir.Table{
		Schema: "public",
		Name:   "medium_11",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "user_id", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Name: "medium_11_pkey", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "medium_11_created_at_idx", Columns: []ir.IndexColumn{{Column: "created_at"}}},
			{Name: "medium_11_event_type_idx", Columns: []ir.IndexColumn{{Column: "event_type"}}},
			{Name: "medium_11_user_id_idx", Columns: []ir.IndexColumn{{Column: "user_id"}}},
		},
	}
}

// TestDiffSchemas_IndexOrderIsNotAnAlter pins task #41's diff half:
// the identical index SET presented in different order is NOT an
// alter_table delta. Pre-fix, tablesEqual compared Indexes
// positionally, and pre-task-#41 manifests recorded Indexes in
// randomized map order — so a DDL-free incremental whose end-of-window
// catalog read ordered indexes differently than the parent manifest
// emitted phantom alter_table entries (observed live: schema_deltas=6
// across 6 untouched tables, 2026-06-10 backup benchmark — the exact
// rotation reproduced here).
func TestDiffSchemas_IndexOrderIsNotAnAlter(t *testing.T) {
	before := &ir.Schema{Tables: []*ir.Table{diffOrderTestTable()}}
	after := &ir.Schema{Tables: []*ir.Table{diffOrderTestTable()}}
	// Rotate: created_at,event_type,user_id → event_type,user_id,created_at.
	idx := after.Tables[0].Indexes
	after.Tables[0].Indexes = []*ir.Index{idx[1], idx[2], idx[0]}

	if deltas := diffSchemas(before, after); len(deltas) != 0 {
		t.Errorf("index reorder produced %d schema deltas; want 0 (kind=%v)", len(deltas), deltas[0].Kind)
	}

	// A REAL index difference must still surface as an alter.
	altered := &ir.Schema{Tables: []*ir.Table{diffOrderTestTable()}}
	altered.Tables[0].Indexes[1].Unique = true
	deltas := diffSchemas(before, altered)
	if len(deltas) != 1 || deltas[0].Kind != ir.SchemaDeltaAlterTable {
		t.Errorf("real index change: deltas = %+v; want exactly one alter_table", deltas)
	}

	// Same index NAMES but different membership (rename shape) must
	// also surface — the name-keyed set compare may not weaken the
	// real-difference detection.
	renamed := &ir.Schema{Tables: []*ir.Table{diffOrderTestTable()}}
	renamed.Tables[0].Indexes[0].Name = "medium_11_other_idx"
	deltas = diffSchemas(before, renamed)
	if len(deltas) != 1 || deltas[0].Kind != ir.SchemaDeltaAlterTable {
		t.Errorf("index rename: deltas = %+v; want exactly one alter_table", deltas)
	}
}
