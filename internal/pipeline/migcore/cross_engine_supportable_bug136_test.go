// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the Bug 136 wire-up: an index key part on a column
// that lands on MySQL as TEXT/BLOB/JSON (no inline key length) must
// refuse at CheckCrossEngineSupportable — the shared pre-DDL
// chokepoint for migrate, chain restore, and restore — instead of
// failing with MySQL Error 1170 at create-indexes, after bulk copy.
// The scan itself (full type-family × index-shape matrix) is pinned in
// internal/translate/text_index_test.go; these tests pin the pipeline
// wiring and the same-engine no-fire contract.

package migcore

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// bug136Schema is a PG-shaped schema with a UNIQUE index on an
// unbounded text column — the canonical Bug 136 repro.
func bug136Schema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Text{Size: ir.TextLong}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{{
			Name:    "users_email_key",
			Unique:  true,
			Kind:    ir.IndexKindBTree,
			Columns: []ir.IndexColumn{{Column: "email"}},
		}},
	}}}
}

// TestCheckCrossEngineSupportable_PGtoMySQL_TextIndexRefuses pins the
// Bug 136 refusal at the shared chokepoint: PG → MySQL with a UNIQUE
// index on a text column refuses BEFORE any DDL/data moves, naming the
// row, the index, and the --type-override workaround.
func TestCheckCrossEngineSupportable_PGtoMySQL_TextIndexRefuses(t *testing.T) {
	err := CheckCrossEngineSupportable(bug136Schema(), "postgres", "mysql", "migrate")
	if err == nil {
		t.Fatal("err = nil; want Bug 136 text-index refusal")
	}
	for _, want := range []string{
		"users.email",     // the offending row named
		"users_email_key", // the offending index named
		"Error 1170",      // the late failure it pre-empts
		"--type-override", // the escape hatch
		"varchar(N)",      // ... with the concrete shape
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal missing %q\n--- got ---\n%v", want, err)
		}
	}
}

// TestCheckCrossEngineSupportable_SameEngineTextIndexAllowed pins the
// same-engine no-fire contract: MySQL → MySQL re-emits a TEXT column's
// prefix index verbatim (ir.IndexColumn.Length carries the prefix),
// and PG → PG never consults MySQL's key-length rules — neither pair
// may trip the Bug 136 refusal.
func TestCheckCrossEngineSupportable_SameEngineTextIndexAllowed(t *testing.T) {
	prefixed := &ir.Schema{Tables: []*ir.Table{{
		Name: "docs",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "body", Type: ir.Text{Size: ir.TextRegular}},
		},
		Indexes: []*ir.Index{{
			Name:    "body_prefix_idx",
			Columns: []ir.IndexColumn{{Column: "body", Length: 64}},
		}},
	}}}
	if err := CheckCrossEngineSupportable(prefixed, "mysql", "mysql", "migrate"); err != nil {
		t.Errorf("MySQL → MySQL prefix-indexed TEXT: err = %v; want nil", err)
	}
	if err := CheckCrossEngineSupportable(bug136Schema(), "postgres", "postgres", "migrate"); err != nil {
		t.Errorf("PG → PG text index: err = %v; want nil", err)
	}
}

// TestCheckCrossEngineSupportable_PGtoMySQL_OverriddenTextIndexAllowed
// pins the escape hatch end-to-end at the chokepoint: the check runs
// on the post-ApplyMappings schema, so a column already overridden to
// a bounded varchar (what `--type-override users.email=varchar(255)`
// produces) must pass.
func TestCheckCrossEngineSupportable_PGtoMySQL_OverriddenTextIndexAllowed(t *testing.T) {
	s := bug136Schema()
	s.Tables[0].Columns[1].Type = ir.Varchar{Length: 255}
	if err := CheckCrossEngineSupportable(s, "postgres", "mysql", "migrate"); err != nil {
		t.Errorf("overridden column: err = %v; want nil", err)
	}
}

// TestCheckCrossEngineDeltaSupportable_TextIndexRefuses pins that the
// incremental schema-delta path (AddTable carrying a text-indexed
// table) trips the same refusal — chain restore's cross-engine deltas
// share the chokepoint.
func TestCheckCrossEngineDeltaSupportable_TextIndexRefuses(t *testing.T) {
	deltas := []*irbackup.SchemaDeltaEntry{{
		Kind:  irbackup.SchemaDeltaAddTable,
		Table: "users",
		After: bug136Schema().Tables[0],
	}}
	err := CheckCrossEngineDeltaSupportable(deltas, "postgres", "mysql", "bk-1")
	if err == nil {
		t.Fatal("err = nil; want Bug 136 refusal through the delta path")
	}
	if !strings.Contains(err.Error(), "users.email") {
		t.Errorf("err = %v; want mention of users.email", err)
	}
}
