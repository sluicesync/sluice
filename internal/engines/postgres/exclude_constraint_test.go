// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// ADR-0053 — PG writer pins for ir.ExcludeConstraint emission. The
// writer inlines EXCLUDE constraints inside CREATE TABLE alongside
// CHECKs (mirroring the existing precedent); each Definition is the
// pg_get_constraintdef body without the `ALTER TABLE ... ADD
// CONSTRAINT <name>` wrapper, so the writer prefixes it with
// `CONSTRAINT <name>` to make a valid clause.

// TestEmitTableDef_ExcludeConstraints_InlineEmission covers the four
// observed real-world EXCLUDE shapes from the GitLab corpus, asserts
// each lands as a `CONSTRAINT "<name>" <Definition>` clause inside the
// CREATE TABLE body, and pins that the verbatim Definition is carried
// through byte-exact (no normalisation).
func TestEmitTableDef_ExcludeConstraints_InlineEmission(t *testing.T) {
	table := &ir.Table{
		Schema: "public",
		Name:   "schedule_slots",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}, Nullable: false},
		},
		ExcludeConstraints: []*ir.ExcludeConstraint{
			{
				Name:       "simple_overlap",
				Definition: "EXCLUDE USING gist (builds_id_range WITH &&)",
			},
			{
				Name:       "predicated_overlap",
				Definition: "EXCLUDE USING gist (builds_id_range WITH &&) WHERE ((builds_id_range IS NOT NULL))",
			},
			{
				Name:       "multikey_overlap",
				Definition: "EXCLUDE USING gist (rotation_id WITH =, tstzrange(starts_at, ends_at, '[)'::text) WITH &&)",
			},
			{
				Name:       "deferrable_overlap",
				Definition: "EXCLUDE USING gist (id WITH &&) WHERE ((id IS NOT NULL)) DEFERRABLE INITIALLY DEFERRED",
			},
		},
	}

	sql, err := emitTableDef("public", table, emitOpts{})
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}

	// Each constraint must appear as `CONSTRAINT "<name>" <Definition>`
	// in the emitted CREATE TABLE body. Byte-exact substring search:
	// any deviation (quoting drift, whitespace mangling, lost token)
	// is a regression we want to surface loudly.
	for _, ex := range table.ExcludeConstraints {
		want := `CONSTRAINT "` + ex.Name + `" ` + ex.Definition
		if !strings.Contains(sql, want) {
			t.Errorf("emitTableDef missing inline EXCLUDE clause %q\n--- emitted ---\n%s", want, sql)
		}
	}
}

// TestEmitTableDef_ExcludeConstraint_EmptyDefinitionRefuses pins the
// sluice-bug-not-silent-loss path: an EXCLUDE constraint with an
// empty Definition is a reader-side bug (pg_get_constraintdef never
// returns empty for a valid contype='x' row). Surface loudly rather
// than emitting `CONSTRAINT "<name>"` followed by nothing — that
// would be invalid PG syntax + a confusing failure mode.
func TestEmitTableDef_ExcludeConstraint_EmptyDefinitionRefuses(t *testing.T) {
	table := &ir.Table{
		Schema: "public",
		Name:   "broken",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
		},
		ExcludeConstraints: []*ir.ExcludeConstraint{
			{Name: "broken_exclude", Definition: ""},
		},
	}
	_, err := emitTableDef("public", table, emitOpts{})
	if err == nil {
		t.Fatal("expected loud refusal on empty Definition; got nil")
	}
	if !strings.Contains(err.Error(), "empty Definition") &&
		!strings.Contains(err.Error(), "sluice bug") {
		t.Errorf("error message %q did not name the sluice-bug condition", err.Error())
	}
}

// TestEmitTableDef_ExcludeConstraints_AbsentWhenEmpty regression-
// guards the no-EXCLUDE path: a table with no EXCLUDE constraints
// must emit NO EXCLUDE / CONSTRAINT-prefix-with-empty-name lines.
// Belt-and-braces against accidental "always emit something"
// regressions in the writer loop.
func TestEmitTableDef_ExcludeConstraints_AbsentWhenEmpty(t *testing.T) {
	table := &ir.Table{
		Schema: "public",
		Name:   "no_excludes",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
		},
	}
	sql, err := emitTableDef("public", table, emitOpts{})
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	if strings.Contains(sql, "EXCLUDE") {
		t.Errorf("emitted SQL unexpectedly contains EXCLUDE for a table with no EXCLUDE constraints:\n%s", sql)
	}
}
