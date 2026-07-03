// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSQLiteRoute_GeneratedAndCheck_Portable pins the ADR-0133-follow-up
// routing: a "sqlite"-dialect generated column / CHECK with a PORTABLE body
// emits the TRANSLATED Postgres form.
func TestSQLiteRoute_GeneratedAndCheck_Portable(t *testing.T) {
	opts := emitOpts{}

	gen := translateGeneratedExpr(
		&ir.Column{Type: ir.Text{}, GeneratedExpr: "a || '-' || b", GeneratedExprDialect: "sqlite"},
		nil, opts,
	)
	if want := "(a || '-' || b)"; gen != want {
		t.Errorf("generated = %q; want %q", gen, want)
	}

	chk := translateCheckExpr(
		&ir.CheckConstraint{Expr: "length(x) > 0", ExprDialect: "sqlite"}, nil, opts,
	)
	if want := "(LENGTH(x) > 0)"; chk != want {
		t.Errorf("check = %q; want %q", chk, want)
	}
}

// TestSQLiteRoute_GeneratedAndCheck_NonPortableRefused pins the F6 policy: a
// "sqlite" generated column / CHECK with a NON-portable body is REFUSED LOUDLY
// at the EMIT path (naming the column/constraint), NOT emitted verbatim — PG
// would silently accept a syntactically-valid-but-divergent body. Covered:
// strftime (a non-portable function) AND `a % b` (a SYNTACTICALLY-valid-on-PG
// operator that verbatim would silently mis-compute).
func TestSQLiteRoute_GeneratedAndCheck_NonPortableRefused(t *testing.T) {
	opts := emitOpts{}

	for _, body := range []string{"strftime('%Y', created_at)", "a % b"} {
		col := &ir.Column{
			Name: "g", Type: ir.Text{},
			GeneratedExpr: body, GeneratedStored: true, GeneratedExprDialect: "sqlite",
		}
		if _, err := emitColumnDef(nil, col, opts); err == nil {
			t.Errorf("emitColumnDef(sqlite gencol %q) err=nil; want a LOUD refusal (never verbatim)", body)
		} else if !strings.Contains(err.Error(), "g") {
			t.Errorf("emitColumnDef(%q) err=%v; want it to name the column", body, err)
		}

		chk := &ir.CheckConstraint{Name: "c_bad", Expr: body, ExprDialect: "sqlite"}
		if _, err := emitCheckConstraint(chk, nil, opts); err == nil {
			t.Errorf("emitCheckConstraint(sqlite CHECK %q) err=nil; want a LOUD refusal", body)
		} else if !strings.Contains(err.Error(), "c_bad") {
			t.Errorf("emitCheckConstraint(%q) err=%v; want it to name the constraint", body, err)
		}
	}
}

// TestSQLiteRoute_PortableGenColEmits confirms a portable "sqlite" gencol emits
// a valid GENERATED column (the refusal does NOT false-fire on a portable body).
func TestSQLiteRoute_PortableGenColEmits(t *testing.T) {
	col := &ir.Column{
		Name: "label", Type: ir.Text{},
		GeneratedExpr: "a || '-' || b", GeneratedStored: true, GeneratedExprDialect: "sqlite",
	}
	def, err := emitColumnDef(nil, col, emitOpts{})
	if err != nil {
		t.Fatalf("emitColumnDef(portable sqlite gencol): %v", err)
	}
	if !strings.Contains(def, "GENERATED ALWAYS AS ((a || '-' || b)) STORED") {
		t.Errorf("def = %q; want the translated generated body", def)
	}
}

// TestSQLiteRoute_Index_PortableEmitted pins that a portable "sqlite"
// expression index and partial predicate emit the translated form.
func TestSQLiteRoute_Index_PortableEmitted(t *testing.T) {
	idx := &ir.Index{
		Name: "ix_expr",
		Columns: []ir.IndexColumn{
			{Expression: "coalesce(email, '')", ExpressionDialect: "sqlite"},
		},
		Predicate:        "deleted_at IS NULL",
		PredicateDialect: "sqlite",
	}
	stmt, err := emitCreateIndex("public", "users", idx, emitOpts{})
	if err != nil {
		t.Fatalf("emitCreateIndex: %v", err)
	}
	if stmt == "" {
		t.Fatal("portable expression index was skipped; want it emitted")
	}
	if !strings.Contains(stmt, "(COALESCE(email, ''))") {
		t.Errorf("stmt = %q; want translated expression (COALESCE(email, ''))", stmt)
	}
	if !strings.Contains(stmt, "WHERE (deleted_at IS NULL)") {
		t.Errorf("stmt = %q; want translated predicate WHERE (deleted_at IS NULL)", stmt)
	}
}

// TestSQLiteRoute_Index_NonPortableSkipped pins the index WARN-skip policy: a
// non-portable "sqlite" expression index / partial predicate is skipped
// (empty stmt, nil error) rather than aborting the migration.
func TestSQLiteRoute_Index_NonPortableSkipped(t *testing.T) {
	exprIdx := &ir.Index{
		Name: "ix_bad_expr",
		Columns: []ir.IndexColumn{
			{Expression: "strftime('%Y', d)", ExpressionDialect: "sqlite"},
		},
	}
	if stmt, err := emitCreateIndex("public", "t", exprIdx, emitOpts{}); err != nil || stmt != "" {
		t.Errorf("non-portable expr index = (%q, %v); want (\"\", nil) — WARN-skip", stmt, err)
	}

	predIdx := &ir.Index{
		Name:             "ix_bad_pred",
		Columns:          []ir.IndexColumn{{Column: "id"}},
		Predicate:        "julianday(d) > 0",
		PredicateDialect: "sqlite",
	}
	if stmt, err := emitCreateIndex("public", "t", predIdx, emitOpts{}); err != nil || stmt != "" {
		t.Errorf("non-portable partial index = (%q, %v); want (\"\", nil) — WARN-skip", stmt, err)
	}
}
