// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestSQLiteRoute_GeneratedAndCheck_Portable pins the ADR-0133-follow-up
// routing on MySQL: a "sqlite" generated column / CHECK with a PORTABLE body
// emits the TRANSLATED MySQL form (|| → CONCAT, length → CHAR_LENGTH).
func TestSQLiteRoute_GeneratedAndCheck_Portable(t *testing.T) {
	gen := translateGeneratedExpr(
		&ir.Column{Type: ir.Text{}, GeneratedExpr: "a || '-' || b", GeneratedExprDialect: "sqlite"},
	)
	if want := "CONCAT(a, '-', b)"; gen != want {
		t.Errorf("generated = %q; want %q", gen, want)
	}

	chk := translateCheckExpr(
		&ir.CheckConstraint{Expr: "length(x) > 0", ExprDialect: "sqlite"},
	)
	if want := "(CHAR_LENGTH(x) > 0)"; chk != want {
		t.Errorf("check = %q; want %q", chk, want)
	}
}

// TestSQLiteRoute_GeneratedAndCheck_NonPortableRefused pins the F6 policy on
// MySQL: a non-portable "sqlite" gencol / CHECK is REFUSED LOUDLY at the emit
// path (naming the column/constraint), never emitted verbatim. Covered:
// strftime (non-portable function), `a / b` (SQLite integer division vs MySQL
// decimal — MySQL would SILENTLY accept and mis-compute), and `a % b`.
func TestSQLiteRoute_GeneratedAndCheck_NonPortableRefused(t *testing.T) {
	for _, body := range []string{"strftime('%Y', created_at)", "a / b", "a % b"} {
		col := &ir.Column{
			Name: "g", Type: ir.Integer{Width: 64},
			GeneratedExpr: body, GeneratedStored: true, GeneratedExprDialect: "sqlite",
		}
		if _, err := emitColumnDef("t", col); err == nil {
			t.Errorf("emitColumnDef(sqlite gencol %q) err=nil; want a LOUD refusal (never verbatim)", body)
		} else if !strings.Contains(err.Error(), "g") {
			t.Errorf("emitColumnDef(%q) err=%v; want it to name the column", body, err)
		}

		chk := &ir.CheckConstraint{Name: "c_bad", Expr: body, ExprDialect: "sqlite"}
		if _, err := emitCheckConstraint(chk); err == nil {
			t.Errorf("emitCheckConstraint(sqlite CHECK %q) err=nil; want a LOUD refusal", body)
		} else if !strings.Contains(err.Error(), "c_bad") {
			t.Errorf("emitCheckConstraint(%q) err=%v; want it to name the constraint", body, err)
		}
	}
}

// TestSQLiteRoute_PortableGenColEmits confirms a portable "sqlite" gencol emits
// a valid GENERATED column on MySQL (refusal does not false-fire).
func TestSQLiteRoute_PortableGenColEmits(t *testing.T) {
	col := &ir.Column{
		Name: "label", Type: ir.Text{},
		GeneratedExpr: "a || '-' || b", GeneratedStored: true, GeneratedExprDialect: "sqlite",
	}
	def, err := emitColumnDef("t", col)
	if err != nil {
		t.Fatalf("emitColumnDef(portable sqlite gencol): %v", err)
	}
	if !strings.Contains(def, "GENERATED ALWAYS AS (CONCAT(a, '-', b)) STORED") {
		t.Errorf("def = %q; want the translated CONCAT generated body", def)
	}
}

// TestSQLiteRoute_Index_PortableEmitted pins that a portable "sqlite"
// expression index emits the translated MySQL form.
func TestSQLiteRoute_Index_PortableEmitted(t *testing.T) {
	idx := &ir.Index{
		Name: "ix_expr",
		Columns: []ir.IndexColumn{
			{Expression: "coalesce(email, '')", ExpressionDialect: "sqlite"},
		},
	}
	stmt, err := emitCreateIndex("users", idx)
	if err != nil {
		t.Fatalf("emitCreateIndex: %v", err)
	}
	if stmt == "" {
		t.Fatal("portable expression index was skipped; want it emitted")
	}
	if !strings.Contains(stmt, "(COALESCE(email, ''))") {
		t.Errorf("stmt = %q; want translated expression (COALESCE(email, ''))", stmt)
	}
}

// TestSQLiteRoute_BackslashLiteral_RefusedNamed pins SEC-1 on the MySQL
// writer across every construct family a SQLite string literal can reach:
// generated column and CHECK refuse LOUDLY with an error NAMING the
// backslash (not the generic non-portable message), the index-expression
// path WARN-skips (never emits), and the DEFAULT-expression path (portable
// bodies translate, non-portable keep the verbatim carry) refuses at
// emitColumnDef. Shapes cover the plain interior backslash and
// the trailing-\ quote-swallow; backslash-free controls prove no
// false-fire.
func TestSQLiteRoute_BackslashLiteral_RefusedNamed(t *testing.T) {
	// Generated column: plain interior backslash.
	col := &ir.Column{
		Name: "g_bs", Type: ir.Text{},
		GeneratedExpr: `a || 'C:\temp'`, GeneratedStored: true, GeneratedExprDialect: "sqlite",
	}
	if _, err := emitColumnDef("t", col); err == nil {
		t.Error(`emitColumnDef("t", gencol a || 'C:\temp') err=nil; want a LOUD backslash refusal`)
	} else {
		if !strings.Contains(err.Error(), "backslash") || !strings.Contains(err.Error(), "g_bs") {
			t.Errorf("gencol refusal = %v; want it to name the backslash and the column", err)
		}
		// The SEC-1 refusal carries the stable code as metadata
		// (docs/operator/error-codes.md); prose unchanged.
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeExprBackslashLiteral {
			t.Errorf("gencol refusal code = %v (found=%v); want %q", ce, ok, sluicecode.CodeExprBackslashLiteral)
		}
	}

	// CHECK: the trailing-\ quote-swallow shape.
	chk := &ir.CheckConstraint{Name: "ck_bs", Expr: `a <> 'x\'`, ExprDialect: "sqlite"}
	if _, err := emitCheckConstraint(chk); err == nil {
		t.Error(`emitCheckConstraint(a <> 'x\') err=nil; want a LOUD backslash refusal`)
	} else if !strings.Contains(err.Error(), "backslash") || !strings.Contains(err.Error(), "ck_bs") {
		t.Errorf("CHECK refusal = %v; want it to name the backslash and the constraint", err)
	}

	// Index expression: WARN-skip (an index is a performance object), and the
	// verbatim fallback must never emit.
	idx := &ir.Index{
		Name: "ix_bs",
		Columns: []ir.IndexColumn{
			{Expression: `coalesce(a, 'x\y')`, ExpressionDialect: "sqlite"},
		},
	}
	if stmt, err := emitCreateIndex("t", idx); err != nil || stmt != "" {
		t.Errorf("backslash-literal expr index = (%q, %v); want (\"\", nil) — WARN-skip, never verbatim", stmt, err)
	}

	// DEFAULT expression: the ADR-0133 verbatim-carry boundary.
	colD := &ir.Column{
		Name: "d_bs", Type: ir.Varchar{Length: 50},
		Default: ir.DefaultExpression{Expr: `coalesce(NULL, 'C:\temp')`, Dialect: "sqlite"},
	}
	if _, err := emitColumnDef("t", colD); err == nil {
		t.Error(`emitColumnDef("t", DEFAULT ('C:\' || 'x')) err=nil; want a LOUD backslash refusal`)
	} else {
		if !strings.Contains(err.Error(), "backslash") || !strings.Contains(err.Error(), "d_bs") {
			t.Errorf("DEFAULT refusal = %v; want it to name the backslash and the column", err)
		}
		// The DEFAULT-position sweep shares the same code as the
		// expression positions — one code per class.
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeExprBackslashLiteral {
			t.Errorf("DEFAULT refusal code = %v (found=%v); want %q", ce, ok, sluicecode.CodeExprBackslashLiteral)
		}
	}

	// Controls: identical shapes without the backslash still emit; the
	// DEFAULT control also pins that a NON-sqlite dialect with a backslash is
	// out of this guard's jurisdiction (it belongs to that dialect's own
	// policy, not the sqlite verbatim-carry one).
	okCol := &ir.Column{
		Name: "g_ok", Type: ir.Text{},
		GeneratedExpr: `a || 'C:temp'`, GeneratedStored: true, GeneratedExprDialect: "sqlite",
	}
	if _, err := emitColumnDef("t", okCol); err != nil {
		t.Errorf("emitColumnDef(backslash-free gencol) = %v; want nil", err)
	}
	okD := &ir.Column{
		Name: "d_ok", Type: ir.Varchar{Length: 50},
		Default: ir.DefaultExpression{Expr: `('a' || 'b')`, Dialect: "sqlite"},
	}
	if _, err := emitColumnDef("t", okD); err != nil {
		t.Errorf("emitColumnDef(backslash-free sqlite DEFAULT) = %v; want nil", err)
	}
	if err := refuseBackslashSQLiteDefaultMySQL("d", ir.DefaultExpression{Expr: `'a\b'`, Dialect: "postgres"}); err != nil {
		t.Errorf("refuseBackslashSQLiteDefaultMySQL(postgres dialect) = %v; want nil (sqlite-carry guard only)", err)
	}
	// A DefaultLiteral is outside this guard's jurisdiction because the
	// literal path is quoted by quoteSQLString, which re-escapes backslashes
	// itself (SEC-1 review gap 2) — the value is carried LOSSLESSLY there,
	// no refusal needed. The quoting is pinned in TestQuoteSQLString; the
	// emitted DEFAULT clause shape is pinned here.
	if err := refuseBackslashSQLiteDefaultMySQL("d", ir.DefaultLiteral{Value: `a\b`}); err != nil {
		t.Errorf("refuseBackslashSQLiteDefaultMySQL(DefaultLiteral) = %v; want nil (the literal path re-escapes losslessly)", err)
	}
	litCol := &ir.Column{
		Name: "d_lit", Type: ir.Varchar{Length: 50},
		Default: ir.DefaultLiteral{Value: `C:\temp`},
	}
	def, err := emitColumnDef("t", litCol)
	if err != nil {
		t.Fatalf("emitColumnDef(DefaultLiteral with backslash) = %v; want nil (lossless re-escape)", err)
	}
	if !strings.Contains(def, `DEFAULT 'C:\\temp'`) {
		t.Errorf("DefaultLiteral emit = %q; want the backslash DOUBLED in the DEFAULT clause", def)
	}
}

// TestSQLiteRoute_DoubleQuoted_RefusedNamed pins SEC-1 review gap 1 on the
// MySQL writer: a "…" double-quoted token — an identifier or the
// double-quoted-string misfeature under SQLite's rules, but a STRING LITERAL
// with escape semantics under MySQL's default sql_mode (no ANSI_QUOTES) —
// refuses LOUDLY naming the mechanism on the data-load-bearing constructs
// (gencol, CHECK) and WARN-skips on the index path, REGARDLESS of backslash
// content. The reviewer-derived cells: gencol `a || "C:\temp"`, CHECK
// `a <> "x\"`, index `coalesce(a, "z\q")`. DEFAULT is asymmetric (probed on
// modernc): SQLite rejects a double-quoted token inside a parenthesised
// DEFAULT expression as non-constant, but ACCEPTS the bare literal-value
// form `DEFAULT "a\b"` — so the DEFAULT sweep refuses only the
// backslash-bearing double-quoted shape (the silent-reinterpretation
// hazard) while the backslash-free `DEFAULT "draft"` keeps its verbatim
// carry (both engines read the same string value there).
func TestSQLiteRoute_DoubleQuoted_RefusedNamed(t *testing.T) {
	col := &ir.Column{
		Name: "g_dq", Type: ir.Text{},
		GeneratedExpr: `a || "C:\temp"`, GeneratedStored: true, GeneratedExprDialect: "sqlite",
	}
	if _, err := emitColumnDef("t", col); err == nil {
		t.Error(`emitColumnDef("t", gencol a || "C:\temp") err=nil; want a LOUD double-quoted refusal`)
	} else if !strings.Contains(err.Error(), "double-quoted") || !strings.Contains(err.Error(), "g_dq") {
		t.Errorf("gencol refusal = %v; want it to name the double-quoted token and the column", err)
	}

	chk := &ir.CheckConstraint{Name: "ck_dq", Expr: `a <> "x\"`, ExprDialect: "sqlite"}
	if _, err := emitCheckConstraint(chk); err == nil {
		t.Error(`emitCheckConstraint(a <> "x\") err=nil; want a LOUD double-quoted refusal`)
	} else if !strings.Contains(err.Error(), "double-quoted") || !strings.Contains(err.Error(), "ck_dq") {
		t.Errorf("CHECK refusal = %v; want it to name the double-quoted token and the constraint", err)
	}

	idx := &ir.Index{
		Name: "ix_dq",
		Columns: []ir.IndexColumn{
			{Expression: `coalesce(a, "z\q")`, ExpressionDialect: "sqlite"},
		},
	}
	if stmt, err := emitCreateIndex("t", idx); err != nil || stmt != "" {
		t.Errorf("double-quoted expr index = (%q, %v); want (\"\", nil) — WARN-skip, never emitted", stmt, err)
	}

	// Refuse-regardless: a backslash-FREE double-quoted token is still a
	// silent meaning change on MySQL (identifier → string literal, vacating
	// the predicate), so it refuses too.
	noBs := &ir.CheckConstraint{Name: "ck_dq2", Expr: `"status" <> ''`, ExprDialect: "sqlite"}
	if _, err := emitCheckConstraint(noBs); err == nil {
		t.Error(`emitCheckConstraint("status" <> '') err=nil; want a LOUD double-quoted refusal (no backslash needed)`)
	} else if !strings.Contains(err.Error(), "double-quoted") {
		t.Errorf("backslash-free DQS refusal = %v; want it to name the double-quoted token", err)
	}

	// DEFAULT position, the bare misfeature form SQLite accepts: a
	// backslash inside the double-quoted token is the same silent-
	// reinterpretation hazard as a single-quoted literal (MySQL reads
	// "a\b" as a string WITH escape semantics), and the single-quote
	// tokenizer check misses it — the DEFAULT sweep must refuse it.
	dqBs := &ir.Column{
		Name: "d_dqbs", Type: ir.Varchar{Length: 50},
		Default: ir.DefaultExpression{Expr: `"a\b"`, Dialect: "sqlite"},
	}
	if _, err := emitColumnDef("t", dqBs); err == nil {
		t.Error(`emitColumnDef("t", DEFAULT "a\b") err=nil; want a LOUD backslash refusal`)
	} else if !strings.Contains(err.Error(), "backslash") || !strings.Contains(err.Error(), "d_dqbs") {
		t.Errorf("DEFAULT dq-backslash refusal = %v; want it to name the backslash and the column", err)
	}
	// Backslash-free `DEFAULT "draft"` keeps its verbatim carry (pinned in
	// TestWriterDialectGuard_DefaultExpr_MySQL) — no refusal.
	if err := refuseBackslashSQLiteDefaultMySQL("d", ir.DefaultExpression{Expr: `"draft"`, Dialect: "sqlite"}); err != nil {
		t.Errorf("refuseBackslashSQLiteDefaultMySQL(\"draft\") = %v; want nil (backslash-free misfeature keeps the verbatim carry)", err)
	}
}

// TestSQLiteRoute_Index_NonPortableSkipped pins the index WARN-skip on MySQL,
// on both the single-emit and combined-emit paths.
func TestSQLiteRoute_Index_NonPortableSkipped(t *testing.T) {
	bad := &ir.Index{
		Name: "ix_bad_expr",
		Columns: []ir.IndexColumn{
			{Expression: "strftime('%Y', d)", ExpressionDialect: "sqlite"},
		},
	}
	if stmt, err := emitCreateIndex("t", bad); err != nil || stmt != "" {
		t.Errorf("non-portable expr index = (%q, %v); want (\"\", nil) — WARN-skip", stmt, err)
	}

	good := &ir.Index{Name: "ix_ok", Columns: []ir.IndexColumn{{Column: "id"}}}
	stmts, err := emitCreateIndexesCombined("t", []*ir.Index{bad, good})
	if err != nil {
		t.Fatalf("emitCreateIndexesCombined: %v", err)
	}
	joined := strings.Join(stmts, "\n")
	if strings.Contains(joined, "ix_bad_expr") {
		t.Errorf("combined stmts = %q; the non-portable index must be skipped", joined)
	}
	if !strings.Contains(joined, "ix_ok") {
		t.Errorf("combined stmts = %q; the portable index must survive", joined)
	}
}
