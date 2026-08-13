// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The SQLite C1-1 validator pins. The valid set covers everything the
// emitters produce PLUS the shapes stage_d1's preserved user-authored
// sqlite_master.sql can carry (comments, every identifier-quoting
// style SQLite accepts); the hostile set is the injection family the
// modernc premise pin proves would execute.
func TestAssertSingleDDLStatement_SQLite(t *testing.T) {
	valid := []struct{ name, stmt string }{
		{"plain create table", `CREATE TABLE "users" ("id" INTEGER PRIMARY KEY, "name" TEXT)`},
		{"semicolon inside a string literal", `CREATE TABLE "t" ("c" TEXT CHECK ("c" <> 'a;b'))`},
		{"doubled quotes inside a literal", `CREATE TABLE "t" ("c" TEXT DEFAULT 'O''Reilly; ok')`},
		{"semicolon inside a quoted identifier", `CREATE TABLE "we;ird" ("id" INTEGER)`},
		{"bracket identifier with a semicolon", `CREATE TABLE [we;ird] ([id] INTEGER)`},
		{"backtick identifier with a semicolon", "CREATE TABLE `we;ird` (`id` INTEGER)"},
		{"line comment carrying a semicolon", "CREATE TABLE t (id INTEGER) -- tool; note"},
		{"block comment carrying a semicolon", `CREATE TABLE t (id INTEGER /* legacy; column */)`},
		{"trailing semicolon", `CREATE INDEX "i" ON "t" ("c");`},
		{"trailing semicolon then a comment", "CREATE INDEX i ON t (c); -- created by tool"},
		{"nested balanced parens", `CREATE TABLE t (c TEXT CHECK ((("c" || 'x') <> 'y')))`},
	}
	for _, tc := range valid {
		if err := assertSingleDDLStatement(tc.stmt); err != nil {
			t.Errorf("%s: refused a valid single statement: %v\n  stmt: %s", tc.name, err, tc.stmt)
		}
	}

	hostile := []struct{ name, stmt string }{
		{
			"the injection family: a CHECK body closes the parens and appends SQL",
			`CREATE TABLE "t" ("c" TEXT, CHECK (1)); DROP TABLE "canary"; CREATE TABLE "x" ("y" INTEGER CHECK ((1)))`,
		},
		{"two statements", `CREATE TABLE a (id INTEGER); CREATE TABLE b (id INTEGER)`},
		{"semicolon inside an open paren group", `CREATE TABLE t (c TEXT CHECK (1; DROP TABLE canary))`},
		{"unbalanced open paren", `CREATE TABLE t (c TEXT CHECK ((1)`},
		{"unbalanced close paren", `CREATE TABLE t (c TEXT CHECK (1)))`},
		{"unterminated string literal", `CREATE TABLE t (c TEXT DEFAULT 'oops)`},
		{"unterminated bracket identifier", `CREATE TABLE [t (c TEXT)`},
		{"unterminated block comment", `CREATE TABLE t (id INTEGER) /* open`},
		{"statement hidden after a trailing comment", "CREATE TABLE a (x INTEGER); -- note\nDROP TABLE canary"},
	}
	for _, tc := range hostile {
		if err := assertSingleDDLStatement(tc.stmt); err == nil {
			t.Errorf("%s: PASSED the single-statement door — the extra SQL would execute\n  stmt: %s", tc.name, tc.stmt)
		}
	}
}

// TestSQLiteInjection_RefusedBeforeExec is the end-to-end half, and on
// this lane it needs no container: modernc is in-process, and the
// premise pin (TestModerncExecRunsMultiStatementStrings) proves the
// hostile body WOULD execute without the door. A canary table stands
// on the target; a CHECK body carrying `; DROP TABLE canary;` must
// refuse with the shared code and leave the canary standing.
func TestSQLiteInjection_RefusedBeforeExec(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "target.db")
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE canary (id INTEGER)`); err != nil {
		t.Fatalf("create canary: %v", err)
	}

	swIface, err := (Engine{}).OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() {
		if c, ok := swIface.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	hostile := &ir.Schema{Tables: []*ir.Table{{
		Name: "t_injected",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "c", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		CheckConstraints: []*ir.CheckConstraint{{
			Name: "chk_c",
			Expr: `1)); DROP TABLE canary; CREATE TABLE t_x (y INTEGER CHECK ((1`,
		}},
	}}}
	err = swIface.CreateTablesWithoutConstraints(ctx, hostile)
	if err == nil {
		t.Fatal("the sqlite schema writer accepted a CHECK body carrying `; DROP TABLE canary;`")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeDDLEmitMultiStatement {
		t.Fatalf("want %s, got: %v", sluicecode.CodeDDLEmitMultiStatement, err)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM canary`).Scan(&n); err != nil {
		t.Fatalf("the canary table is gone — the injection executed: %v", err)
	}
}

// TestSQLiteEmittedDDLGoesThroughTheSingleStatementDoor is the
// anti-drift roster, mirroring the postgres door's gate. Scope, stated
// per the gate rule: schema_writer.go and stage_d1.go — the files
// whose executed strings are built from recorded content. Enumerated
// exempt FILES (deliberately outside the roster, each with its
// reason): dump.go executes the operator's own dump SCRIPT, which is
// multi-statement BY DESIGN (the feature); row_writer.go's two sites
// build only from quoteIdent (a `;` inside a doubled-quote identifier
// cannot split a statement, and no expression bodies are inlined);
// the sqlite-trigger executor's DDL is sluice-rendered trigger install
// SQL whose recorded content is names-through-quoting only.
func TestSQLiteEmittedDDLGoesThroughTheSingleStatementDoor(t *testing.T) {
	files := []string{"schema_writer.go", "stage_d1.go", "ddl_single_statement.go"}
	exempt := map[string]bool{
		"execEmittedDDL": true, // the door itself — the one real ExecContext
		// stage_d1's row-copy PREPARED statement executes with BIND
		// ARGUMENTS; recorded values cannot split a statement.
		"stageInsertPage": true,
	}

	fset := token.NewFileSet()
	total := 0
	for _, f := range files {
		parsed, err := parser.ParseFile(fset, filepath.Join(".", f), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok {
				return true
			}
			name := fn.Name.Name
			ast.Inspect(fn, func(m ast.Node) bool {
				sel, ok := m.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "ExecContext" {
					return true
				}
				total++
				if !exempt[name] {
					pos := fset.Position(sel.Pos())
					t.Errorf("%s:%d: direct ExecContext in %s — recorded-content DDL must go through "+
						"execEmittedDDL (the C1-1 door), or the function needs an exemption HERE with a "+
						"stated reason", pos.Filename, pos.Line, name)
				}
				return true
			})
			return false
		})
	}
	if total < 2 {
		t.Fatalf("the roster walk found only %d ExecContext sites across %v — the scan is not reaching "+
			"the surface it claims to gate", total, files)
	}
	for _, f := range []string{"schema_writer.go", "stage_d1.go"} {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if !strings.Contains(string(raw), "execEmittedDDL(") {
			t.Fatalf("%s no longer calls execEmittedDDL — the door is unwired", f)
		}
	}
}
