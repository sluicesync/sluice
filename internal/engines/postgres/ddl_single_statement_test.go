// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The C1-1 validator pins: every quoting form the emitters produce must
// pass, and every structural multi-statement shape must refuse. The
// hostile cases are the audit's observed injection family — a recorded
// expression body that closes the surrounding syntax and appends more
// SQL — plus the quoting-escape variants (a `$$` inside an enum label
// terminating the DO-block's dollar quoting early).
func TestAssertSingleDDLStatement(t *testing.T) {
	valid := []struct{ name, stmt string }{
		{"plain create table", `CREATE TABLE "users" ("id" bigint PRIMARY KEY, "name" text)`},
		{"semicolon inside a string literal", `CREATE TABLE "t" ("c" text CHECK ("c" <> 'a;b'))`},
		{"doubled quotes inside a literal", `CREATE TABLE "t" ("c" text DEFAULT 'O''Reilly; ok')`},
		{"semicolon inside a quoted identifier", `CREATE TABLE "we;ird" ("id" int)`},
		{"doubled double-quote identifier", `ANALYZE "weird""name"`},
		{"the enum-guard DO block (internal semicolons inside $$)", guardedCreateEnumType(`CREATE TYPE "m" AS ENUM ('a', 'b');`)},
		{"tagged dollar quote", `CREATE FUNCTION-ish $tag$ body; with ; semis $tag$`},
		{"single trailing semicolon", `CREATE INDEX "i" ON "t" ("c");`},
		{"trailing semicolon then whitespace", "CREATE INDEX \"i\" ON \"t\" (\"c\");\n\t "},
		{"nested balanced parens", `ALTER TABLE "t" ADD CONSTRAINT "c" CHECK ((("a" + 1) > f("b")))`},
	}
	for _, tc := range valid {
		if err := assertSingleDDLStatement(tc.stmt); err != nil {
			t.Errorf("%s: refused a valid single statement: %v\n  stmt: %s", tc.name, err, tc.stmt)
		}
	}

	hostile := []struct{ name, stmt string }{
		{
			"the audit's observed injection: a CHECK body closes the parens and appends SQL",
			`CREATE TABLE "t" ("c" text, CHECK (true)); DROP TABLE "canary"; CREATE TABLE "x" ("y" int CHECK ((true)))`,
		},
		{"two statements", `CREATE TABLE "a" ("id" int); CREATE TABLE "b" ("id" int)`},
		{"semicolon inside an open paren group", `CREATE TABLE "t" ("c" text CHECK (true; DROP TABLE "canary"))`},
		{"unbalanced open paren", `CREATE TABLE "t" ("c" text CHECK ((true)`},
		{"unbalanced close paren", `CREATE TABLE "t" ("c" text CHECK (true)))`},
		{"unterminated string literal (the Bug 243 mangle family)", `ALTER TABLE "t" ADD CONSTRAINT "c" CHECK ("n" <> 'o\\'brien')`},
		{"unterminated dollar quote", `DO $$ BEGIN SELECT 1;`},
		{
			"a $$ inside an enum label terminates the DO-block quoting early",
			guardedCreateEnumType(`CREATE TYPE "e" AS ENUM ('a$$', 'b'); DROP TABLE "canary";`),
		},
		// C-1 Family B (comment-smuggled injection): the ';DROP' is
		// OUTSIDE the comment; the comment only carries an apostrophe
		// that a comment-blind door misreads as a string opener. The
		// server-differential oracle (TestSingleStatementDoorMatchesPG)
		// proves these inject on a real PG 16.
		{
			"a block comment carrying an apostrophe hides a following top-level ;",
			`ALTER TABLE "t" ADD CONSTRAINT "c" CHECK (true /* ' */ ); DROP TABLE "canary"; -- '`,
		},
		{
			"a line comment carrying an apostrophe hides a following top-level ;",
			"ALTER TABLE \"t\" ADD CONSTRAINT \"c\" CHECK (true -- '\n); DROP TABLE \"canary\"; -- '",
		},
		{
			"a nested block comment (PG-nesting) still hides nothing after it",
			`ALTER TABLE "t" ADD CONSTRAINT "c" CHECK (true /* a /* b */ c */ ); DROP TABLE "canary"`,
		},
		// C-1 Family A (dollar sign continuing an identifier): `col$q$`
		// is one identifier token to PG, so the `;` between the `$q$`
		// pair is top-level — not hidden inside a dollar-quoted body as
		// a preceding-byte-blind door assumed.
		{
			"a $ continuing an identifier is not a dollar-quote opener; the ; is top-level",
			`col$q$; DROP TABLE "canary"; $q$`,
		},
	}
	for _, tc := range hostile {
		if err := assertSingleDDLStatement(tc.stmt); err == nil {
			t.Errorf("%s: PASSED the single-statement door — the extra SQL would execute\n  stmt: %s", tc.name, tc.stmt)
		}
	}
}

// TestSchemaWriterEmittedDDLGoesThroughTheSingleStatementDoor is the
// anti-drift roster for the C1-1 door: within the schema-writing
// surface (schema_writer.go, schema_writer_check.go — the files whose
// executed strings are BUILT from recorded schema content), every
// ExecContext call must sit inside execEmittedDDL or inside one of the
// named exempt functions, each exempt for a stated reason:
//
//   - tuneIndexBuildConn: two session SETs built from sluice-computed
//     integers, no recorded content.
//   - syncOneIdentity: a setval query with BIND ARGUMENTS — recorded
//     content travels as parameters, which cannot split a statement.
//
// Scope, stated per the gate rule: this roster reaches the two
// schema-writer files only. The row/CDC apply paths execute
// parameterized DML (recorded content as bind args), and the OTHER
// engines' DDL surfaces are enumerated in the C1-1 commit (MySQL's
// driver refuses multi-statement strings by default; SQLite's is filed
// as an open sibling).
func TestSchemaWriterEmittedDDLGoesThroughTheSingleStatementDoor(t *testing.T) {
	files := []string{"schema_writer.go", "schema_writer_check.go", "ddl_single_statement.go"}
	exempt := map[string]bool{
		"execEmittedDDL":     true, // the door itself — the one real ExecContext
		"tuneIndexBuildConn": true,
		"syncOneIdentity":    true,
	}

	fset := token.NewFileSet()
	total := 0
	for _, f := range files {
		parsed, err := parser.ParseFile(fset, filepath.Join(".", f), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok {
				name := fn.Name.Name
				ast.Inspect(fn, func(n ast.Node) bool {
					sel, ok := n.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "ExecContext" {
						return true
					}
					total++
					if !exempt[name] {
						pos := fset.Position(sel.Pos())
						t.Errorf("%s:%d: direct ExecContext in %s — emitted DDL must go through execEmittedDDL "+
							"(the C1-1 single-statement door), or the function needs an exemption HERE with a "+
							"stated reason", pos.Filename, pos.Line, name)
					}
					return true
				})
				continue
			}
			// Function literals assigned at package level (indexStmtExec)
			// are covered by walking the whole file below.
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			// Package-level var func literals: indexStmtExec routes through
			// execEmittedDDL, so any direct ExecContext in a package-level
			// literal is a miss. Walk GenDecls' values.
			gd, ok := n.(*ast.GenDecl)
			if !ok || gd.Tok != token.VAR {
				return true
			}
			ast.Inspect(gd, func(m ast.Node) bool {
				sel, ok := m.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "ExecContext" {
					return true
				}
				total++
				pos := fset.Position(sel.Pos())
				t.Errorf("%s:%d: direct ExecContext in a package-level var — route through execEmittedDDL", pos.Filename, pos.Line)
				return true
			})
			return false
		})
	}
	// Anti-vacuity: the walk must actually see the surface. One real
	// ExecContext (the door's own) + two exempt functions' three sites =
	// at least 4; a parse scoping bug that saw none must fail loudly.
	if total < 4 {
		t.Fatalf("the roster walk found only %d ExecContext sites across %v — the scan is not reaching the "+
			"surface it claims to gate", total, files)
	}
	raw, err := os.ReadFile("schema_writer.go")
	if err != nil {
		t.Fatalf("read schema_writer.go: %v", err)
	}
	if !strings.Contains(string(raw), "execEmittedDDL(") {
		t.Fatal("schema_writer.go no longer calls execEmittedDDL — the door is unwired")
	}
}
