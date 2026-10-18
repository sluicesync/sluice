// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestControlTableDDLPrintsEveryEnsuredColumn holds `sluice control-tables
// ddl` to the sluice_cdc_state columns sluice ensures (Bug 321 R6 C). The
// site says the printed set and what sluice checks cannot drift apart; the
// three table-set columns and the live-added set shipped ensured on first
// write and absent from the printout, which made that sentence false.
//
// THE UNIVERSE, derived rather than listed: the table's start-time column
// list (cdcStateTable(...).columns), the lists the first-write paths ensure
// (tableSetColumnNames, liveAddedTablesColumn), and every column name an AST
// walk of this package finds handed to tableSetColumn() as a literal or a
// package constant — so a new first-write column added ad hoc is in the
// universe whether or not anyone remembers this gate. An argument the walk
// cannot resolve fails the gate rather than being skipped.
//
// WHAT IT REACHES, stated so the name cannot be read as broader: the
// sluice_cdc_state table only, and only columns ensured through
// controlTable.columns or tableSetColumn().
func TestControlTableDDLPrintsEveryEnsuredColumn(t *testing.T) {
	universe := map[string]bool{}
	for _, c := range cdcStateTable(bootstrapSchema).columns {
		universe[c.name] = true
	}
	for _, c := range tableSetColumnNames {
		universe[c] = true
	}
	universe[liveAddedTablesColumn] = true
	for name := range tableSetColumnLiteralArgs(t) {
		universe[name] = true
	}
	// Anti-vacuity: the table carries 8 start-time columns and 4 first-write
	// ones; a derivation that found far fewer has stopped seeing them.
	if len(universe) < 11 {
		t.Fatalf("derived only %d ensured sluice_cdc_state columns; the derivation has gone blind", len(universe))
	}

	printed := map[string]bool{}
	create := regexp.MustCompile(`(?m)^\s*([a-z_]+)\s+[A-Z]`)
	add := regexp.MustCompile(`ADD COLUMN IF NOT EXISTS ([a-z_]+)`)
	for _, s := range (Engine{}).ControlTableDDL() {
		if s.Table != controlTableName {
			continue
		}
		for _, m := range add.FindAllStringSubmatch(s.DDL, -1) {
			printed[m[1]] = true
		}
		if strings.Contains(s.DDL, "CREATE TABLE") {
			for _, m := range create.FindAllStringSubmatch(s.DDL, -1) {
				printed[m[1]] = true
			}
		}
	}
	for name := range universe {
		if !printed[name] {
			t.Errorf("sluice ensures sluice_cdc_state.%s but `sluice control-tables ddl` does not print it: an owner who "+
				"runs the printed set leaves a DML-only role refusing at the first write that needs it", name)
		}
	}
}

// tableSetColumnLiteralArgs walks this package's non-test sources for calls
// to tableSetColumn and returns every argument it can resolve to a name.
// The two non-literal arguments the write paths use are accounted for: the
// loop variable over tableSetColumnNames, and liveAddedTablesColumn.
func tableSetColumnLiteralArgs(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	out := map[string]bool{}
	calls := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 1 {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "tableSetColumn" {
				return true
			}
			calls++
			switch arg := call.Args[0].(type) {
			case *ast.BasicLit:
				v, _ := strconv.Unquote(arg.Value)
				out[v] = true
			case *ast.Ident:
				switch arg.Name {
				case "liveAddedTablesColumn":
					out[liveAddedTablesColumn] = true
				case "c", "name":
					// the loop variable over tableSetColumnNames, or the
					// helper's own parameter
				default:
					t.Errorf("%s: tableSetColumn(%s) — the gate cannot resolve that argument; extend it",
						fset.Position(call.Pos()), arg.Name)
				}
			default:
				t.Errorf("%s: tableSetColumn with an argument the gate cannot resolve; extend it", fset.Position(call.Pos()))
			}
			return true
		})
	}
	if calls < 3 {
		t.Fatalf("found only %d tableSetColumn calls; the walk has gone blind", calls)
	}
	return out
}
