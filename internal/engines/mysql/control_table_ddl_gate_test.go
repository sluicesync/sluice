// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

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

	"sluicesync.dev/sluice/internal/appliershared"
)

// TestControlTableDDLPrintsEveryEnsuredColumn holds `sluice control-tables
// ddl` to the sluice_cdc_state columns sluice ensures (Bug 321 R6 C): the
// printed CREATE must declare every column some ensure path adds. It did
// not declare live_added_tables (since v0.27.0) nor the three table-set
// columns, so an owner who bootstrapped from the printout left a DML-only
// role (a PlanetScale safe-migrations branch) refusing at the first write.
//
// THE UNIVERSE is derived by an AST walk of this package: every column name
// handed to ensureCrossEngineParityColumn (a literal, a resolvable package
// constant, or the loop over tableSetColumnNames) and every literal
// `ADD COLUMN <name>` statement. An argument the walk cannot resolve fails
// the gate rather than being skipped.
//
// WHAT IT REACHES: sluice_cdc_state only (ensureCrossEngineParityColumn
// writes no other table; the shard-lease column helper is a different
// function and table).
func TestControlTableDDLPrintsEveryEnsuredColumn(t *testing.T) {
	universe := ensuredCDCStateColumns(t)
	// Anti-vacuity: nine ensure sites (seven parity columns, two literal
	// ADD COLUMNs) plus the three table-set columns.
	if len(universe) < 11 {
		t.Fatalf("derived only %d ensured sluice_cdc_state columns; the walk has gone blind", len(universe))
	}
	printed := map[string]bool{}
	col := regexp.MustCompile(`(?m)^\s*([a-z_]+)\s+[A-Z]`)
	for _, s := range (Engine{}).ControlTableDDL() {
		if s.Table != controlTableName {
			continue
		}
		for _, m := range col.FindAllStringSubmatch(s.DDL, -1) {
			printed[m[1]] = true
		}
	}
	for name := range universe {
		if !printed[name] {
			t.Errorf("sluice ensures sluice_cdc_state.%s but `sluice control-tables ddl` does not declare it", name)
		}
	}
}

func ensuredCDCStateColumns(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	constants := map[string]string{"appliershared.UnforwardedRefusalColumn": appliershared.UnforwardedRefusalColumn}
	addColumn := regexp.MustCompile(`ADD COLUMN ([a-z_]+) `)
	fset := token.NewFileSet()
	out := map[string]bool{}
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
			switch n := n.(type) {
			case *ast.BinaryExpr:
				// A concatenated `ALTER TABLE <controlTableRef(_, controlTableName)>
				// ADD COLUMN <name> …` — sluice_cdc_state's, not another table's.
				ops := flattenConcat(n)
				names, onCDCState := []string{}, false
				for _, op := range ops {
					switch op := op.(type) {
					case *ast.CallExpr:
						if id, ok := op.Fun.(*ast.Ident); ok && id.Name == "controlTableRef" && len(op.Args) == 2 {
							if a, ok := op.Args[1].(*ast.Ident); ok && a.Name == "controlTableName" {
								onCDCState = true
							}
						}
					case *ast.BasicLit:
						if v, err := strconv.Unquote(op.Value); err == nil && op.Kind == token.STRING {
							for _, m := range addColumn.FindAllStringSubmatch(v, -1) {
								names = append(names, m[1])
							}
						}
					}
				}
				if onCDCState {
					for _, c := range names {
						out[c] = true
					}
				}
			case *ast.CallExpr:
				id, ok := n.Fun.(*ast.Ident)
				if !ok || id.Name != "ensureCrossEngineParityColumn" || len(n.Args) != 5 {
					return true
				}
				switch arg := n.Args[3].(type) {
				case *ast.BasicLit:
					v, _ := strconv.Unquote(arg.Value)
					out[v] = true
				case *ast.SelectorExpr:
					key := exprName(arg)
					if v, ok := constants[key]; ok {
						out[v] = true
					} else {
						t.Errorf("%s: ensureCrossEngineParityColumn(..., %s, ...) — the gate cannot resolve it; add it to constants", fset.Position(n.Pos()), key)
					}
				case *ast.Ident:
					if arg.Name != "col" {
						t.Errorf("%s: ensureCrossEngineParityColumn(..., %s, ...) — the gate cannot resolve it; extend it", fset.Position(n.Pos()), arg.Name)
					}
					for _, c := range tableSetColumnNames { // the loop in ensureTableSetColumns
						out[c] = true
					}
				default:
					t.Errorf("%s: ensureCrossEngineParityColumn with an argument the gate cannot resolve", fset.Position(n.Pos()))
				}
			}
			return true
		})
	}
	return out
}

func flattenConcat(e ast.Expr) []ast.Expr {
	if b, ok := e.(*ast.BinaryExpr); ok && b.Op == token.ADD {
		return append(flattenConcat(b.X), flattenConcat(b.Y)...)
	}
	return []ast.Expr{e}
}

func exprName(e ast.Expr) string {
	if s, ok := e.(*ast.SelectorExpr); ok {
		if x, ok := s.X.(*ast.Ident); ok {
			return x.Name + "." + s.Sel.Name
		}
	}
	return ""
}
