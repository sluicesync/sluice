// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/appliershared"
)

// TestPositionWritesStampLiveness is the roster behind
// DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED's load-bearing stamp: every
// position write an applying stream commits must stamp the liveness column,
// or a stream that last recorded a stop (or whose attempt-start stamp failed)
// could advance its position past a new table's changes while reading as
// stopped. Every position write in this package goes through writePositionTx
// (and writePositionUpsertSQL, from it); the walk holds each call of writePositionTx, anywhere in the
// package's non-test files, to passing the applier's stampsLiveness() as its
// stamp argument.
//
// WHAT IT REACHES: writePositionTx's call sites in this package. A position
// write that bypassed it would not be seen, which is why the floor below
// counts the known sites.
func TestPositionWritesStampLiveness(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	sites := 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok || id.Name != "writePositionTx" {
				return true
			}
			sites++
			last, ok := call.Args[len(call.Args)-1].(*ast.CallExpr)
			sel, isSel := (*ast.SelectorExpr)(nil), false
			if ok {
				sel, isSel = last.Fun.(*ast.SelectorExpr)
			}
			if !isSel || sel.Sel.Name != "stampsLiveness" {
				t.Errorf("%s: writePositionTx without the applier's stampsLiveness(): its position writes would not stamp liveness",
					fset.Position(call.Pos()))
			}
			return true
		})
	}
	// The schema-event, per-change, batch, position-only and multirow paths.
	if sites < 5 {
		t.Fatalf("found %d writePositionTx call sites; want >= 5 — the walk has gone blind", sites)
	}
}

// TestWritePositionUpsertSQL_LivenessStamp: with the column the upsert stamps
// it from the server's clock on both arms, in both upsert spellings; without
// it the statement does not name the column, so a control table that lacks it
// keeps working.
func TestWritePositionUpsertSQL_LivenessStamp(t *testing.T) {
	col := appliershared.StreamLivenessColumn
	for _, u := range []upsertSpelling{upsertRowAlias, upsertValuesFunc} {
		with := writePositionUpsertSQL("ctl", u, true)
		for _, want := range []string{", " + col + ") VALUES", ", " + mysqlNowMillisExpr + ")", col + " = " + u.newRowRef(col)} {
			if !strings.Contains(with, want) {
				t.Errorf("stamping upsert (spelling %d) lacks %q:\n%s", u, want, with)
			}
		}
		if without := writePositionUpsertSQL("ctl", u, false); strings.Contains(without, col) {
			t.Errorf("an upsert told not to stamp names %s:\n%s", col, without)
		}
	}
}

// TestSourcePositionWrittenOnlyByTheBuilder is the ratchet behind
// TestPositionWritesStampLiveness's floor: a statement that writes
// sluice_cdc_state.source_position anywhere but writePositionUpsertSQL would
// be a position write that bypasses the liveness stamp. The walk takes every
// function in the package's non-test files whose string literals name
// source_position, and refuses one whose literals also write (a literal that
// starts INSERT INTO or UPDATE, or assigns `source_position =`) unless it is
// the builder. A floor on the readers it sees keeps it from passing blind.
func TestSourcePositionWrittenOnlyByTheBuilder(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	writers, readers := 0, 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			names, writes := false, false
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				bl, ok := n.(*ast.BasicLit)
				if !ok || bl.Kind != token.STRING {
					return true
				}
				v, err := strconv.Unquote(bl.Value)
				if err != nil {
					return true
				}
				names = names || strings.Contains(v, "source_position")
				up := strings.ToUpper(strings.TrimSpace(v))
				writes = writes || strings.HasPrefix(up, "INSERT INTO") || strings.HasPrefix(up, "UPDATE ") ||
					strings.Contains(v, "source_position =")
				return true
			})
			switch {
			case !names:
			case writes && fd.Name.Name == "writePositionUpsertSQL":
				writers++
			case writes:
				t.Errorf("%s writes sluice_cdc_state.source_position outside writePositionUpsertSQL: a position "+
					"write that bypasses the liveness stamp", fd.Name.Name)
			default:
				readers++
			}
		}
	}
	if writers != 1 || readers < 3 {
		t.Fatalf("saw %d writers and %d readers of source_position; want the builder and >= 3 readers — the walk has gone blind", writers, readers)
	}
}
