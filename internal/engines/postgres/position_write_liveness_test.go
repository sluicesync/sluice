// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"go/ast"
	"go/token"
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
// stopped. Every position write in this package goes through buildWritePositionSQL, either from
// writePositionTx or from the pipelined queue; the walk holds each call of
// either, anywhere in the package, to passing the applier's
// stampsLiveness() (writePositionTx's own call passes its parameter through).
//
// WHAT IT REACHES: every call site of the two functions in this package's
// non-test files. A position write that bypassed both would not be seen,
// which is why the floor below counts the known sites.
func TestPositionWritesStampLiveness(t *testing.T) {
	sites := 0
	forEachPackageFunc(t, func(fnName string, body *ast.BlockStmt) {
		ast.Inspect(body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok || (id.Name != "writePositionTx" && id.Name != "buildWritePositionSQL") {
				return true
			}
			sites++
			last := call.Args[len(call.Args)-1]
			if fnName == "writePositionTx" && id.Name == "buildWritePositionSQL" {
				if p, ok := last.(*ast.Ident); ok && p.Name == "stampAlive" {
					return true
				}
			}
			if !isStampsLivenessCall(last) {
				t.Errorf("%s calls %s without the applier's stampsLiveness(): its position writes would not stamp liveness", fnName, id.Name)
			}
			return true
		})
	})
	// The serial, schema-event, per-change, batch, concurrent-lane and
	// position-only paths call writePositionTx; the pipelined queue and
	// writePositionTx itself call buildWritePositionSQL.
	if sites < 8 {
		t.Fatalf("found %d position-write call sites; want >= 8 — the walk has gone blind", sites)
	}
}

func isStampsLivenessCall(e ast.Expr) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "stampsLiveness"
}

// TestBuildWritePositionSQL_LivenessStamp: with the column the upsert stamps
// it from the server's clock on both arms (insert and conflict); without it
// the statement does not name the column at all, so a control table that
// lacks it keeps working.
func TestBuildWritePositionSQL_LivenessStamp(t *testing.T) {
	col := appliershared.StreamLivenessColumn
	with, _ := buildWritePositionSQL("public", "s", "t", "", "", "", "", "", 0, true)
	for _, want := range []string{", " + col + ") VALUES", ", " + pgNowMillisExpr + ")", col + " = GREATEST(EXCLUDED." + col + ", COALESCE(" + controlTableRef("public") + "." + col + ", 0) + 1)"} {
		if !strings.Contains(with, want) {
			t.Errorf("stamping upsert lacks %q:\n%s", want, with)
		}
	}
	without, _ := buildWritePositionSQL("public", "s", "t", "", "", "", "", "", 0, false)
	if strings.Contains(without, col) {
		t.Errorf("an upsert told not to stamp names %s:\n%s", col, without)
	}
}

// TestSourcePositionWrittenOnlyByTheBuilder is the ratchet behind
// TestPositionWritesStampLiveness's floor: a statement that writes
// sluice_cdc_state.source_position anywhere but buildWritePositionSQL would be
// a position write that bypasses the liveness stamp. The walk takes every
// function in the package's non-test files whose string literals name
// source_position, and refuses one whose literals also write (a literal that
// starts INSERT INTO or UPDATE, or assigns `source_position =`) unless it is
// the builder. A floor on the readers it sees keeps it from passing blind.
func TestSourcePositionWrittenOnlyByTheBuilder(t *testing.T) {
	writers, readers := 0, 0
	forEachPackageFunc(t, func(fnName string, body *ast.BlockStmt) {
		var lits []string
		ast.Inspect(body, func(n ast.Node) bool {
			if bl, ok := n.(*ast.BasicLit); ok && bl.Kind == token.STRING {
				if v, err := strconv.Unquote(bl.Value); err == nil {
					lits = append(lits, v)
				}
			}
			return true
		})
		names, writes := false, false
		for _, v := range lits {
			names = names || strings.Contains(v, "source_position")
			up := strings.ToUpper(strings.TrimSpace(v))
			writes = writes || strings.HasPrefix(up, "INSERT INTO") || strings.HasPrefix(up, "UPDATE ") ||
				strings.Contains(v, "source_position =")
		}
		switch {
		case !names:
		case writes && fnName == "buildWritePositionSQL":
			writers++
		case writes:
			t.Errorf("%s writes sluice_cdc_state.source_position outside buildWritePositionSQL: a position write "+
				"that bypasses the liveness stamp", fnName)
		default:
			readers++
		}
	})
	if writers != 1 || readers < 3 {
		t.Fatalf("saw %d writers and %d readers of source_position; want the builder and >= 3 readers — the walk has gone blind", writers, readers)
	}
}
