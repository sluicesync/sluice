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
)

// TestSetErrSitesClassify is the Tier-1 gate for the Bug 207 class: an error
// stored via setErr must first pass through an error CLASSIFIER, or be a
// purpose-built typed error that the streamer already recognises structurally.
//
// # Why this exists
//
// A reader goroutine cannot return an error to its caller, so it parks one via
// setErr for the streamer to collect. Everything the streamer decides —
// retriable vs terminal, position-invalid vs transient — is decided from that
// parked value. An unclassified one is therefore TERMINAL by default, however
// routine the underlying condition.
//
// That is how the v0.101.0 post-DDL FIELD-cache fix shipped INERT: the
// retriable arm was applied only to the stream's Recv() failures, while
// everything raised while INTERPRETING an event was parked raw. The fix had a
// passing unit test that called classifyReaderError directly — it pinned the
// FUNCTION, not the PATH the function is reached by — so nothing failed until a
// real vtgate killed a real stream. Auditing every setErr site afterwards found
// a second, independent instance on the binlog reader, where dispatchRows
// re-reads information_schema live and a connection blip there was equally
// terminal.
//
// A grep found both in under a minute, which is the whole argument for
// spending forty lines to make it permanent.
//
// # What counts as classified
//
// Either a call to a classify*Error function, or a call to a constructor that
// returns a typed error the streamer matches structurally (the liveness and
// progress timeouts implement ir.LivenessProgressTimeoutError and are handled
// by their own arm, so routing them through a text classifier would be strictly
// worse). A bare identifier — setErr(err) — is what this gate rejects.
//
// # Keep the allowlist small
//
// A gate whose exceptions are bulk-annotated away is decoration. The current
// set is small and each entry is a deliberate, named exception; if it starts
// growing, that is a signal the gate is being worked around rather than
// honoured.
func TestSetErrSitesClassify(t *testing.T) {
	// Call expressions whose RESULT is an acceptable setErr argument.
	classifiers := map[string]bool{
		"classifyReaderError":         true,
		"classifyApplierError":        true,
		"vstreamLivenessTimeoutError": true,
		"vstreamProgressTimeoutError": true,
	}
	// Deliberate exceptions, keyed "file:argument-source" so an entry cannot
	// silently widen to another site that happens to share the argument text.
	// Every entry states why classification would be wrong or redundant THERE —
	// an entry without a reason is an entry that should have been a fix.
	allowed := map[string]string{
		"cdc_snapshot_concurrent_resume.go:err":                           "already classified — this arm is guarded by !isRetriableReadErr(err), so the value has been judged terminal by a dedicated predicate before it is parked",
		"cdc_snapshot_concurrent_resume.go:rerr":                          "recoverFromDrop's failure is terminal by construction (retry budget exhausted / binlog purged); it exists to abort the copy loudly so restart-from-scratch takes over",
		"cdc_snapshot_concurrent_resume.go:ctx.Err()":                     "the caller's own cancellation, never a source fault",
		"row_reader.go:ctx.Err()":                                         "the caller's own cancellation, never a source fault",
		`row_reader.go:fmt.Errorf("mysql: column %q: %w", col.Name, err)`: "a value decode / zero-date-policy fault is a data-fidelity error: no retry can change the bytes, and classifying it risks routing corruption to a retry loop",
		// SETTLED (roadmap item 83) by killing a real connection mid-stream in
		// TestRowReader_MidStreamConnectionDrop_IsClassifiedRetriable: the drop
		// surfaces at the SIBLING rows.Err() exit, which IS classified. This exit
		// is not on the transient path — database/sql records a driver failure in
		// lasterr and Next() returns false, so the loop exits without ever
		// re-entering Scan — and its destinations are *any, which convertAssign
		// cannot fail on. Classifying here would be unreachable code.
		`row_reader.go:fmt.Errorf("mysql: scan: %w", err)`: "settled by the mid-stream-drop probe (item 83): a real connection drop surfaces at the classified rows.Err() exit, never here; this exit is not on the transient path",
	}

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	var offenders []string
	checked := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "setErr" || len(call.Args) != 1 {
				return true
			}
			// Skip the setErr METHOD DECLARATIONS' own internal calls by
			// requiring a receiver expression (r.setErr / s.setErr).
			if _, ok := sel.X.(*ast.Ident); !ok {
				return true
			}
			checked++
			arg := call.Args[0]
			if argIsClassified(arg, classifiers) {
				return true
			}
			src := exprText(fset, arg, name)
			if _, ok := allowed[filepath.Base(name)+":"+src]; ok {
				return true
			}
			pos := fset.Position(call.Pos())
			offenders = append(offenders, filepath.Base(pos.Filename)+":"+strconv.Itoa(pos.Line)+"  setErr("+src+")")
			return true
		})
	}

	// Anti-vacuity: if the walk stops finding setErr calls (a rename, a
	// refactor to a different parking mechanism), this gate silently passes
	// forever. Fail instead so the gate is re-pointed deliberately.
	if checked < 10 {
		t.Fatalf("only %d setErr call sites found; the gate has probably stopped matching the real parking mechanism and is now vacuous — re-point it", checked)
	}

	if len(offenders) > 0 {
		t.Errorf("%d setErr site(s) park an UNCLASSIFIED error, which the streamer treats as TERMINAL "+
			"however routine the cause (the Bug 207 class — a retriable carve-out that can never be reached):\n  %s\n\n"+
			"Wrap the argument in classifyReaderError(...) / classifyApplierError(...), or — if the value is a "+
			"purpose-built typed error the streamer matches structurally — add its constructor to `classifiers` "+
			"with a comment saying why classification would be worse.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
}

// argIsClassified reports whether the setErr argument is (or wraps) a call to
// one of the accepted classifier/constructor functions. It looks through one
// level of wrapping so `classifyReaderError(fmt.Errorf(...))` passes and
// `fmt.Errorf("...: %w", err)` alone does not.
func argIsClassified(arg ast.Expr, classifiers map[string]bool) bool {
	call, ok := arg.(*ast.CallExpr)
	if !ok {
		return false
	}
	if id, ok := call.Fun.(*ast.Ident); ok && classifiers[id.Name] {
		return true
	}
	return false
}

// exprText renders an expression back to source for the failure message and
// for allowlist matching.
func exprText(fset *token.FileSet, e ast.Expr, filename string) string {
	src, err := os.ReadFile(filename)
	if err != nil {
		return "<unrenderable>"
	}
	start := fset.Position(e.Pos()).Offset
	end := fset.Position(e.End()).Offset
	if start < 0 || end > len(src) || start >= end {
		return "<unrenderable>"
	}
	return strings.TrimSpace(string(src[start:end]))
}
