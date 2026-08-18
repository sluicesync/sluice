// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package errclassgate is the shared Tier-1 gate for the Bug 207 class: an
// error parked for a streamer to collect must first pass through an error
// CLASSIFIER, or be a purpose-built typed error the streamer matches
// structurally.
//
// # Why this exists, and why it is SHARED
//
// A reader goroutine cannot return an error to its caller, so it parks one via
// setErr for the streamer to collect. Everything the streamer decides —
// retriable vs terminal, position-invalid vs transient — is decided from that
// parked value. An unclassified one is therefore TERMINAL by default, however
// routine the underlying condition.
//
// The original gate lived in package mysql and walked its own directory. That
// scoping is itself how the class recurred: the gate was written after the
// defect shipped INERT on the MySQL VStream reader, found a second instance on
// the MySQL binlog reader — and could not see the Postgres CDC pump, which had
// the identical defect on the identical shape (a dispatch path running live
// catalog queries). A gate that can only see the engine it was born in will
// keep re-learning the same lesson one engine at a time.
//
// So the walker lives here. Every engine with a resumable-CDC parking surface
// instantiates it (mysql, postgres, pgtrigger, sqlite-trigger); the bulk-read
// RowReaders that also park via setErr (mydumper, sqlite) are terminal-by-
// default-correct — a migrate read fault aborts and re-runs, so the retriable
// carve-out the class is about does not apply — and are exempted by name. That
// distinction is not a comment anyone has to trust: TestEverySetErrPackageHasA
// GateOrReason derives the set of setErr-parking packages from the AST and
// fails unless each carries a gate file OR a rostered reason, so adding an
// engine (or growing a new park surface) without one is a build failure, not a
// finding someone has to notice.
package errclassgate

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

// Config describes one package's instantiation of the gate.
type Config struct {
	// Dir is the package directory to walk (usually ".").
	Dir string
	// Method is the error-parking method name, e.g. "setErr".
	Method string
	// Classifiers are the function names whose RESULT is an acceptable
	// argument: error classifiers, plus constructors returning a typed error
	// the streamer matches structurally (routing one of those through a text
	// classifier would be strictly worse, so they are accepted as-is).
	Classifiers map[string]bool
	// Allowed are deliberate exceptions, keyed "file.go:argument-source" so an
	// entry cannot silently widen to another site sharing the argument text.
	// The value is the REASON classification would be wrong or redundant
	// there; an entry without a real reason is a fix that should have been
	// made.
	Allowed map[string]string
	// MinSites is the anti-vacuity floor: if the walk finds fewer parking
	// sites than this, the gate has probably stopped matching the real parking
	// mechanism (a rename, a refactor) and would pass forever. Fail instead.
	MinSites int
}

// Assert runs the gate. It reports every parking site whose argument is not a
// classifier call and not allowlisted.
func Assert(t *testing.T, cfg Config) {
	t.Helper()

	fset := token.NewFileSet()
	entries, err := os.ReadDir(cfg.Dir)
	if err != nil {
		t.Fatalf("read package dir %s: %v", cfg.Dir, err)
	}

	var offenders []string
	checked := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(cfg.Dir, name)
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", path, perr)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != cfg.Method || len(call.Args) != 1 {
				return true
			}
			// Require a receiver expression (r.setErr / s.setErr) so the
			// method's own declaration body is not counted.
			if _, ok := sel.X.(*ast.Ident); !ok {
				return true
			}
			checked++
			arg := call.Args[0]
			if argIsClassified(arg, cfg.Classifiers) {
				return true
			}
			src := exprText(fset, arg, path)
			if _, ok := cfg.Allowed[filepath.Base(name)+":"+src]; ok {
				return true
			}
			pos := fset.Position(call.Pos())
			offenders = append(offenders, filepath.Base(pos.Filename)+":"+strconv.Itoa(pos.Line)+"  "+cfg.Method+"("+src+")")
			return true
		})
	}

	if checked < cfg.MinSites {
		t.Fatalf("only %d %s call sites found (floor %d); the gate has probably stopped matching the real "+
			"parking mechanism and is now vacuous — re-point it", checked, cfg.Method, cfg.MinSites)
	}

	if len(offenders) > 0 {
		t.Errorf("%d %s site(s) park an UNCLASSIFIED error, which the streamer treats as TERMINAL "+
			"however routine the cause (the Bug 207 class — a retriable carve-out that can never be reached):\n  %s\n\n"+
			"Wrap the argument in the package's classifier, or — if the value is a purpose-built typed error the "+
			"streamer matches structurally — add its constructor to Classifiers with a comment saying why "+
			"classification would be worse.",
			len(offenders), cfg.Method, strings.Join(offenders, "\n  "))
	}
}

// argIsClassified reports whether the parked argument is a call to one of the
// accepted classifier/constructor functions. A bare identifier — setErr(err) —
// is what the gate rejects.
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
