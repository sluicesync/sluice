// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// connectionSetupFuncs is the fail-by-default roster of functions that run
// while a connection is being OPENED — AfterConnect hooks, and the probes the
// engine runs immediately after opening a pool, before handing it to a caller.
//
// Every error these return lands on the chunk-OPEN path
// ([pipeline.isRetriableChunkOpenError]), which decides by asking whether the
// error carries an engine verdict. An unclassified return there is read as
// fatal, so a transient platform condition fails the whole table instead of
// being ridden out.
//
// Each entry is a CLASSIFICATION with a reason, not a waiver. To add a function
// here, say which it is.
var connectionSetupFuncs = map[string]struct {
	mustClassify bool
	reason       string
}{
	"afterConnectSessionPins": {
		mustClassify: true,
		reason: "AfterConnect hook on EVERY pool, and composeAfterConnect runs it FIRST — so an " +
			"unclassified return here is reached before any other setup verdict can be",
	},
	"afterConnectRegisterGeometry": {
		mustClassify: true,
		reason: "AfterConnect hook on four pools; the v0.152.1 defect. Its bare NK205 killed a live " +
			"migration 3m40s into a storage-grow window",
	},
	"detectPostGIS": {
		mustClassify: true,
		reason: "runs inside OpenRowWriter/OpenSchemaWriter three lines after the pool opens; a " +
			"pg_extension catalog query of exactly the spatial probe's shape",
	},
}

// TestConnectionSetupReturnsAreClassified is the gate that would have caught
// the v0.152.1 defect, and the two siblings that fix missed.
//
// # Why the existing gate could not
//
// `internal/errclassgate` walks errors that are PARKED via setErr — the Bug-207
// class. A connection-setup hook does not park its error, it RETURNS it, so the
// whole family was invisible to the only gate covering error classification.
// The defect that produced this test sat three lines from a second instance of
// itself, and a third was the default hook on every pool in the engine.
//
// # What it grades
//
// Every `return` inside a rostered function must either construct its error
// through a classifier (classifyCopyError / classifyApplierError /
// classifyReaderError) or return an already-classified value. A bare
// `fmt.Errorf`/`errors.New` return is the defect.
//
// This is deliberately an AST walk over a NAMED roster rather than a derived
// universe: "functions that run during connection setup" is not mechanically
// derivable (an AfterConnect hook is identified by where it is INSTALLED, at a
// call site in another file, and a post-open probe only by reading the caller).
// The roster is therefore hand-held — and the anti-vacuity floor plus the
// unknown-name check below are what stop it from silently covering nothing.
func TestConnectionSetupReturnsAreClassified(t *testing.T) {
	t.Parallel()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	fset := token.NewFileSet()
	files, err := parseNonTestGoFiles(fset, dir)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}

	classifiers := map[string]bool{
		"classifyCopyError":    true,
		"classifyApplierError": true,
		"classifyReaderError":  true,
	}

	seen := map[string]bool{}
	var bare []string

	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			spec, rostered := connectionSetupFuncs[fn.Name.Name]
			if !rostered || !spec.mustClassify {
				continue
			}
			seen[fn.Name.Name] = true

			ast.Inspect(fn.Body, func(n ast.Node) bool {
				ret, ok := n.(*ast.ReturnStmt)
				if !ok {
					return true
				}
				// A bare fmt.Errorf / errors.New in a return is the defect;
				// the same call wrapped in a classifier is the fix.
				for _, res := range ret.Results {
					if isBareErrorConstruction(res, classifiers) {
						pos := fset.Position(res.Pos())
						bare = append(bare, fmt.Sprintf("%s (%s:%d)", fn.Name.Name, filepath.Base(pos.Filename), pos.Line))
					}
				}
				return true
			})
		}
	}

	// Anti-vacuity floor: every rostered function must have been FOUND. A
	// rename, a move to another file, or a parser that returned nothing would
	// otherwise leave this test passing while grading zero functions — which is
	// exactly the failure mode it exists to prevent elsewhere.
	var missing []string
	for name, spec := range connectionSetupFuncs {
		if spec.mustClassify && !seen[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("rostered connection-setup functions were NOT FOUND in the package: %v\n\n"+
			"Either they were renamed or moved — in which case this gate is now inert over them and the "+
			"roster must be repointed — or the AST walk is broken. A gate that passes by finding nothing "+
			"is worse than no gate, because it stops anyone from looking.", missing)
	}

	if len(seen) < 3 {
		t.Fatalf("the roster graded only %d functions; the floor is 3 (the two AfterConnect hooks and "+
			"the post-open PostGIS probe). Below that the walk has stopped matching the code", len(seen))
	}

	sort.Strings(bare)
	if len(bare) > 0 {
		t.Fatalf("connection-setup functions return UNCLASSIFIED errors:\n\n  %s\n\n"+
			"These run while a connection is being opened — the AfterConnect hooks on every pool, and the "+
			"probes that run immediately after. Their errors land on the chunk-open retry, which decides "+
			"by asking for an engine verdict and treats an unclassified error as FATAL. Measured on a real "+
			"PS-10 Neki: a transient `NK205 no healthy sidecars available` returned bare from one of these "+
			"killed a migration 3m40s into a storage-grow window it was otherwise surviving.\n\n"+
			"Wrap the error: `return classifyCopyError(fmt.Errorf(...))`.",
			strings.Join(bare, "\n  "))
	}
}

// parseNonTestGoFiles parses every non-test .go file in dir. Written out
// rather than using parser.ParseDir, which is deprecated as of Go 1.25 — and
// which this package's other AST gates avoid for the same reason.
func parseNonTestGoFiles(fset *token.FileSet, dir string) ([]*ast.File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []*ast.File
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if perr != nil {
			return nil, perr
		}
		files = append(files, f)
	}
	return files, nil
}

// isBareErrorConstruction reports whether expr is a direct fmt.Errorf /
// errors.New call not wrapped in one of the classifiers.
func isBareErrorConstruction(expr ast.Expr, classifiers map[string]bool) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	if id, ok := call.Fun.(*ast.Ident); ok && classifiers[id.Name] {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return (pkg.Name == "fmt" && sel.Sel.Name == "Errorf") ||
		(pkg.Name == "errors" && sel.Sel.Name == "New")
}
