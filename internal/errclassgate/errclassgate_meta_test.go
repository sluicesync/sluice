// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package errclassgate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The package doc asserts "every engine instantiates it. Adding an engine
// without a gate file is itself a finding." That sentence was an UNVERIFIED
// invariant (audit backlog A-4): the reach was mysql + postgres, while
// pgtrigger, sqlite-trigger, mydumper, and sqlite parked errors with no
// setErr gate — exactly the one-engine-at-a-time recurrence the package was
// written to end. This meta-gate makes the sentence mechanically true: it
// derives the universe of packages that HAVE a setErr parking surface from the
// AST, and fails unless each one either carries an errclassgate.Assert gate in
// its tests OR is on setErrGateRoster with a reason the class does not apply.
//
// It is the errclassgate analogue of the domaingate meta-walker, and shares its
// two disciplines: the roster is self-pruning (an entry for a package that has
// since grown a gate, or lost its park surface, fails as stale), and an
// anti-vacuity floor stops a detection break from greening on an empty universe.

// setErrGateRoster records packages that have a setErr parking surface but
// deliberately carry NO per-site errclassgate.Assert gate, each with the reason
// the Bug-207 class does not apply there. Keyed by the package dir relative to
// internal/. An entry that becomes redundant (the package grew a gate) or stale
// (its park surface is gone) fails the gate, so this list cannot rot silently.
var setErrGateRoster = map[string]string{
	"engines/mydumper": "the single setErr site is in the bulk-read RowReader (a migrate-path read), not a resumable CDC streamer; a read fault aborts the migrate and the operator re-runs, so terminal-by-default is correct — the Bug-207 retriable-carve-out concern is CDC-specific.",
	"engines/sqlite":   "all setErr sites are in the bulk-read RowReaders (row_reader.go / d1_rows.go, migrate path). Terminal-by-default is correct for a migrate read; SQLite/D1's CDC lane is trigger-based and lives in the separate sqlite-trigger / d1-trigger packages, which are gated (or have no park surface) on their own.",
}

func TestEverySetErrPackageHasAGateOrReason(t *testing.T) {
	internalRoot := ".." // this test runs in internal/errclassgate, so .. is internal/
	fset := token.NewFileSet()

	type pkgInfo struct {
		hasParkSite bool
		hasGate     bool
	}
	pkgs := map[string]*pkgInfo{}
	info := func(rel string) *pkgInfo {
		pi := pkgs[rel]
		if pi == nil {
			pi = &pkgInfo{}
			pkgs[rel] = pi
		}
		return pi
	}

	err := filepath.Walk(internalRoot, func(path string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		rel, relErr := filepath.Rel(internalRoot, filepath.Dir(path))
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		isTest := strings.HasSuffix(path, "_test.go")

		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			// Park site: a X.setErr(arg) CALL with an identifier receiver in a
			// non-test file — the same shape errclassgate.Assert counts, so a
			// package the meta-gate flags is exactly one Assert can inspect.
			if !isTest && sel.Sel.Name == "setErr" && len(call.Args) == 1 {
				if _, ok := sel.X.(*ast.Ident); ok {
					info(rel).hasParkSite = true
				}
			}
			// Gate: an errclassgate.Assert(...) call in a test file.
			if isTest && sel.Sel.Name == "Assert" {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "errclassgate" {
					info(rel).hasGate = true
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/: %v", err)
	}

	var parkPkgs, gatedPkgs int
	for rel, pi := range pkgs {
		if pi.hasParkSite {
			parkPkgs++
		}
		if pi.hasParkSite && pi.hasGate {
			gatedPkgs++
		}
		if !pi.hasParkSite {
			continue
		}
		if pi.hasGate {
			if _, rostered := setErrGateRoster[rel]; rostered {
				t.Errorf("%s has BOTH an errclassgate.Assert gate AND a setErrGateRoster exemption — the roster entry is now redundant; remove it so the roster stays a true list of the un-gated", rel)
			}
			continue
		}
		if _, rostered := setErrGateRoster[rel]; !rostered {
			t.Errorf("%s parks errors via setErr but has NO errclassgate.Assert gate and NO setErrGateRoster reason — add a seterr gate file mirroring engines/postgres, or roster it with why the Bug-207 class does not apply", rel)
		}
	}

	// Self-pruning: a roster entry whose package no longer has a park surface
	// (renamed, deleted, refactored away) is stale and must be removed.
	for rel := range setErrGateRoster {
		pi := pkgs[rel]
		if pi == nil || !pi.hasParkSite {
			t.Errorf("setErrGateRoster names %q, but no setErr park site was found there — the roster entry is stale; remove it", rel)
		}
	}

	// Anti-vacuity: the measured universe at authoring is 6 park packages, 4 of
	// them gated. A detection break that drops these to near-zero would green on
	// an empty universe, which is how a derived gate rots into a no-op.
	if parkPkgs < 5 {
		t.Fatalf("found only %d packages with a setErr park surface (floor 5) — the walker stopped matching the parking convention; fix the detection, not the floor", parkPkgs)
	}
	if gatedPkgs < 3 {
		t.Fatalf("found only %d gated setErr packages (floor 3) — the errclassgate.Assert detection likely broke; fix it rather than lowering the floor", gatedPkgs)
	}
}
