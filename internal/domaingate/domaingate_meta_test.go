// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package domaingate

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

// packageHasRawIRDispatch reports the RAW (non-UnwrapDomain) column-type
// dispatch sites in dir's non-test source, and whether dir imports internal/ir
// (a cheap filter against non-IR `.Type` dispatches). Reuses the same
// detection Assert uses.
func packageHasRawIRDispatch(t *testing.T, dir string) (sites []string, importsIR bool) {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, false
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr == nil {
			for _, imp := range f.Imports {
				if strings.Contains(imp.Path.Value, "sluicesync.dev/sluice/internal/ir") {
					importsIR = true
				}
			}
		}
		full, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			continue
		}
		for _, scope := range dispatchScopes(full) {
			ast.Inspect(scope.node, func(n ast.Node) bool {
				operand, ok := dispatchOperand(n)
				if !ok {
					return true
				}
				kind, ok := classifyOperand(operand)
				if !ok || kind != operandRaw {
					return true
				}
				pos := fset.Position(operand.Pos())
				sites = append(sites, name+":"+strconv.Itoa(pos.Line))
				return true
			})
		}
	}
	return sites, importsIR
}

// packageHasGateFile reports whether dir has a domaingate gate file — a
// *_test.go that calls domaingate.Assert.
func packageHasGateFile(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err == nil && strings.Contains(string(b), "domaingate.Assert") {
			return true
		}
	}
	return false
}

// ungatedDispatchRoster is the fail-by-default record (audit A-3) of every
// package under internal/ that dispatches on a RAW ir column type
// (`col.Type.(ir.X)`) WITHOUT its own domaingate gate file. The map value is
// the written reason the gate's package doc requires. These are un-audited
// domain-dispatch surfaces: a Domain-wrapped column reaches each and matches no
// arm, so some carry latent A-1/A-2-shaped hazards (e.g. postgres
// schema_writer's `col.Type.(ir.Enum)` would skip enum-type creation for a
// Domain-over-Enum). Building a real domaingate gate for each — starting with
// the postgres engine, the direct sibling of the already-gated mysql — is the
// tracked remediation (docs/dev/audit-backlog.md → A-3). Keys are
// forward-slash rel paths under internal/.
//
// This roster is the "written exemption" half of the gate's rule ("every
// IR-type-dispatching package instantiates the gate OR carries a written
// reason"): a NEW dispatching package that is neither gated nor listed here
// FAILS the meta-walker below, and a listed package that gains a gate (or stops
// dispatching) must be REMOVED — so the roster shrinks as the debt is paid.
var ungatedDispatchRoster = map[string]string{
	"engines/postgres": "A-3 remediation PRIORITY: the direct sibling of the gated mysql engine, 34 raw column-type dispatch sites (ddl_emit/row_writer/schema_writer/…) that a PG-sourced Domain reaches — needs its own domaingate gate file.",
	"engines/mydumper": "A-3 remediation: mydumper reader/parse dispatch on column type; needs a gate or a domain-safety audit.",
	"ir/diff":          "A-3 remediation: schema-diff column-shape normalization dispatches on ir.Integer; a Domain-over-Integer misses the AutoIncrement normalization (comparison nuance).",
	"pipeline":         "A-3 remediation: infer-types / redact-preflight / where-pushdown dispatch on column type; needs a gate or a domain-safety audit.",
	"pipeline/backup":  "A-3 remediation: chain-restore column-type dispatch; needs a gate or a domain-safety audit.",
	"pipeline/lineage": "A-3 remediation: verbatim-column detection dispatches on ir.VerbatimType; a Domain-over-VerbatimType would be missed.",
	"pipeline/migcore": "A-3 remediation: chunk-strategy + float-repair dispatch on ir.Integer; a Domain-over-Integer PK falls to keyset (perf, not correctness).",
	"rowpredicate":     "A-3 remediation: predicate rendering dispatches on column type; needs a gate or a domain-safety audit.",
}

// TestDomainGateMetaWalker_EveryDispatcherGatedOrRostered is the A-3 gate: every
// package under internal/ that imports ir and dispatches on a RAW column type
// must EITHER have its own domaingate gate file (mysql/sqlite/translate) OR be
// in ungatedDispatchRoster with a reason. A new dispatcher that is neither is
// the finding — the "new package grew a dispatch and no gate" shape Bug 233
// recurred through three times. The roster is fail-by-default and self-pruning.
func TestDomainGateMetaWalker_EveryDispatcherGatedOrRostered(t *testing.T) {
	root := ".." // internal/
	scanned := 0
	gatedFound := 0
	rosterUsed := map[string]bool{}
	var undeclared []string

	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // skip an unreadable dir, keep walking the rest
		}
		if !d.IsDir() {
			return nil
		}
		scanned++
		sites, importsIR := packageHasRawIRDispatch(t, path)
		if len(sites) == 0 || !importsIR {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if packageHasGateFile(path) {
			gatedFound++
			if _, listed := ungatedDispatchRoster[rel]; listed {
				undeclared = append(undeclared, rel+" is in ungatedDispatchRoster but now HAS a gate file — remove the stale roster entry")
			}
			return nil
		}
		if _, listed := ungatedDispatchRoster[rel]; listed {
			rosterUsed[rel] = true
			return nil
		}
		undeclared = append(undeclared, rel+" dispatches on a raw column type ("+strings.Join(sites, ",")+
			") but has NO domaingate gate file and is NOT in ungatedDispatchRoster — add a gate file or a written roster reason (audit A-3)")
		return nil
	})

	for _, u := range undeclared {
		t.Errorf("domaingate meta-walker: %s", u)
	}
	// A roster entry that no longer dispatches (or vanished) is stale.
	for rel := range ungatedDispatchRoster {
		if !rosterUsed[rel] {
			t.Errorf("domaingate meta-walker: ungatedDispatchRoster entry %q no longer matches a raw-dispatching, ungated package — remove it (the debt was paid or the code moved)", rel)
		}
	}
	// Anti-vacuity: the walk must actually be finding things.
	if scanned < 20 {
		t.Fatalf("meta-walker scanned only %d dirs — the walk is broken", scanned)
	}
	if gatedFound < 3 {
		t.Fatalf("meta-walker found only %d gated dispatchers; expected >=3 (mysql, sqlite, translate) — detection broke", gatedFound)
	}
}
