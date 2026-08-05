// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The G-2 divergence map for [BatchConfig.CheckpointOnlyAtTxBoundary]
// (roadmap item 132, audit 2026-08-05 A-2).
//
// # Why this gate exists
//
// CheckpointOnlyAtTxBoundary is a knob on the TARGET applier that encodes a
// property of the SOURCE: "is a position stamped mid-source-transaction a
// legal restart point?". Nothing about the target can answer that, because
// every target can be fed by every source. The knob was set on the MySQL
// applier and left false on the Postgres one, with a written justification —
// "replay from that position via idempotency reproduces the missed work" —
// that is true for a Postgres source and false for a MySQL one, on both
// position modes:
//
//   - GTID mode: before item 132 the reader folded a transaction's GTID into
//     the running executed set at the transaction's START, so every row's
//     position already CONTAINED its own transaction. StartSyncGTID with such
//     a set skips that transaction entirely — silent loss of its tail.
//   - file/pos mode: go-mysql seeks to the persisted byte offset, so a
//     position pointing INTO a transaction lands after the TABLE_MAP that
//     names the row event's table and the stream crash-loops with "no
//     corresponding table map event".
//
// So the gate is fail-by-default over the knob rather than a review habit:
// every engine that drives the shared batch loop either sets the flag true,
// or carries a written exemption in [checkpointBoundaryExempt] arguing that
// a mid-transaction position from EVERY possible source engine is a valid
// restart point for it.
//
// # What this gate reaches, stated so the name cannot read broader than
// the truth (the 2026-08-02 rule)
//
// It walks internal/engines/**, non-test files only, and covers:
//
//  1. every [BatchConfig] composite literal, plus any later assignment to the
//     field, in an engine package — the construction sites; and
//  2. every ApplyBatch method declaration (the [ir.BatchedChangeApplier]
//     surface) — the roster of appliers that CAN reach the shared loop.
//
// Cross-checked: an applier in (2) whose package has no site in (1) is a
// finding, because it means the applier hand-rolls a batch loop this gate
// cannot see. Both halves carry an anti-vacuity floor, so a walker that
// stops matching the real code fails loudly instead of passing on zero
// findings.
//
// It does NOT reach: the CONCURRENT laneapply path (which enforces the same
// invariant structurally via its seq-frontier — it never persists a partial
// point, see internal/laneapply), per-change Apply implementations that never
// touch BatchConfig, or engines whose OpenChangeApplier returns
// ErrNotImplemented. internal/engines/pgtrigger is deliberately absent from
// both rosters: it delegates OpenChangeApplier to the Postgres engine and
// therefore inherits the Postgres applier's config verbatim.
const enginesRootForCheckpointGate = "../engines"

// checkpointBoundaryField is the knob under gate. Named once so a rename
// trips the anti-vacuity floor rather than silently emptying the walk.
const checkpointBoundaryField = "CheckpointOnlyAtTxBoundary"

// checkpointBoundaryExempt maps an engine package directory (relative to
// internal/engines) to the ARCHITECTURAL REASON its applier may persist a
// resume position mid-source-transaction. An exemption must argue the claim
// for every source engine that can feed that target, not just the one the
// author had in mind — the item-132 defect is exactly a justification that
// held for one source and was applied to all of them.
//
// EMPTY as of item 132: both appliers set the flag. The Postgres applier
// joined the MySQL one rather than taking an exemption, because no honest
// exemption could be written for it — a MySQL file/pos source pointed at a
// Postgres target had (and, on the reader side, still has) no valid mid-
// transaction restart point.
//
// The map and its enforcement stay so the moment a new engine's applier
// appears without the flag, this gate fails until it is either set or
// exempted here with a reason. A stale entry cannot rot silently either:
// an exemption naming a package that DOES set the flag is itself a failure.
var checkpointBoundaryExempt = map[string]string{}

// Anti-vacuity floors. Both halves of the walk must keep finding the real
// code; a walker that matches nothing would otherwise pass forever.
const (
	minCheckpointBatchConfigSites = 2 // mysql + postgres
	minCheckpointApplyBatchImpls  = 2 // mysql + postgres
)

// TestCheckpointOnlyAtTxBoundaryDivergenceMap is the G-2 gate. See the
// package-level commentary above for scope and for what it deliberately
// does not reach.
func TestCheckpointOnlyAtTxBoundaryDivergenceMap(t *testing.T) {
	sites, appliers := walkEngineAppliers(t)

	if len(sites) < minCheckpointBatchConfigSites {
		t.Fatalf("found %d %s construction site(s) under %s (floor %d); the walker has probably stopped "+
			"matching the real config (a rename, a move) and is now vacuous — re-point it",
			len(sites), "appliershared.BatchConfig", enginesRootForCheckpointGate, minCheckpointBatchConfigSites)
	}
	if len(appliers) < minCheckpointApplyBatchImpls {
		t.Fatalf("found %d ApplyBatch implementation(s) under %s (floor %d); the applier roster has gone "+
			"vacuous — re-point it", len(appliers), enginesRootForCheckpointGate, minCheckpointApplyBatchImpls)
	}

	// Fold the per-site settings into a per-package verdict. A package
	// passes when at least one site sets the field true and none sets it
	// false; anything else (absent, or a non-literal value the gate cannot
	// evaluate) is a finding.
	type verdict struct {
		sawTrue  bool
		sawFalse bool
		sawOther bool
		where    []string
	}
	byPkg := map[string]*verdict{}
	for _, s := range sites {
		v := byPkg[s.pkg]
		if v == nil {
			v = &verdict{}
			byPkg[s.pkg] = v
		}
		v.where = append(v.where, s.where)
		switch s.value {
		case "true":
			v.sawTrue = true
		case "false":
			v.sawFalse = true
		case "":
			// The literal simply omits the field: the Go zero value is
			// false, which is the unsafe setting. Treat as absent-false.
			v.sawFalse = true
		default:
			v.sawOther = true
		}
	}

	// Every applier in the roster must have a config site the gate can see.
	for _, a := range appliers {
		if _, ok := byPkg[a.pkg]; !ok {
			t.Errorf("applier %s declares ApplyBatch at %s but its package has no %s construction site — "+
				"it hand-rolls a batch loop this gate cannot see. Route it through appliershared, or the "+
				"item-132 mid-transaction-checkpoint question goes unanswered for it.",
				a.name, a.where, "appliershared.BatchConfig")
		}
	}

	var offenders []string
	for _, pkg := range sortedKeys(byPkg) {
		v := byPkg[pkg]
		reason, exempt := checkpointBoundaryExempt[pkg]
		ok := v.sawTrue && !v.sawFalse && !v.sawOther
		switch {
		case ok && exempt:
			t.Errorf("engine %q sets %s=true AND carries an exemption (%q) — the exemption is STALE. "+
				"Remove it: an exemption that describes nothing stops the next reader from checking.",
				pkg, checkpointBoundaryField, reason)
		case ok:
			// Compliant.
		case exempt && strings.TrimSpace(reason) != "":
			// Deliberate divergence, recorded with a reason.
		default:
			offenders = append(offenders, pkg+"  ("+strings.Join(v.where, ", ")+")")
		}
	}

	if len(offenders) > 0 {
		t.Errorf("%d engine applier(s) neither set %s=true nor carry a written exemption:\n  %s\n\n"+
			"A false setting means the applier will persist the resume position on a row-cap / byte-cap / "+
			"idle / channel-close flush that lands MID-SOURCE-TRANSACTION. That is only legal if a mid-"+
			"transaction position is a valid restart point for EVERY source engine that can feed this "+
			"target — which is false for a MySQL binlog source in either position mode (roadmap item 132). "+
			"Set the flag, or add a %s entry whose reason argues the claim per source engine.",
			len(offenders), checkpointBoundaryField, strings.Join(offenders, "\n  "), "checkpointBoundaryExempt")
	}
}

// configSite is one place an engine sets (or fails to set) the gated field:
// a BatchConfig composite literal, or a later assignment to the field.
type configSite struct {
	pkg   string // engine package dir relative to internal/engines, e.g. "mysql"
	where string // file:line, repo-relative
	value string // "true" / "false" / "" (field omitted) / other source text
}

// applierDecl is one ApplyBatch method declaration — the roster half.
type applierDecl struct {
	pkg   string
	name  string // receiver type + method, e.g. "*ChangeApplier.ApplyBatch"
	where string
}

// walkEngineAppliers parses every non-test Go file under internal/engines and
// returns the BatchConfig construction/assignment sites and the ApplyBatch
// roster. Parse failures are fatal: a gate that silently skips a file it
// cannot read is the vacuous shape this floor exists to prevent.
func walkEngineAppliers(t *testing.T) (sites []configSite, appliers []applierDecl) {
	t.Helper()

	fset := token.NewFileSet()
	err := filepath.WalkDir(enginesRootForCheckpointGate, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return fs.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		pkg := enginePkgOf(path)
		src, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CompositeLit:
				if !isBatchConfigType(node.Type) {
					return true
				}
				value, at := literalFieldValue(node, src, fset)
				if at == token.NoPos {
					// Field omitted entirely — point at the literal instead.
					at = node.Pos()
				}
				sites = append(sites, configSite{
					pkg:   pkg,
					where: posOf(fset, at),
					value: value,
				})
			case *ast.AssignStmt:
				for i, lhs := range node.Lhs {
					sel, ok := lhs.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != checkpointBoundaryField || i >= len(node.Rhs) {
						continue
					}
					sites = append(sites, configSite{
						pkg:   pkg,
						where: posOf(fset, node.Pos()),
						value: exprSource(node.Rhs[i], src, fset),
					})
				}
			case *ast.FuncDecl:
				if node.Recv == nil || node.Name.Name != "ApplyBatch" {
					return true
				}
				appliers = append(appliers, applierDecl{
					pkg:   pkg,
					name:  pkg + "." + receiverTypeName(node) + ".ApplyBatch",
					where: posOf(fset, node.Pos()),
				})
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", enginesRootForCheckpointGate, err)
	}
	return sites, appliers
}

// isBatchConfigType reports whether a composite literal's type is
// appliershared.BatchConfig (engine packages import it qualified).
func isBatchConfigType(e ast.Expr) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "BatchConfig" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "appliershared"
}

// literalFieldValue returns the source text of the gated field's value inside
// a BatchConfig literal and the position to blame, or ("", token.NoPos) when
// the literal omits the field (Go zero value — false, the unsafe setting).
func literalFieldValue(lit *ast.CompositeLit, src []byte, fset *token.FileSet) (string, token.Pos) {
	for _, el := range lit.Elts {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != checkpointBoundaryField {
			continue
		}
		return exprSource(kv.Value, src, fset), kv.Pos()
	}
	return "", token.NoPos
}

// exprSource renders an expression back to its source text.
func exprSource(e ast.Expr, src []byte, fset *token.FileSet) string {
	start := fset.Position(e.Pos()).Offset
	end := fset.Position(e.End()).Offset
	if start < 0 || end > len(src) || start >= end {
		return "<unrenderable>"
	}
	return strings.TrimSpace(string(src[start:end]))
}

// receiverTypeName renders a method's receiver type for the roster message.
func receiverTypeName(fn *ast.FuncDecl) string {
	if len(fn.Recv.List) == 0 {
		return "?"
	}
	switch rt := fn.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if id, ok := rt.X.(*ast.Ident); ok {
			return "*" + id.Name
		}
	case *ast.Ident:
		return rt.Name
	}
	return "?"
}

// enginePkgOf reduces a walked path to the engine package directory name.
func enginePkgOf(path string) string {
	rel, err := filepath.Rel(enginesRootForCheckpointGate, path)
	if err != nil {
		return filepath.Dir(path)
	}
	return filepath.ToSlash(filepath.Dir(rel))
}

func posOf(fset *token.FileSet, p token.Pos) string {
	pos := fset.Position(p)
	return filepath.ToSlash(pos.Filename) + ":" + strconv.Itoa(pos.Line)
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
