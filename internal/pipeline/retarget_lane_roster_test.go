// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Which lane each retarget call site is on, derived rather than
// remembered (roadmap item 153).
//
// [translate.RetargetForEngine] and [translate.RetargetForShapeCompare]
// differ in one thing: whether a Postgres `CREATE DOMAIN` wrapper is
// read through its storage type. Picking the wrong one is silent in
// both directions and expensive in both:
//
//   - a COMPARE site on RetargetForEngine compares `Domain{d AS X}`
//     against the base type a MySQL catalog reads back and REFUSES every
//     re-run over tables sluice itself created (item 153, the loud
//     direction);
//   - an EMIT site on RetargetForShapeCompare hands the target's DDL
//     writer a column whose Type is no longer a domain, so
//     [ir.DomainOf] stops matching and the domain's CHECK constraints
//     are DROPPED from the emitted table — silently, on the restore and
//     broker replay lanes (the worse direction).
//
// So the choice is a decision that gets recorded, not a habit. Every
// call site is keyed below with the lane it is on and why; a new one
// fails this test until its author writes that down.
//
// # What this reaches, stated so the name cannot be read as broader
//
// The three orchestrator packages that call the retarget at all —
// internal/pipeline, internal/pipeline/backup, internal/pipeline/migcore
// — walked as source. It does NOT grade internal/translate's own
// callers elsewhere (there are none today; a call from a fourth package
// would go unseen, which is why the floor below is a floor and not an
// equality), and it says nothing about whether a site on the right lane
// then uses the result correctly.

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// retargetLane is the two-valued classification a call site declares.
type retargetLane string

const (
	// laneEmit: the result is handed to a SchemaWriter or a RowWriter.
	// It must keep the DOMAIN wrapper — the target's DDL emitter reads
	// it through ir.DomainOf for the CHECK translation and the
	// CHECK-drop WARN.
	laneEmit retargetLane = "RetargetForEngine"

	// laneCompare: the result is compared against a target catalog's
	// read-back. It must be flattened to the storage the target
	// actually holds.
	laneCompare retargetLane = "RetargetForShapeCompare"
)

// retargetCallSites is the fail-by-default divergence map: every
// retarget call site in the walked packages, keyed
// "<pkgdir>/<file>.go:<func>", with the lane it declares and the reason.
var retargetCallSites = map[string]struct {
	lane   retargetLane
	reason string
}{
	"pipeline/diff.go:Run": {
		laneCompare,
		"the `schema diff` COMPARISON side. The same function also builds an `expectedDDL` on the EMIT " +
			"lane for previewMissingDDL's CREATE TABLE suggestions; both appear under this one key " +
			"because they are one decision made twice on purpose, and diff.go's comment says which is which.",
	},
	"pipeline/migrate_existing_tables.go:plan": {
		laneCompare,
		"the ADR-0166 pre-create shape gate — a pure comparison against the target catalog, discarded " +
			"afterwards. The CREATE phase keeps the untouched schema.",
	},
	"pipeline/schema_forward_intercept.go:retargetTableScrub": {
		laneEmit,
		"CDC schema-forward: the retargeted table goes straight to the target SchemaWriter's " +
			"CREATE/ALTER emit.",
	},
	"pipeline/schema_forward_intercept.go:retargetAddedColumns": {
		laneEmit,
		"CDC schema-forward ADD COLUMN: emitted via the target's SchemaDeltaApplier.",
	},
	"pipeline/broker.go:applySchemaDeltas": {
		laneEmit,
		"the from-backup broker's schema-delta replay: CreateTablesWithoutConstraints on the result.",
	},
	"backup/restore.go:Run": {
		laneEmit,
		"`sluice restore` builds the target tables from this schema AND hands the same columns to the " +
			"row writer; flattening it would drop every DOMAIN's CHECKs from the restored DDL. " +
			"`sluice chain restore`'s full-restore leg reaches the writer through this same call.",
	},
	"backup/chain_restore.go:applySchemaDeltas": {
		laneEmit,
		"chain-restore schema-delta replay: CreateTablesWithoutConstraints on the result.",
	},
	"backup/chain_restore.go:syncIdentitySequencesAtTail": {
		laneEmit,
		"the tail identity/sequence sync opens a SchemaWriter and emits against these tables. No DOMAIN " +
			"reaches the decision (identity columns are integers), but the lane is what it is.",
	},
	"migcore/incremental_delta_shape.go:ApplyAlterDelta": {
		laneEmit,
		"the shared ALTER replay both the broker and chain restore call: emitted via the target's " +
			"SchemaDeltaApplier / ShapeDeltaApplier.",
	},
}

// retargetWalkedPackages are the package directories walked, relative to
// internal/pipeline.
var retargetWalkedPackages = map[string]string{
	"pipeline": ".",
	"backup":   "backup",
	"migcore":  "migcore",
}

// TestRetargetCallSitesDeclareTheirLane fails on a call site that is not
// in [retargetCallSites], on one whose declared lane does not match the
// function it actually calls, and on a roster entry no longer backed by
// a call site.
func TestRetargetCallSitesDeclareTheirLane(t *testing.T) {
	found := make(map[string]map[retargetLane]bool)
	total := 0

	for pkgName, dir := range retargetWalkedPackages {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", filepath.Join(dir, name), err)
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				ast.Inspect(fn, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					pkg, ok := sel.X.(*ast.Ident)
					if !ok || pkg.Name != "translate" {
						return true
					}
					lane := retargetLane(sel.Sel.Name)
					if lane != laneEmit && lane != laneCompare {
						return true
					}
					key := pkgName + "/" + name + ":" + fn.Name.Name
					if found[key] == nil {
						found[key] = make(map[retargetLane]bool)
					}
					found[key][lane] = true
					total++
					return true
				})
			}
		}
	}

	// Anti-vacuity: the walk must find the sites that exist. Nine keys
	// across ten calls today (diff.go:Run makes two, deliberately).
	if total < 9 {
		t.Fatalf("the walk found only %d retarget call site(s); the dispatch shape has changed (a rename, "+
			"an import alias, a wrapper helper) and this gate is grading nothing", total)
	}

	var problems []string
	for key, lanes := range found {
		entry, declared := retargetCallSites[key]
		if !declared {
			problems = append(problems, "UNDECLARED call site "+key+
				" — add it to retargetCallSites with the lane it is on and WHY. "+
				"A comparison against a target catalog takes RetargetForShapeCompare; anything handed to a "+
				"SchemaWriter takes RetargetForEngine, because flattening a DOMAIN there drops its CHECKs "+
				"from the emitted DDL")
			continue
		}
		if !lanes[entry.lane] {
			var actual []string
			for l := range lanes {
				actual = append(actual, string(l))
			}
			sort.Strings(actual)
			problems = append(problems, key+" declares lane "+string(entry.lane)+
				" but calls "+strings.Join(actual, "+")+" — the declaration and the code disagree: "+entry.reason)
		}
	}
	for key := range retargetCallSites {
		if _, ok := found[key]; !ok {
			problems = append(problems, "STALE roster entry "+key+
				" — no retarget call site there any more; remove it so the map stays a description "+
				"rather than a wish")
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		t.Errorf("retarget lane roster is out of date:\n  %s", strings.Join(problems, "\n  "))
	}
}
