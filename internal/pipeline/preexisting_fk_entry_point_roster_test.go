// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

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

// EVERY COPY ENTRY POINT MUST HAVE AN ANSWER FOR THE PRE-EXISTING-FOREIGN-KEY
// CHECK — COVERED, OR EXEMPT WITH A REASON (roadmap item 140).
//
// # Why this gate exists, and why it is not the sibling next door
//
// Item 140's check rides [existingTablesGate], which has two constructors, so
// it shipped reaching two of the six declarations
// [indexPreflightEntryPoints] rosters. Its doc named four exclusions
// honestly — and all four were BRANCHES INSIDE those two callers, so the four
// sibling ENTRY POINTS were neither covered nor mentioned. That is the
// sibling-sweep shape in its expensive direction: the enumeration that existed
// read as complete and was scoped to the wrong axis.
//
// TestIndexEmitPreflightReachesEveryCopyEntryPoint cannot carry this. Its
// contract is that every rostered declaration calls ALL of its helpers, and
// this check is not one of them: two entry points satisfy it indirectly
// (through the gate, in a different phase and a different function) and three
// are deliberately exempt. A "must call" roster with three exemptions and two
// indirections would have to lie in one direction or the other. So this is its
// own fail-by-default map, DERIVED from that roster's entry-point set so the
// two cannot drift: a sixth copy path added there with no posture here fails
// this test.
//
// # Scope, stated so the name cannot be read as broader than the truth
//
// For a COVERED entry point this proves that the declaration named as carrying
// its answer really does call the check — an AST fact. It does NOT prove that
// the entry point reaches that declaration; for the two that go through
// [existingTablesGate] that link is held by the shape-gate tests
// (TestMigrateShapeGate_* / TestColdStartShapeGate_*) and, on a real server, by
// TestMigrate_PreExistingTargetForeignKeys_MySQL. For an EXEMPT entry point it
// proves only that a reason was written down; the reasons themselves are
// argued in migrate_existing_tables_fks.go, which is where the scope is
// defined.
//
// The reverse direction IS mechanical and is the half that catches drift: a
// call to either check from a declaration this map does not name fails, so a
// future wiring cannot land without joining the roster.

// preExistingFKChecks are the two forms of the item-140 verdict. A declaration
// calling either is a call site of the check and must be rostered.
var preExistingFKChecks = []string{
	"checkPreExistingForeignKeys",
	"readAndCheckPreExistingForeignKeys",
}

// fkEntryPointPosture is one copy entry point's answer. Exactly one of via/why
// shapes applies: `via` non-empty means COVERED by that declaration (which is
// verified to call the check); empty means EXEMPT and `why` is the whole of the
// claim.
type fkEntryPointPosture struct {
	via string
	why string
}

// preExistingFKEntryPointPosture is the DECLARED roster, keyed exactly as
// [indexPreflightEntryPoints] is (package-relative directory → declaration).
// Fail-by-default in both directions: an entry point with no posture fails, and
// a posture for an entry point that roster no longer lists fails.
//
// Keep the reasons in sync with the roster table in
// migrate_existing_tables_fks.go — that comment is where the exemptions are
// argued, and the point of this gate is that the argument cannot silently
// outlive the code.
var preExistingFKEntryPointPosture = map[string]map[string]fkEntryPointPosture{
	".": {
		"(*Migrator).phaseTranslateAndGateSchema": {
			via: "(*existingTablesGate).plan",
			why: "`migrate`: the check rides the ADR-0166 shape gate's target-catalog read, one phase later " +
				"than this declaration, so it costs no extra round trip",
		},
		"(*Streamer).coldStart": {
			via: "(*existingTablesGate).plan",
			why: "`sync` cold-start, single database: same gate, same read, via coldStartGatePreflight",
		},
		"(*Streamer).coldStartCopyOneDatabase": {
			via: "(*Streamer).coldStartCopyOneDatabase",
			why: "`sync` cold-start, multi-database fan-out: a FIRST copy per database with no shape compare " +
				"to ride, so it takes the catalog read itself — the one genuine sibling the first cut missed",
		},
		"(*AddTable).Run": {
			why: "EXEMPT: the scoped schema holds tables that are not on the target yet, so the catalog lookup " +
				"misses and the verdict is a no-op by construction — while the read it needs is not free. A " +
				"check that can only be vacuous or wrong is worth naming, not wiring",
		},
	},
	"backup": {
		"(*Restore).refuseUnrepresentableTargetShape": {
			why: "EXEMPT: restore is idempotent by contract (CREATE TABLE IF NOT EXISTS + upsert), so a re-run " +
				"of a completed restore reads back the foreign keys its OWN constraints phase created with " +
				"every parent in scope — the check would refuse it for having worked. Same argument as the " +
				"--resume exclusion. The residual is real and filed (roadmap item 140 / matrix gap 20)",
		},
		"(*ChainRestore).refuseUnrepresentableTargetShape": {
			why: "EXEMPT: worse than a re-run — ChainRestore.applyFull IS built out of them, re-entering " +
				"Restore.Run once per segment with segment 0 establishing constraints, so every later segment " +
				"would meet segment 0's own foreign keys. `sync from-backup` inherits this: its cold-start " +
				"reset leg is a ChainRestore",
		},
	},
}

// callersOfFKCheck parses every non-test .go file in dir and returns the set of
// declarations calling any of [preExistingFKChecks], keyed as
// [indexPreflightEntryPoints] keys are.
func callersOfFKCheck(t *testing.T, dir string) (callers map[string]bool, parsed int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	want := map[string]bool{}
	for _, c := range preExistingFKChecks {
		want[c] = true
	}
	callers = map[string]bool{}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		parsed++
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !want[sel.Sel.Name] {
					return true
				}
				callers[declIdentity(fn)] = true
				return true
			})
		}
	}
	return callers, parsed
}

func TestPreExistingFKCheckNamesEveryCopyEntryPoint(t *testing.T) {
	// The check itself lives in package pipeline; the `backup` entry points are
	// rostered as exempt, so no call is expected there. Discover once.
	callers, parsed := callersOfFKCheck(t, ".")
	if parsed < 5 {
		t.Fatalf("parsed only %d non-test files in %q — the walk is not seeing the package", parsed, ".")
	}
	// Anti-vacuity: a rename of either check would empty this set and every
	// "covered" assertion below would then pass on nothing.
	if len(callers) == 0 {
		t.Fatalf("no call to any of %v found in the pipeline package. Either both checks were renamed "+
			"(re-point this gate) or roadmap item 140 has been removed entirely", preExistingFKChecks)
	}

	var covered, exempt []string
	for dir, wanted := range indexPreflightEntryPoints {
		for decl := range wanted {
			posture, ok := preExistingFKEntryPointPosture[dir][decl]
			if !ok {
				t.Errorf("%s in %q is a rostered copy entry point with NO pre-existing-foreign-key posture.\n\n"+
					"Every path that copies into a target an operator may have branched from an existing "+
					"database owes an answer here: cover it (roadmap item 140 — the failure is Error 1452 / "+
					"SQLSTATE 23503 about twenty seconds in), or exempt it in "+
					"preExistingFKEntryPointPosture with the reason, and argue the reason in "+
					"migrate_existing_tables_fks.go.", decl, dir)
				continue
			}
			if strings.TrimSpace(posture.why) == "" {
				t.Errorf("%s/%s declares a posture with no reason; an unexplained exemption is "+
					"indistinguishable from an oversight", dir, decl)
			}
			if posture.via == "" {
				exempt = append(exempt, dir+"/"+decl)
				continue
			}
			covered = append(covered, dir+"/"+decl)
			if !callers[posture.via] {
				got := make([]string, 0, len(callers))
				for c := range callers {
					got = append(got, c)
				}
				sort.Strings(got)
				t.Errorf("%s in %q is declared COVERED via %s, but %s calls neither %v.\n\n"+
					"Either the coverage regressed — in which case a target branched from an existing "+
					"database now fails mid-copy on this path with nothing having warned — or the check "+
					"moved and this roster must follow it.\ncall sites found: %v",
					decl, dir, posture.via, posture.via, preExistingFKChecks, got)
			}
		}
	}

	// The other direction: a posture for an entry point the shared roster no
	// longer lists is a claim about a path that does not exist.
	for dir, postures := range preExistingFKEntryPointPosture {
		for decl := range postures {
			if _, ok := indexPreflightEntryPoints[dir][decl]; !ok {
				t.Errorf("preExistingFKEntryPointPosture declares %q in %q, which is not a rostered copy "+
					"entry point — remove it, or add it to indexPreflightEntryPoints", decl, dir)
			}
		}
	}

	// And the direction that catches a NEW wiring landing unrostered: every
	// call site must be named by some posture's `via`. This is what stops the
	// roster from quietly becoming narrower than the code.
	declared := map[string]bool{}
	for _, postures := range preExistingFKEntryPointPosture {
		for _, p := range postures {
			if p.via != "" {
				declared[p.via] = true
			}
		}
	}
	// The two forms call each other; the read-taking wrapper is plumbing, not
	// an entry point, and is named here rather than filtered silently.
	declared["(*existingTablesGate).readAndCheckPreExistingForeignKeys"] = true
	for c := range callers {
		if !declared[c] {
			t.Errorf("%s calls the item-140 check but no entry-point posture names it as its `via`. "+
				"Add the entry point it serves to preExistingFKEntryPointPosture, so the roster stays the "+
				"answer to \"which copy paths ask this?\"", c)
		}
	}

	// BOTH buckets must be populated. A roster that collapsed to one answer
	// would agree with itself: all-covered would mean the exemptions were
	// dropped without anyone reading them, all-exempt that the check is dead.
	if len(covered) == 0 || len(exempt) == 0 {
		t.Errorf("covered=%v exempt=%v — this roster exists because BOTH postures are present; an empty "+
			"bucket means the derivation broke rather than that the pipeline changed", covered, exempt)
	}
}
