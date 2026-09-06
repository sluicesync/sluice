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

// EVERY FUNCTION THAT OPENS A CHANGE STREAM EITHER SEEDS THE READER'S PRIOR
// SHAPE OR IS EXEMPT HERE, BY NAME, WITH A REASON.
//
// # Why this exists, and why the older gate could not have caught it
//
// TestSchemaSeed_WiredWhereverTheRefusalIsArmed next door rosters the
// functions that ARM the session-zone refusal (they type-assert
// [schemaDeltaTargetApplySetter]) and requires each to seed. Its universe is
// therefore "sites that arm" — and the multi-database fan-out's two
// reader-open sites arm nothing, because [Streamer.schemaDeltaAppliesToTarget]
// is false in multi-database mode by construction. They sat OUTSIDE the gate's
// universe, unseeded, for as long as they existed, and the gate stayed green
// the whole time. Audit 2026-09-06 finding 1 measured the consequence on
// postgres:16: a `timestamptz`→`timestamp` swap performed while a `--schemas`
// stream was stopped primed silently on warm resume and the target's rows
// ended nine hours off the source's, at exit 0.
//
// The Postgres lane's seeded door does not consult the arming flag at all —
// it fires on the seed alone — so "arms" was never the right universe. This
// gate takes the wider one: a site that opens a change stream is a site where
// a first boundary can arrive, and a first boundary with no prior is the
// SLM-1 window regardless of what else that site wires.
//
// # What it reaches, stated so the name cannot be read as broader
//
// The universe is DERIVED: every function in the package's non-test files
// whose body contains a call to a method named StreamChanges. It finds the
// call by NAME, not by type — a reader opened through a differently-named
// method, or a stream opened in another package, is outside it. Seeding is
// checked through the package-local call graph (package-level functions and
// calls on the function's own receiver), so a site that seeds through a
// helper still passes; a site that seeds through a function value or another
// type's method reads as unseeded, which is the false-positive direction a
// reviewer resolves by reading the code.
//
// The exemption map is the roster's other half: an entry that no longer opens
// a stream fails too, so the list cannot rot into a blanket.
func TestSchemaSeed_ReachesEveryReaderOpenSite(t *testing.T) {
	// The change-stream opens that deliberately do NOT seed, each with the
	// reason. Both are backup-chain lanes: they stream changes into chunk
	// files, never into a target, so no schema delta is ever re-applied and
	// there is no target column for a re-zoned value to land in wrongly. They
	// also have no Streamer receiver — [Streamer.wireReaderSchemaSeed] is not
	// reachable from them at all — and a seed would have no consumer, since
	// the refusal it feeds exists to protect a target that these lanes do not
	// write.
	exempt := map[string]string{
		"(*BackupStream).Run": "backup chain lane: changes are written to chunk files, not applied to a target, " +
			"so no re-zoned value can land in a target column",
		"(*BackupStream).newRolloverLoop": "backup chain lane, same as (*BackupStream).Run — this is its " +
			"pump-open half",
		"(*IncrementalBackup).Run": "backup incremental lane: changes are written to chunk files, not applied " +
			"to a target",
	}

	const dir = "."
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}

	opens := map[string]string{} // decl identity -> file
	calls := map[string]map[string]bool{}
	seeders := map[string]bool{} // decls that call wireReaderSchemaSeed directly
	parsed := 0
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
			self := declIdentity(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch callee := call.Fun.(type) {
				case *ast.Ident:
					if calls[self] == nil {
						calls[self] = map[string]bool{}
					}
					calls[self][callee.Name] = true
				case *ast.SelectorExpr:
					switch callee.Sel.Name {
					case "StreamChanges":
						opens[self] = name
					case "wireReaderSchemaSeed":
						seeders[self] = true
					}
					if x, ok := callee.X.(*ast.Ident); ok && x.Name == receiverName(fn) {
						if calls[self] == nil {
							calls[self] = map[string]bool{}
						}
						calls[self][receiverType(fn)+"."+callee.Sel.Name] = true
					}
				}
				return true
			})
		}
	}

	if parsed < 5 {
		t.Fatalf("parsed only %d non-test files in %q — the walk is not seeing the package", parsed, dir)
	}
	// Anti-vacuity, both halves. SIX opens exist today: four sync reader-opens
	// (single-stream cold start + warm resume, multi-database cold start +
	// warm resume) and the two backup-chain lanes' three sites. A walk that
	// found fewer has stopped matching the call shape.
	if len(opens) < 6 {
		got := make([]string, 0, len(opens))
		for d := range opens {
			got = append(got, d)
		}
		sort.Strings(got)
		t.Fatalf("found %d function(s) calling StreamChanges (%v); at least 6 exist. The call shape this "+
			"gate matches has changed — re-point it, do not lower the floor.", len(opens), got)
	}
	if len(seeders) == 0 {
		t.Fatal("nothing in the package calls wireReaderSchemaSeed — either it was renamed (re-point this " +
			"gate) or the seeding surface is gone, in which case every assertion below is vacuous.")
	}

	seeded := 0
	for decl, file := range opens {
		if reachesCall(decl, seeders, calls) {
			seeded++
			if why, isExempt := exempt[decl]; isExempt {
				t.Errorf("%s (%s) is listed as exempt from seeding (%q) but now DOES seed — drop the "+
					"exemption so the roster keeps meaning what it says", decl, file, why)
			}
			continue
		}
		if _, isExempt := exempt[decl]; isExempt {
			continue
		}
		t.Errorf("%s (%s) opens a change stream but never reaches wireReaderSchemaSeed.\n\n"+
			"A reader opened without a seed has no prior shape at each table's FIRST schema boundary of "+
			"the process, so a value-changing DDL performed while the stream was STOPPED (a "+
			"timestamptz/timestamp swap under a differing session TimeZone) primes the cache instead of "+
			"refusing, and every post-swap row lands in the target's unchanged column at exit 0 (audit "+
			"2026-09-06 finding 1, OBSERVED on postgres:16 for the multi-database lane). Wire "+
			"wireReaderSchemaSeed before StreamChanges — and make sure something ASSIGNS "+
			"Streamer.readerSchemaSeed on that path, since the helper is a no-op without a loader — or "+
			"add %q to this file's exempt map with the reason it cannot lose a value.", decl, file, decl)
	}
	// The four sync reader-opens must all be seeding. Without this floor an
	// exemption map that grew to cover everything would pass.
	if seeded < 4 {
		t.Errorf("only %d change-stream open(s) reach wireReaderSchemaSeed; floor 4 (single-stream cold "+
			"start + warm resume, multi-database cold start + warm resume)", seeded)
	}
	for decl := range exempt {
		if _, stillOpens := opens[decl]; !stillOpens {
			t.Errorf("the exempt map lists %s, but nothing by that name opens a change stream any more — "+
				"drop the entry rather than leaving a stale blanket", decl)
		}
	}
}

// TestMultiDatabaseWarmResumeSeed_ReadsBothPriors is the fan-out twin of the
// two-witness check TestSchemaSeed_WiredWhereverTheRefusalIsArmed makes for
// the single-stream loader: a loader that dropped either witness would resume
// on a narrower prior with nothing failing.
func TestMultiDatabaseWarmResumeSeed_ReadsBothPriors(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "schema_seed_multidb.go", nil, 0)
	if err != nil {
		t.Fatalf("parse schema_seed_multidb.go: %v", err)
	}
	called := map[string]bool{}
	found := false
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Name.Name != "loadMultiDatabaseWarmResumeSchemaSeed" {
			continue
		}
		found = true
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch callee := call.Fun.(type) {
			case *ast.Ident:
				called[callee.Name] = true
			case *ast.SelectorExpr:
				called[callee.Sel.Name] = true
			}
			return true
		})
	}
	if !found {
		t.Fatal("loadMultiDatabaseWarmResumeSchemaSeed is gone or renamed; re-point this gate")
	}
	for _, want := range []string{
		"loadRetainedSchemaSeed",
		"loadMultiDatabaseTargetZoneWitness",
		"mergeWarmResumeSeed",
	} {
		if !called[want] {
			t.Errorf("loadMultiDatabaseWarmResumeSchemaSeed no longer calls %s; the fan-out warm-resume "+
				"prior lost one of its two witnesses", want)
		}
	}
}
