// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

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

// flavorDoorExemptMarker lets an Open* door declare that it deliberately
// does not run [Engine.checkServerFlavor], with the reason at the site.
const flavorDoorExemptMarker = "flavor-door-exempt:"

// flavorDoorsThatMustProbe is the floor stated by NAME rather than by
// count: each of these doors reaches openDB and must reach the probe. A
// count floor can be satisfied by exempting the door that matters and
// probing two that do not; a name cannot. The six engine.go doors are the
// ones Bug 280's fix enumerated; the two CDC doors are the ones its roster
// then failed to see (audit 2026-09-15, reconciler §2 item 13) because
// they delegate to package functions and the first cut recorded that
// delegation as `checks = checks || false` — a literal no-op under a
// comment claiming otherwise.
var flavorDoorsThatMustProbe = []string{
	"OpenSchemaReader", "OpenSchemaWriter", "OpenRowReader", "OpenRowWriter",
	"OpenMigrationStateStore", "OpenChangeApplier",
	"OpenCDCReader", "OpenServerCDCReader",
}

// Every Engine door that opens a connection either runs the flavor probe
// or says at the site why it does not.
//
// The probe carries two things: a WARN steering a MariaDB server off the
// mysql driver, and a REFUSAL of Vitess under a non-VStream flavor, which
// is a silent-loss guard (the vanilla flavor's full scans run without
// `set workload=olap`, and Vitess's OLTP workload truncates result sets
// at its row cap). Both lived at exactly two of the doors —
// OpenSchemaReader and OpenSchemaWriter — and their coverage rested on an
// unwritten assumption: that every path reaching data opens one of those
// first.
//
// The v0.148.2 regression cycle measured that assumption failing (Bug
// 280). The migrate pipeline opens the migration-state store at phase
// 1.75, ahead of both schema doors, and that store's SQL is itself
// flavor-specific — so a MariaDB target addressed with
// `--target-driver mysql` died on `Error 1064 … near 'AS new ON
// DUPLICATE KEY UPDATE'`, sluice's own statement, while the WARN naming
// the right driver never ran. The diagnosis was pre-empted by its own
// symptom, and the silent-loss refusal rode in the same probe.
//
// So the classification is written down per door instead of inferred
// from call order, and this roster derives its universe from the AST.
//
// # What it reaches, stated so it cannot be read as broader
//
// UNIVERSE: every method on Engine whose name starts with `Open`, in every
// non-test file of this package — not engine.go alone, which was the
// first cut's stated residual and left the snapshot-stream, backup-snapshot
// and backfill doors ungraded.
//
// REACHABILITY IS TRANSITIVE: a door "opens a pool" when openDB (or
// database/sql's Open) is reachable from its body through package-level
// functions and Engine methods, by name; it "probes" when checkServerFlavor
// is reachable the same way. That is what grades OpenCDCReader →
// openBinlogCDCReader → openBinlogCDCReaderShared → openDB, which the
// first cut could not see. The walk is by NAME and over-approximates in
// both directions: a helper reachable on any branch counts, so a door
// whose binlog branch probes and whose VStream branch does not is graded
// as probing. That imprecision is tolerated because checkServerFlavor
// returns nil for every VStream flavor on its first line, so the
// unprobed branch is the one where the probe is a no-op. A future flavor
// for which that stops being true needs a branch-sensitive walk here.
func TestFlavorDoorRoster_EveryConnectionOpenerIsClassified(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	type fn struct {
		decl  *ast.FuncDecl
		file  string
		lines []string
	}
	pkgFuncs := map[string]*fn{} // package-level functions by name
	methods := map[string]*fn{}  // Engine methods by name
	var doors []*fn
	parsed := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, file, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		parsed++
		lines := strings.Split(string(src), "\n")
		for _, decl := range f.Decls {
			d, ok := decl.(*ast.FuncDecl)
			if !ok || d.Body == nil {
				continue
			}
			entry := &fn{decl: d, file: file, lines: lines}
			if d.Recv == nil {
				pkgFuncs[d.Name.Name] = entry
				continue
			}
			if len(d.Recv.List) == 0 || receiverTypeName(d.Recv.List[0].Type) != "Engine" {
				continue
			}
			methods[d.Name.Name] = entry
			if strings.HasPrefix(d.Name.Name, "Open") {
				doors = append(doors, entry)
			}
		}
	}
	if parsed < 50 {
		t.Fatalf("parsed only %d non-test files in the mysql package; expected far more — the glob or the cwd is wrong", parsed)
	}

	// reach walks a function's calls transitively and reports whether a
	// pool is opened and whether the probe runs anywhere along the way.
	var reach func(entry *fn, seen map[*fn]bool) (opens, probes bool)
	reach = func(entry *fn, seen map[*fn]bool) (opens, probes bool) {
		if seen[entry] {
			return false, false
		}
		seen[entry] = true
		ast.Inspect(entry.decl.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fun := call.Fun.(type) {
			case *ast.Ident:
				if fun.Name == "openDB" {
					opens = true
				}
				if callee, ok := pkgFuncs[fun.Name]; ok {
					o, p := reach(callee, seen)
					opens, probes = opens || o, probes || p
				}
			case *ast.SelectorExpr:
				switch fun.Sel.Name {
				case "checkServerFlavor":
					probes = true
				case "Open", "OpenDB":
					// database/sql opened directly, bypassing openDB.
					if x, ok := fun.X.(*ast.Ident); ok && x.Name == "sql" {
						opens = true
					}
				}
				if callee, ok := methods[fun.Sel.Name]; ok {
					o, p := reach(callee, seen)
					opens, probes = opens || o, probes || p
				}
			}
			return true
		})
		return opens, probes
	}

	sort.Slice(doors, func(i, j int) bool { return doors[i].decl.Name.Name < doors[j].decl.Name.Name })
	probedDoors := map[string]bool{}
	opening, exempt := 0, 0
	for _, door := range doors {
		name := door.decl.Name.Name
		opens, probes := reach(door, map[*fn]bool{})
		start := fset.Position(door.decl.Pos()).Line
		if !opens {
			t.Logf("%s:%d %s opens no pool (not graded)", door.file, start, name)
			continue
		}
		opening++
		if probes {
			probedDoors[name] = true
			t.Logf("%s:%d %s opens a pool and reaches checkServerFlavor", door.file, start, name)
			continue
		}
		end := fset.Position(door.decl.End()).Line
		if end > len(door.lines) {
			end = len(door.lines)
		}
		body := strings.Join(door.lines[start-1:end], "\n")
		idx := strings.Index(body, flavorDoorExemptMarker)
		if idx < 0 {
			t.Errorf("%s:%d %s opens a connection pool (directly or through a helper) but neither reaches "+
				"checkServerFlavor nor declares why not. That probe carries the MariaDB driver steer AND the "+
				"Vitess-under-vanilla REFUSAL, which is a silent-loss guard; a door without it hands those cases "+
				"to whichever door happens to open first (Bug 280). Call it, or write `%s <why>` inside this function.",
				door.file, start, name, flavorDoorExemptMarker)
			continue
		}
		if reason := strings.TrimSpace(body[idx+len(flavorDoorExemptMarker):]); len(reason) < 30 {
			t.Errorf("%s:%d %s carries %s with no real reason (%q)", door.file, start, name,
				flavorDoorExemptMarker, reason)
		}
		exempt++
		t.Logf("%s:%d %s opens a pool and is exempt with a reason", door.file, start, name)
	}

	// Anti-vacuity, three ways. The named floor is the one that matters:
	// a green cannot be bought by exempting the doors that carry the
	// pipelines' first connection and probing two that do not.
	for _, name := range flavorDoorsThatMustProbe {
		if !probedDoors[name] {
			t.Errorf("%s does not reach checkServerFlavor (or was not found as a pool-opening Engine.Open* method); "+
				"it is one of the doors Bug 280's enumeration requires to probe", name)
		}
	}
	// Count floors within ~20% of the measured truth (17 doors, 17 opening,
	// 15 probing, 2 exempt on 2026-09-15), so a scan or walk that quietly
	// halves the universe fails rather than passing on what is left.
	if len(doors) < 14 {
		t.Fatalf("found %d Open* methods on Engine across the package; 17 exist — the scan broke", len(doors))
	}
	if opening < 14 {
		t.Fatalf("only %d Open* door(s) reach openDB; 17 do — the transitive walk broke", opening)
	}
	if len(probedDoors) < 12 {
		t.Fatalf("only %d door(s) reach checkServerFlavor; 15 do — the walk stopped seeing the probe", len(probedDoors))
	}
	t.Logf("flavor doors: %d Open* methods, %d open a pool, %d probe, %d exempt with a reason",
		len(doors), opening, len(probedDoors), exempt)
}

// TestCheckServerFlavor_IsANoOpForVStreamFlavors pins the fact the two
// VStream-only doors' `flavor-door-exempt:` reasons cite — and that the
// roster's by-name walk leans on when it tolerates a probed binlog branch
// beside an unprobed VStream one. A nil *sql.DB and nil config would panic
// on the first query, so a nil return here proves the connection was never
// touched. Every VStream flavor is exercised, not one representative.
func TestCheckServerFlavor_IsANoOpForVStreamFlavors(t *testing.T) {
	t.Parallel()
	vstream := 0
	// Every recognised flavor, derived from String(): the enum is dense
	// from zero and an unrecognised value renders as "flavor(N)".
	for f := Flavor(0); !strings.HasPrefix(f.String(), "flavor("); f++ {
		if !f.usesVStream() {
			continue
		}
		vstream++
		if err := (Engine{Flavor: f}).checkServerFlavor(t.Context(), nil, nil); err != nil {
			t.Errorf("flavor %s: checkServerFlavor returned %v on a nil connection; it must return nil before touching the pool for every VStream flavor", f, err)
		}
	}
	if vstream < 2 {
		t.Fatalf("only %d VStream flavor(s) enumerated; planetscale and vitess both are — the flavor roster broke", vstream)
	}
}
