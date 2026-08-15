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

// The sharded-target write door (H-2) lives at the MySQL engine's create
// chokepoint — SchemaWriter.CreateTablesWithoutConstraints refuses a NEW
// vindex-less table on a sharded Vitess/PlanetScale keyspace, and lets a
// pre-existing (operator-vindexed) table through — plus the state-store arm
// at OpenMigrationStateStore for migrate's phase-1.75 bootstrap.
//
// It deliberately does NOT live at OpenSchemaWriter, because that open is
// reached by many TABLE-FREE pipeline paths (the sync schema-forward writer,
// cutover, shard-consolidation, `schema diff`, `schema preview`) that would be
// over-refused by an open-time door — the v0.126.0 regression this fixed.
//
// That safety therefore rests on a class invariant: EVERY pipeline path that
// creates tables on the target does so through the one guarded method
// (CreateTablesWithoutConstraints), and every OpenSchemaWriter call that is
// NOT followed by table creation is genuinely table-free. This gate enumerates
// both call-site families from the AST (deriving the universe, not a
// hand-maintained list) and fails the build when a NEW call site appears
// unclassified — the permanent guard against the next site silently reopening
// the over-refusal (an open-only site mis-built to create) or the under-refusal
// (a create site that bypasses the guarded method).
//
// The classification is per (file, enclosing-func, symbol). A rename or a new
// call site changes the key and fails here, forcing a fresh verdict.

// doorClass is the sharded-target verdict for one call site.
type doorClass int

const (
	// classCreates: the site creates tables on the target (it calls
	// CreateTablesWithoutConstraints, or it opens a writer whose lifetime
	// leads to that call). Sharded-safety comes from the guard INSIDE
	// CreateTablesWithoutConstraints, which covers every such site uniformly.
	classCreates doorClass = iota
	// classOpenOnly: the site opens a SchemaWriter but never creates tables
	// (forward-DDL / sequence-priming / preview / diff). It is correctly
	// UN-doored — an open-time refusal here is the H-2 over-refusal bug.
	classOpenOnly
)

// shardedTargetDoorRoster classifies every pipeline call site of
// OpenSchemaWriter and CreateTablesWithoutConstraints. Key:
// "<relpath>::<enclosingFunc>::<symbol>". Update this map when the gate
// reports an unclassified site — and state the reason in the trailing comment,
// because the reason is the load-bearing part (it is the human verdict the AST
// cannot make).
var shardedTargetDoorRoster = map[string]doorClass{
	// --- OpenSchemaWriter, table-FREE (must NOT be doored at open) ---
	"cutover.go::(*Cutover).Run::OpenSchemaWriter":                                           classOpenOnly, // opens for the SequencePrimer (AUTO_INCREMENT/identity priming); no CREATE TABLE.
	"diff.go::(*Differ).openTargetSchemaWriter::OpenSchemaWriter":                            classOpenOnly, // `schema diff`: side-effect-free PreviewDDL, no exec.
	"preview.go::(*Previewer).Run::OpenSchemaWriter":                                         classOpenOnly, // `schema preview`: PreviewDDL only, no exec.
	"schema_forward_engage.go::(*Streamer).engageAddColumnForward::OpenSchemaWriter":         classOpenOnly, // sync schema-forward: opens to forward ALTERs (ShapeDeltaApplier); never creates tables. THE H-2 regression case.
	"shard_consolidation_engage.go::(*Streamer).engageShardCoordination::OpenSchemaWriter":   classOpenOnly, // opens for the per-shape DDL applier (ShapeDeltaApplier); no CREATE TABLE.
	"migrate_multidb.go::(*Migrator).applyDeferredConstraints::OpenSchemaWriter":             classOpenOnly, // deferred cross-database FK pass: CreateConstraints only, tables already created per-database.
	"backup/chain_restore.go::(*ChainRestore).reprimeStandaloneSequences::OpenSchemaWriter":  classOpenOnly, // opens for the sequence reprimer; no CREATE TABLE.
	"backup/chain_restore.go::(*ChainRestore).syncIdentitySequencesAtTail::OpenSchemaWriter": classOpenOnly, // opens for SyncIdentitySequences at the chain tail; no CREATE TABLE.

	// --- OpenSchemaWriter, followed by table creation (guarded by the
	//     CreateTablesWithoutConstraints door it leads to) ---
	"add_table.go::(*AddTable).Run::OpenSchemaWriter":                                 classCreates, // add-table opens then calls CreateTablesWithoutConstraints.
	"broker.go::(*SyncFromBackup).applySchemaDeltas::OpenSchemaWriter":                classCreates, // broker reset opens then CreateTablesWithoutConstraints.
	"migrate.go::(*Migrator).runSingleDatabase::OpenSchemaWriter":                     classCreates, // migrate's single-database schema-apply reaches CreateTablesWithoutConstraints in runBulkCopy*.
	"streamer_coldstart.go::(*Streamer).coldStartOpenTargetWriters::OpenSchemaWriter": classCreates, // sync cold-start creates tables.
	"streamer_multidb.go::(*Streamer).coldStartCopyOneDatabase::OpenSchemaWriter":     classCreates, // multi-database sync cold-start creates tables.
	"backup/restore.go::(*Restore).Run::OpenSchemaWriter":                             classCreates, // restore opens then CreateTablesWithoutConstraints.
	"backup/chain_restore.go::(*ChainRestore).applySchemaDeltas::OpenSchemaWriter":    classCreates, // chain restore's schema-delta apply calls CreateTablesWithoutConstraints (for a table added mid-chain).

	// --- CreateTablesWithoutConstraints (every call is a create; the door
	//     lives INSIDE this method, so all are covered uniformly) ---
	"add_table.go::(*AddTable).Run::CreateTablesWithoutConstraints":                              classCreates,
	"broker.go::(*SyncFromBackup).applySchemaDeltas::CreateTablesWithoutConstraints":             classCreates,
	"migrate.go::runBulkCopyWithOpts::CreateTablesWithoutConstraints":                            classCreates,
	"migrate.go::runBulkCopyForAddTable::CreateTablesWithoutConstraints":                         classCreates,
	"migrate.go::runBulkCopyPhases::CreateTablesWithoutConstraints":                              classCreates,
	"backup/restore.go::(*Restore).Run::CreateTablesWithoutConstraints":                          classCreates,
	"backup/chain_restore.go::(*ChainRestore).applySchemaDeltas::CreateTablesWithoutConstraints": classCreates,
}

// shardedTargetDoorScanDirs are the pipeline directories whose non-test Go
// files carry the enumerated call sites. Relative to the pipeline package dir.
var shardedTargetDoorScanDirs = []string{".", "backup"}

func TestShardedTargetDoorRoster_EveryCallSiteClassified(t *testing.T) {
	sites := discoverShardedTargetDoorSites(t)

	// Anti-vacuity floor: the AST walk must find a substantial universe, or a
	// broken matcher would pass by finding nothing. These counts are the
	// current known sites; a genuine addition raises them (and appears below
	// as "unclassified" first), a matcher regression drops them.
	var open, create int
	for _, s := range sites {
		switch {
		case strings.HasSuffix(s.key, "::OpenSchemaWriter"):
			open++
		case strings.HasSuffix(s.key, "::CreateTablesWithoutConstraints"):
			create++
		}
	}
	if open < 12 {
		t.Fatalf("anti-vacuity: expected >=12 OpenSchemaWriter call sites, found %d — the AST matcher is likely broken", open)
	}
	if create < 5 {
		t.Fatalf("anti-vacuity: expected >=5 CreateTablesWithoutConstraints call sites, found %d — the AST matcher is likely broken", create)
	}

	var unclassified []string
	for _, s := range sites {
		if _, ok := shardedTargetDoorRoster[s.key]; !ok {
			unclassified = append(unclassified, s.key+"  ("+s.pos+")")
		}
	}
	if len(unclassified) > 0 {
		sort.Strings(unclassified)
		t.Fatalf("new/renamed sharded-target-door call site(s) not classified in shardedTargetDoorRoster:\n  %s\n\n"+
			"Classify each in internal/pipeline/sharded_target_door_roster_test.go with a REASON:\n"+
			"  classCreates  — the site creates tables (guarded by CreateTablesWithoutConstraints's H-2 door)\n"+
			"  classOpenOnly — opens a writer but never creates tables (must stay UN-doored at open)\n"+
			"and, for a classCreates OpenSchemaWriter site, confirm it reaches CreateTablesWithoutConstraints "+
			"rather than a bespoke CREATE path that would bypass the door.",
			strings.Join(unclassified, "\n  "))
	}

	// The reverse guard: a roster entry whose site vanished (dead classification)
	// hides drift. Every classified key must still be a live site.
	live := make(map[string]struct{}, len(sites))
	for _, s := range sites {
		live[s.key] = struct{}{}
	}
	for key := range shardedTargetDoorRoster {
		if _, ok := live[key]; !ok {
			t.Errorf("stale roster entry %q: no such call site in the pipeline AST — remove it or fix the key", key)
		}
	}
}

type shardedDoorSite struct {
	key string
	pos string
}

// discoverShardedTargetDoorSites walks the pipeline (+backup) non-test Go
// files and returns every call site of OpenSchemaWriter /
// CreateTablesWithoutConstraints, keyed by "<relpath>::<func>::<symbol>".
func discoverShardedTargetDoorSites(t *testing.T) []shardedDoorSite {
	t.Helper()
	const (
		symOpen   = "OpenSchemaWriter"
		symCreate = "CreateTablesWithoutConstraints"
	)
	wanted := map[string]struct{}{symOpen: {}, symCreate: {}}

	seen := make(map[string]shardedDoorSite)
	fset := token.NewFileSet()
	for _, dir := range shardedTargetDoorScanDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read dir %q: %v", dir, err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			rel := name
			if dir != "." {
				rel = filepath.ToSlash(filepath.Join(dir, name))
			}
			path := filepath.Join(dir, name)
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				t.Fatalf("parse %q: %v", path, err)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				fd, ok := n.(*ast.FuncDecl)
				if !ok {
					return true
				}
				funcName := funcDeclName(fd)
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					if _, ok := wanted[sel.Sel.Name]; !ok {
						return true
					}
					key := rel + "::" + funcName + "::" + sel.Sel.Name
					if _, dup := seen[key]; !dup {
						seen[key] = shardedDoorSite{key: key, pos: fset.Position(call.Pos()).String()}
					}
					return true
				})
				return false // don't descend into nested func literals as separate decls; the inner Inspect already covered the body
			})
		}
	}
	out := make([]shardedDoorSite, 0, len(seen))
	for _, s := range seen {
		out = append(out, s)
	}
	return out
}

// funcDeclName renders a stable name for a FuncDecl: "(*Recv).Name" for a
// method, "Name" for a plain func.
func funcDeclName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	return "(" + exprString(fd.Recv.List[0].Type) + ")." + fd.Name.Name
}

// exprString renders a receiver type expression as "*T" / "T".
func exprString(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return "*" + exprString(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr: // generic receiver T[P]
		return exprString(t.X)
	case *ast.SelectorExpr:
		return t.Sel.Name
	default:
		return "?"
	}
}
