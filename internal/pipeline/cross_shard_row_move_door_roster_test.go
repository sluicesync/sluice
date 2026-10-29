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

// The cross-shard row-move door (CROSS-SHARD-VINDEX-NOT-IN-PK) is called from
// two entry points. This roster is the enumeration the door rule asks for:
// every orchestrator entry point — every `Run(ctx)` method in this package
// and internal/pipeline/backup, derived from the AST so a new one appears
// here unclassified — carries a verdict, and the verdict is checked against
// the code: a REACHED entry point's body must call preflightCrossShardRowMoves,
// and no other entry point's may (a call that appears without a verdict is a
// classification that went stale).
//
// SCOPE, stated narrowly: it proves the call is in the entry point's own
// body, not that it runs on every branch of it, and it enumerates entry
// points, not the sub-paths inside one (Streamer.Run's cold start, warm
// resume and retry attempts are all below the one call, which is why the
// call is in Run and not in a branch).
type rowMoveDoorClass int

const (
	// rowMoveDoorReached: the entry point merges a multi-shard source into a
	// target and calls the door.
	rowMoveDoorReached rowMoveDoorClass = iota
	// rowMoveDoorExempt: the entry point cannot lose a row to the cross-shard
	// ordering hazard; the reason says why.
	rowMoveDoorExempt
	// rowMoveDoorFiledGap: the entry point IS exposed and is not covered;
	// the reason names the tracker entry. Honest, not hidden.
	rowMoveDoorFiledGap
)

type rowMoveDoorVerdict struct {
	class  rowMoveDoorClass
	reason string
}

const rowMoveBackupLaneGap = "filed as CROSS-SHARD-BACKUP-LANE (docs/dev/audit-backlog.md): a backup chain captured from a " +
	"multi-shard VStream source records the shards' merged change log in arrival order and is replayed in that " +
	"order, with neither Bug 152's collision door nor this one on the lane; not closed here because the backup " +
	"commands carry no --allow-cross-shard-merge to key a door on, and adding a refusal there is its own guard change"

var crossShardRowMoveDoorRoster = map[string]rowMoveDoorVerdict{
	"(*Streamer).Run": {rowMoveDoorReached, "every sync start and sync run leg, before the cold-start/warm-resume dispatch"},
	"(*AddTable).Run": {rowMoveDoorReached, "add-table merges every shard of a multi-shard source into its one target table"},

	"(*Migrator).Run": {rowMoveDoorExempt, "migrate is a one-shot copy with no change stream, so no change from one shard is " +
		"ever applied against another shard's; a row moved WHILE the copy reads is the documented non-snapshot posture of " +
		"a copy of a live source (a keyset read under concurrent writes), which --inject-shard-column does not make exact " +
		"either (the old shard read after the DELETE and the new one before the INSERT see the row on neither). Bug 152's " +
		"door governs its at-rest merge"},
	"(*Supervisor).Run": {rowMoveDoorExempt, "runs SyncRunners; the production runner is *Streamer, whose Run calls the door"},
	"(*Backfiller).Run": {rowMoveDoorExempt, "updates columns of a table in place on one database; merges no shards"},
	"(*Cutover).Run":    {rowMoveDoorExempt, "primes target sequences; writes no rows"},
	"(*Differ).Run":     {rowMoveDoorExempt, "read-only schema diff"},
	"(*Previewer).Run":  {rowMoveDoorExempt, "read-only DDL preview"},
	"(*Verifier).Run":   {rowMoveDoorExempt, "read-only verification"},
	"(*ParquetExport).Run": {rowMoveDoorExempt, "writes a backup's rows to parquet files; no keyed target applies a change " +
		"stream, so a reordered pair is two rows in a file, not a lost one (the capture is CROSS-SHARD-BACKUP-LANE's)"},

	"(*BackupStream).Run":      {rowMoveDoorFiledGap, rowMoveBackupLaneGap},
	"(*IncrementalBackup).Run": {rowMoveDoorFiledGap, rowMoveBackupLaneGap},
	"(*Backup).Run":            {rowMoveDoorFiledGap, rowMoveBackupLaneGap},
	"(*Restore).Run":           {rowMoveDoorFiledGap, rowMoveBackupLaneGap},
	"(*ChainRestore).Run":      {rowMoveDoorFiledGap, rowMoveBackupLaneGap},
	"(*SyncFromBackup).Run":    {rowMoveDoorFiledGap, rowMoveBackupLaneGap},
}

func TestCrossShardRowMoveDoorRoster_EveryMergeEntryPoint(t *testing.T) {
	entries := discoverRunEntryPoints(t, []string{".", "backup"})

	// Anti-vacuity: the walk must find the universe, not an empty one a
	// broken matcher would pass on.
	if len(entries) < 16 {
		t.Fatalf("anti-vacuity: found %d Run(ctx) entry points, want >= 16 — the AST matcher is likely broken", len(entries))
	}
	reached := 0
	for name, callsDoor := range entries {
		v, ok := crossShardRowMoveDoorRoster[name]
		if !ok {
			t.Errorf("entry point %s has no verdict in crossShardRowMoveDoorRoster: does it merge a multi-shard source "+
				"into a keyed target from a change stream? Reach the door, or exempt it with the reason", name)
			continue
		}
		if strings.TrimSpace(v.reason) == "" {
			t.Errorf("%s: verdict carries no reason", name)
		}
		switch {
		case v.class == rowMoveDoorReached && !callsDoor:
			t.Errorf("%s is classified REACHED but its body does not call preflightCrossShardRowMoves", name)
		case v.class != rowMoveDoorReached && callsDoor:
			t.Errorf("%s calls preflightCrossShardRowMoves but is not classified REACHED; update its verdict", name)
		}
		if v.class == rowMoveDoorReached {
			reached++
		}
	}
	if reached < 2 {
		t.Errorf("anti-vacuity: %d REACHED entry points, want >= 2 (Streamer.Run, AddTable.Run)", reached)
	}
	var stale []string
	for name := range crossShardRowMoveDoorRoster {
		if _, ok := entries[name]; !ok {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	for _, name := range stale {
		t.Errorf("stale verdict %s: no such Run(ctx) entry point", name)
	}
}

// discoverRunEntryPoints returns every method named Run whose first
// parameter is a context.Context, in the non-test files of dirs, mapped to
// whether its body calls something named preflightCrossShardRowMoves.
func discoverRunEntryPoints(t *testing.T, dirs []string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	fset := token.NewFileSet()
	for _, dir := range dirs {
		files, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read dir %q: %v", dir, err)
		}
		for _, e := range files {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", name, err)
			}
			for _, decl := range f.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Recv == nil || fd.Name.Name != "Run" || fd.Body == nil || !firstParamIsContext(fd) {
					continue
				}
				calls := false
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					switch fn := call.Fun.(type) {
					case *ast.SelectorExpr:
						calls = calls || fn.Sel.Name == "preflightCrossShardRowMoves"
					case *ast.Ident:
						calls = calls || fn.Name == "preflightCrossShardRowMoves"
					}
					return true
				})
				out[funcDeclName(fd)] = calls
			}
		}
	}
	return out
}

func firstParamIsContext(fd *ast.FuncDecl) bool {
	if fd.Type.Params == nil || len(fd.Type.Params.List) == 0 {
		return false
	}
	sel, ok := fd.Type.Params.List[0].Type.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "context" && sel.Sel.Name == "Context"
}
