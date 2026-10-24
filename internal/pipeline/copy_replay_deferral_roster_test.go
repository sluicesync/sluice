// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// copyReplayDeferralExempt names, by "file:function", the production callers
// of the table-less ir.Engine.OpenSnapshotStream that do NOT defer in-copy
// replays, each with the reason refusing is right there.
var copyReplayDeferralExempt = map[string]string{
	"streamer_coldstart.go:openSnapshotStreamScoped": "a cold start's change stream starts at the copy's own " +
		"position, after anything replayed inside the copy, so it must never defer: a replayed change it cannot " +
		"apply has to refuse (VSTREAM-COPY-ROW-CHANGE-REFUSED). Reached table-less only by an empty table list.",
}

// TestCopyReplayDeferralRoster holds every production caller of the
// table-less snapshot opener, ir.Engine.OpenSnapshotStream, to a decision.
// On a VStream source that opener is one keyspace-wide copy stream, the only
// stream shape whose in-copy replay is deterministic (vttablet's catch-up
// before each later table), so the copy refuses such a replay terminally
// unless its caller defers it to a later change stream
// (ir.CopyReplayDeferrer). Each caller must either call
// DeferCopyReplaysToStream in the same function (the drained schema
// add-table) or be listed in copyReplayDeferralExempt with the reason a
// refusal is correct there. This backs the claim in
// cdc_vstream_shard_copy_boundary.go about which paths can reach the
// terminal verdict: a new keyspace-wide caller fails here until it decides.
//
// Scope, stated so the gate is not read as broader: production .go files of
// internal/pipeline (all subpackages) and cmd. Engine packages are not
// callers (a wrapper engine delegating OpenSnapshotStream is not a
// consumer). The table-scoped openers (OpenSnapshotStreamForTables and its
// filtered twin) open single-table VStream copies, whose refusal is
// retriable, so they are out of scope by construction.
func TestCopyReplayDeferralRoster(t *testing.T) {
	type site struct{ key, pos string }
	var callers []site
	defers := map[string]bool{}
	fset := token.NewFileSet()
	for _, root := range []string{".", filepath.Join("..", "..", "cmd")} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				key := filepath.Base(path) + ":" + fn.Name.Name
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					switch sel.Sel.Name {
					case "OpenSnapshotStream":
						callers = append(callers, site{key: key, pos: fset.Position(call.Pos()).String()})
					case "DeferCopyReplaysToStream":
						defers[key] = true
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}

	// Anti-vacuity: today there are exactly two callers (the drained
	// add-table and the cold start's table-less fallback). Fewer means the
	// walk or the matcher broke and the gate would pass on nothing.
	if len(callers) < 2 {
		t.Fatalf("found %d production caller(s) of OpenSnapshotStream; want at least 2 (add_table.go, streamer_coldstart.go): "+
			"the roster walk is broken", len(callers))
	}
	seenExempt := map[string]bool{}
	var undecided []string
	for _, c := range callers {
		if defers[c.key] {
			continue
		}
		if _, ok := copyReplayDeferralExempt[c.key]; ok {
			seenExempt[c.key] = true
			continue
		}
		undecided = append(undecided, c.key+" ("+c.pos+")")
	}
	sort.Strings(undecided)
	for _, u := range undecided {
		t.Errorf("%s opens a table-less snapshot (a keyspace-wide VStream copy) and neither calls DeferCopyReplaysToStream "+
			"nor is in copyReplayDeferralExempt: decide whether a later change stream replays from before the copy (defer) "+
			"or a replayed change must refuse (exempt, with the reason)", u)
	}
	for key := range copyReplayDeferralExempt {
		if !seenExempt[key] {
			t.Errorf("copyReplayDeferralExempt names %s, which no longer calls OpenSnapshotStream without deferring: remove the stale entry", key)
		}
	}
	if !defers["add_table.go:openSnapshotForAdd"] {
		t.Errorf("add_table.go:openSnapshotForAdd no longer calls DeferCopyReplaysToStream: the drained add-table would fail on " +
			"any delete during its copy (VSTREAM-COPY-ROW-CHANGE-REFUSED)")
	}
}
