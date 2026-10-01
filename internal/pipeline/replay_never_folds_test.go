// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestReplayPathsNeverFold holds ADR-0190 amendment D's scope exemption
// (§D.7): the `sync from-backup` broker and chain replay reach the lane
// orchestrator through migcore.ApplyApplyConcurrency, and neither can ever
// issue a fold ticket — for TWO independent reasons, each checked here
// because two reasons are only worth having if each holds:
//
//  1. only the streamer turns --exactly-once-lanes on: migcore.
//     ApplyExactlyOnceLanes is called from streamer_run_phases.go and from no
//     other file under internal/pipeline, so the orchestrator's fence returns
//     at its first line for them;
//  2. their changes carry no ADR-0190 identity: no file on the replay paths
//     (broker*.go, the backup package) mentions ApplyID, so ApplyMarkTx
//     answers "" regardless.
//
// The anti-vacuity floor: the walk must find both replay paths' calls to
// ApplyApplyConcurrency, so a walk that misses the replay files cannot pass.
func TestReplayPathsNeverFold(t *testing.T) {
	fset := token.NewFileSet()
	var exactlyOnce, concurrency, identity []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		path = filepath.ToSlash(path)
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.SelectorExpr:
				switch n.Sel.Name {
				case "ApplyExactlyOnceLanes":
					exactlyOnce = append(exactlyOnce, path)
				case "ApplyApplyConcurrency":
					concurrency = append(concurrency, path)
				}
			case *ast.Ident: // a field key, a selector's field, a type: any mention
				if n.Name == "ApplyID" {
					identity = append(identity, path)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/pipeline: %v", err)
	}
	for _, p := range exactlyOnce {
		if p != "streamer_run_phases.go" {
			t.Errorf("%s turns --exactly-once-lanes on; only the streamer may (a replay path that did could fold a fence)", p)
		}
	}
	if !slices.Contains(exactlyOnce, "streamer_run_phases.go") {
		t.Error("streamer_run_phases.go no longer calls migcore.ApplyExactlyOnceLanes — the walk or this gate is stale")
	}
	isReplay := func(p string) bool { return strings.HasPrefix(p, "broker") || strings.HasPrefix(p, "backup/") }
	for _, p := range identity {
		if isReplay(p) {
			t.Errorf("%s, on a replay path, mentions ApplyID: replayed changes must carry no ADR-0190 identity", p)
		}
	}
	var replays []string
	for _, p := range concurrency {
		if isReplay(p) {
			replays = append(replays, p)
		}
	}
	slices.Sort(replays)
	if !slices.Contains(replays, "broker.go") || !slices.Contains(replays, "backup/chain_restore.go") {
		t.Fatalf("the walk found the replay paths' lane wiring in %v; want broker.go and backup/chain_restore.go — it is not "+
			"reaching the files this gate is about", replays)
	}
}
