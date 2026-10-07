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

// switchSites are the only places the --exactly-once-lanes switch may be
// written, per form, keyed by file (relative to the module root). Every other
// write fails TestReplayPathsNeverFoldAMarkFence.
var switchSites = map[string][]string{
	// sync start and the fleet spec build a Streamer from the operator's flag.
	"ExactlyOnceLanes:": {
		"cmd/sluice/cli.go", "cmd/sluice/sync_run.go",
		// each lane adapter hands the applier's own switch to laneapply.Config.
		"internal/engines/mysql/change_applier_concurrent.go", "internal/engines/postgres/change_applier_concurrent.go",
	},
	// the streamer plumbs Streamer.ExactlyOnceLanes to the applier.
	"ApplyExactlyOnceLanes": {"internal/pipeline/streamer_run_phases.go"},
	// migcore's plumbing is the only caller of the applier's setter.
	"SetExactlyOnceLanes": {"internal/pipeline/migcore/apply_concurrency.go"},
}

// replayFile reports whether path (relative to the module root) is on a
// replay path: the `sync from-backup` broker, the backup package (chain
// restore, chain replay), and the change-chunk codec that decodes the
// replayed changes (blobcodec/backup_change_chunk.go, decodeChange).
func replayFile(path string) bool {
	return strings.HasPrefix(path, "internal/pipeline/broker") ||
		strings.HasPrefix(path, "internal/pipeline/backup/") ||
		path == "internal/pipeline/blobcodec/backup_change_chunk.go"
}

// TestReplayPathsNeverFoldAMarkFence holds ADR-0190 amendment D's scope
// exemption (§D.7): the `sync from-backup` broker and chain replay reach the
// lane orchestrator through migcore.ApplyApplyConcurrency, and neither can
// ever issue a MARK FENCE's fold ticket.
//
// Scope, stated because the name once read broader: this gates amendment D's
// fold only. The replay paths DO fold at their lane BARRIERS (amendment E):
// a barrier's pre-apply checkpoint rides the barrier's own transaction
// wherever the lane orchestrator runs, with no flag — on the broker it is the
// frontier token of the barrier's own transaction start (ADR-0191 §3.2),
// which TestBroker_BarrierFoldKeepsTheParentToken pins.
//
// The reason: only `sync start` (and the fleet spec) turns
// --exactly-once-lanes on. Every write of the switch across internal/ and
// cmd/ is held to switchSites, in all three forms it can take: a
// composite-literal `ExactlyOnceLanes:` field (a Streamer, a
// laneapply.Config), a call of migcore.ApplyExactlyOnceLanes, and a call of
// the applier's SetExactlyOnceLanes; and an assignment to an applier's
// `exactlyOnceLanes` field must sit inside SetExactlyOnceLanes. So the
// orchestrator's fence returns at its first line on a replay path.
//
// There used to be a second, independent reason — "replayed changes carry no
// ADR-0190 identity: no replay-path file mentions ApplyID". ADR-0191 makes it
// false on purpose: the change-chunk codec now restores the reader's identity,
// so a replayed change DOES carry one, and the fence's own guard (the switch
// above) is what keeps the fold away. What replaced that reason is a
// different property, held by TestReplayIdentityIsPassThrough: on a replay
// path an identity is only ever CARRIED, never assigned (ADR-0191 §9 P14).
//
// Reach, stated: a write of the switch through reflection, or a new field
// spelled otherwise, is outside this AST walk. The anti-vacuity floor: the
// walk must find both replay paths' calls to ApplyApplyConcurrency and every
// listed switch site.
func TestReplayPathsNeverFoldAMarkFence(t *testing.T) {
	fset := token.NewFileSet()
	found := map[string][]string{}
	var concurrency, strays []string
	for _, root := range []string{"../../internal", "../../cmd"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
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
			rel := strings.TrimPrefix(filepath.ToSlash(path), "../../")
			for _, decl := range f.Decls {
				fn, _ := decl.(*ast.FuncDecl)
				ast.Inspect(decl, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.KeyValueExpr:
						if k, ok := n.Key.(*ast.Ident); ok && k.Name == "ExactlyOnceLanes" {
							found["ExactlyOnceLanes:"] = append(found["ExactlyOnceLanes:"], rel)
						}
					case *ast.SelectorExpr:
						switch n.Sel.Name {
						case "ApplyExactlyOnceLanes", "SetExactlyOnceLanes":
							found[n.Sel.Name] = append(found[n.Sel.Name], rel)
						case "ApplyApplyConcurrency":
							concurrency = append(concurrency, rel)
						}
					case *ast.AssignStmt:
						for _, lhs := range n.Lhs {
							if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "exactlyOnceLanes" &&
								(fn == nil || fn.Name.Name != "SetExactlyOnceLanes") {
								strays = append(strays, rel)
							}
						}
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
	for form, allowed := range switchSites {
		for _, p := range found[form] {
			if !slices.Contains(allowed, p) {
				t.Errorf("%s writes the --exactly-once-lanes switch (%s) and is not one of %v: a replay path that turned "+
					"it on could fold a fence", p, form, allowed)
			}
		}
		for _, p := range allowed {
			if !slices.Contains(found[form], p) {
				t.Errorf("switchSites lists %s for %s, which no longer writes it — the walk or this gate is stale", p, form)
			}
		}
	}
	for _, p := range strays {
		t.Errorf("%s assigns an applier's exactlyOnceLanes outside SetExactlyOnceLanes", p)
	}
	if !slices.Contains(concurrency, "internal/pipeline/broker.go") || !slices.Contains(concurrency, "internal/pipeline/backup/chain_restore.go") {
		t.Fatalf("the walk found the lane wiring in %v; want broker.go and backup/chain_restore.go among them — it is not "+
			"reaching the files this gate is about", concurrency)
	}
}
