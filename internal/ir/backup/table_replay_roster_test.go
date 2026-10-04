// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

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

// TestTableReplayIdempotentCallerRoster is the sibling-sweep for the F-E1
// retry-gate defect, made mechanical. TableReplayIdempotent judges the
// RECORDED table only; the engines' in-run retry gates used it alone to
// license a re-send onto a TARGET and duplicated rows into a surrogate-keyed
// target. Every production caller must be either the combined predicate
// itself or a caller with a written reason it has no target to judge.
//
// The roster is derived from the source tree (every non-test .go file under
// internal/ and cmd/), so a new caller fails here until it is classified.
// Anti-vacuity: every classified caller must still be found.
func TestTableReplayIdempotentCallerRoster(t *testing.T) {
	classified := map[string]string{
		"internal/ir/backup/replay_judge.go:JudgeReplayKey":                        "the combined predicate (recorded half)",
		"internal/ir/backup/replay_judge.go:Judge":                                 "ReplayKeyCache.Judge, the combined predicate memoised (recorded half)",
		"internal/pipeline/backup/backup.go:refuseKeylessRestreamOnAnchoredResume": "backup time: no target exists to judge; the overlap it guards reaches a target only through restore/broker, whose doors judge it (gap filed: F-E1-ROTATED-SEGMENT-OVERLAP)",
	}
	root := filepath.Join("..", "..", "..")
	found := map[string]bool{}
	var unclassified []string
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, perr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if perr != nil {
				return perr
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok || !callsTableReplayIdempotent(call.Fun) {
						return true
					}
					site := rel + ":" + fn.Name.Name
					if _, ok := classified[site]; ok {
						found[site] = true
					} else {
						unclassified = append(unclassified, site)
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(unclassified)
	for _, site := range unclassified {
		t.Errorf("%s calls TableReplayIdempotent, which judges only the RECORDED table. If it gates a re-write onto a "+
			"target, call JudgeReplayKey (or a ReplayKeyCache) instead; if it has no target, classify it here with the reason", site)
	}
	for site := range classified {
		if !found[site] {
			t.Errorf("classified caller %s was not found: the walk is broken or the roster is stale", site)
		}
	}
}

// callsTableReplayIdempotent matches the bare (same-package) and the
// package-qualified spelling of a call to TableReplayIdempotent.
func callsTableReplayIdempotent(fun ast.Expr) bool {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name == "TableReplayIdempotent"
	case *ast.SelectorExpr:
		return f.Sel.Name == "TableReplayIdempotent"
	}
	return false
}
