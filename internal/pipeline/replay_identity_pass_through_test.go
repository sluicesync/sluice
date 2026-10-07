// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// identityPassThroughSites are the only functions on a replay path that may
// ASSIGN an ir.ApplyID: the change-chunk codec's decode, which restores the
// identity the READER stamped and the capture recorded (ADR-0191 §3.1).
var identityPassThroughSites = map[string]bool{
	"internal/pipeline/blobcodec/backup_change_chunk.go:decodeChange": true,
	"internal/pipeline/blobcodec/backup_change_chunk.go:decodeAID":    true,
}

// TestReplayIdentityIsPassThrough is ADR-0191 §9 P14: on every replay path
// (replayFile — the `sync from-backup` broker, the backup package's chain
// restore and compaction, the change-chunk codec) an ADR-0190 apply identity
// is CARRIED, never made up. The apply marks skip a change only when a mark
// names the same (TxID, Seq), so an identity a replay path assigned itself —
// a counter, an ordinal over the decoded stream, anything not the reader's —
// could name a change differently on the next read and skip work the target
// never received. The reader assigns it (ADR-0190 §1); the capture records it;
// the decode restores it; nothing else on these paths writes one.
//
// What counts as an assignment, by AST shape: a composite-literal field
// `ApplyID: …` (an ir.Insert/Update/Delete built with one), an assignment
// whose left side selects `.ApplyID` (or a field of it), and a non-empty
// `ApplyID{…}` literal. Clearing an identity is not an assignment here: the
// replay paths that drop one (smart compaction) call ir.WithoutApplyID, which
// lives outside them, and a zero identity can never cause a skip.
//
// Reach, stated: identifiers in the files replayFile names; a replay path that
// moves to a new file must be added there. The anti-vacuity floor: every
// allowed site must be found ASSIGNING, so a walk that stopped seeing the
// decode would fail rather than pass on an empty set.
func TestReplayIdentityIsPassThrough(t *testing.T) {
	fset := token.NewFileSet()
	var strays []string
	seen := map[string]bool{}
	err := filepath.WalkDir("../../internal", func(path string, d fs.DirEntry, err error) error {
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
		rel := strings.TrimPrefix(filepath.ToSlash(path), "../../")
		if !replayFile(rel) {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range f.Decls {
			fnName := "<package scope>"
			if fn, ok := decl.(*ast.FuncDecl); ok {
				fnName = fn.Name.Name
			}
			site := rel + ":" + fnName
			ast.Inspect(decl, func(n ast.Node) bool {
				if !assignsApplyID(n) {
					return true
				}
				if identityPassThroughSites[site] {
					seen[site] = true
					return true
				}
				strays = append(strays, fmt.Sprintf("%s (%s)", site, fset.Position(n.Pos())))
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	sort.Strings(strays)
	for _, s := range strays {
		t.Errorf("%s assigns an ADR-0190 apply identity on a replay path. A replayed change carries the identity the "+
			"reader stamped and the codec restored (ADR-0191 §3.1); one a replay path makes up can name a change "+
			"differently on the next read and skip work the target never received", s)
	}
	for site := range identityPassThroughSites {
		if !seen[site] {
			t.Errorf("the walk saw no ApplyID assignment in %s, the codec's decode: either the decode stopped restoring "+
				"the identity (a chain would replay with none) or this gate stopped seeing it — fix the one that broke", site)
		}
	}
}

// assignsApplyID reports whether n writes an ApplyID (see the gate's doc).
func assignsApplyID(n ast.Node) bool {
	switch n := n.(type) {
	case *ast.KeyValueExpr:
		k, ok := n.Key.(*ast.Ident)
		return ok && k.Name == "ApplyID"
	case *ast.AssignStmt:
		for _, lhs := range n.Lhs {
			if selectsApplyID(lhs) {
				return true
			}
		}
	case *ast.CompositeLit:
		if len(n.Elts) == 0 {
			return false
		}
		switch ty := n.Type.(type) {
		case *ast.Ident:
			return ty.Name == "ApplyID"
		case *ast.SelectorExpr:
			return ty.Sel.Name == "ApplyID"
		}
	}
	return false
}

// selectsApplyID reports whether e is `x.ApplyID` or a selection below it
// (`x.ApplyID.Seq`).
func selectsApplyID(e ast.Expr) bool {
	for {
		sel, ok := e.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		if sel.Sel.Name == "ApplyID" {
			return true
		}
		e = sel.X
	}
}
