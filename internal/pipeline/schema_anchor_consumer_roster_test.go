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

// The negative claim `assertDataWindowEndPositionInvariant` now makes,
// turned into something that fails when it stops being true.
//
// That function's doc said, for the whole of its life until the 2026-08-07
// invariant sweep, that "the restore-side completeness net
// (chain_restore/broker) relies on exactly that" — on the property that a
// data-bearing window never anchors a schema snapshot at its EndPosition.
// It does not, and has not since item 60 / audit-2026-07-12: restore
// distrusts an anchor at EndPosition unconditionally and rests completeness
// on the change-chunk tail. [irbackup.Manifest.SchemaHistoryAnchors] said so
// in its own NOTE while two other comments said the opposite, which is the
// whole problem — three doc sites, one truth, nothing holding them together.
//
// The corrected comments assert a NEGATIVE: nothing on the restore side
// consumes the anchor distinction, so the writer-side assert is a belt
// rather than a load-bearing invariant. A negative is exactly the shape
// that rots invisibly — someone adds a restore-side consumer, the assert
// silently becomes load-bearing again, and three comments quietly become
// wrong in the other direction. So this roster grades every call site.
//
// # What it derives, and what it does not
//
//   - It walks non-test Go under internal/pipeline (which contains the
//     backup, chain-restore and broker paths) for calls to
//     `SchemaHistoryAnchors` and for reads of the
//     `CDCPositionCommitsAfterRows` field, and requires each site's
//     enclosing FUNCTION to be rostered with a written classification.
//   - Fail-by-default: an unrostered site fails, and a rostered site that
//     disappears fails too, so the roster cannot rot in either direction.
//   - It grades the pipeline package tree only. Both symbols live on
//     `irbackup.Manifest`, so an engine package could in principle read
//     them; none does today, and the anti-vacuity floors below would not
//     notice if one started. That is the stated residual — the restore and
//     broker paths this claim is about are all inside this tree.
//   - It grades SYNTAX, not reachability: a call inside dead code still
//     counts. That is the conservative direction for a negative claim.
func TestSchemaHistoryAnchorHasNoRestoreSideConsumer(t *testing.T) {
	sites := anchorSymbolSites(t)

	// Anti-vacuity. A walker that matched nothing would report a confident
	// green about a property it never looked for — and the two floors are
	// separate because the two symbols rot independently.
	byImportance := map[string]int{}
	for _, s := range sites {
		byImportance[s.symbol]++
	}
	if byImportance["SchemaHistoryAnchors"] < 1 {
		t.Fatalf("found no SchemaHistoryAnchors call site under internal/pipeline. The writer-side assert calls it; "+
			"a zero count means the walker is broken, not that the call vanished. Sites found overall: %d", len(sites))
	}
	if byImportance["CDCPositionCommitsAfterRows"] < 2 {
		t.Fatalf("found %d CDCPositionCommitsAfterRows site(s) under internal/pipeline; the two writers (incremental, "+
			"stream) each set it and the assert reads it, so fewer than 2 means the walker is broken",
			byImportance["CDCPositionCommitsAfterRows"])
	}

	for _, s := range sites {
		reason, ok := anchorSymbolRoster[s.fn]
		if !ok {
			t.Errorf("%s reads %s at %s, and no roster entry classifies it.\n"+
				"  If this is a WRITE-side site (stamping the flag, asserting the writer invariant), add it with that "+
				"reason and nothing else changes.\n"+
				"  If it is a RESTORE-side consumer, then the anchor distinction is load-bearing again and THREE "+
				"comments are now wrong the other way: assertDataWindowEndPositionInvariant's \"it is a BELT\" "+
				"paragraph (internal/pipeline/incremental.go), the Manifest.CDCPositionCommitsAfterRows field doc, "+
				"and Manifest.SchemaHistoryAnchors' audit-2026-07-12 NOTE. Fix them in the same commit.",
				s.fn, s.symbol, s.pos)
			continue
		}
		if strings.TrimSpace(reason) == "" {
			t.Errorf("%s is rostered with an empty reason — the reason is the only thing a future reader can check", s.fn)
		}
	}

	seen := map[string]bool{}
	for _, s := range sites {
		seen[s.fn] = true
	}
	for fn := range anchorSymbolRoster {
		if !seen[fn] {
			t.Errorf("the roster classifies %q, which no longer reads either symbol. A stale entry pre-blesses whatever "+
				"that function does next — remove it", fn)
		}
	}
}

// anchorSymbolRoster maps the enclosing function of every site that reads
// the anchor-completeness symbols to why that site is write-side.
//
// Every entry today is a WRITER. That is the claim, and it is the reason
// the corrected comments say BELT rather than RELIES-ON.
var anchorSymbolRoster = map[string]string{
	"assertDataWindowEndPositionInvariant": "the writer-side belt itself: refuses to persist a manifest whose " +
		"data-bearing window anchors a schema snapshot at EndPosition, on an engine where that cannot legitimately " +
		"happen. Reads both symbols. Backup-time, never restore-time.",
	"(*IncrementalBackup).newInProgressManifest": "STAMPS CDCPositionCommitsAfterRows from the source engine's " +
		"registered capability while building an incremental manifest. Write side.",
	"(*BackupStream).runRollover": "STAMPS CDCPositionCommitsAfterRows on a `backup stream` segment manifest at " +
		"rollover — the streaming twin of the incremental writer. Write side.",
}

type anchorSite struct {
	fn     string
	symbol string
	pos    string
}

// anchorSymbolSites walks non-test Go under internal/pipeline for calls to
// SchemaHistoryAnchors and reads of the CDCPositionCommitsAfterRows field,
// attributing each to its enclosing function or method.
func anchorSymbolSites(t *testing.T) []anchorSite {
	t.Helper()
	var out []anchorSite
	fset := token.NewFileSet()

	err := filepath.Walk(".", func(path string, info os.FileInfo, werr error) error {
		if werr != nil {
			return werr
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", path, perr)
		}
		for _, decl := range file.Decls {
			fn, isFunc := decl.(*ast.FuncDecl)
			if !isFunc || fn.Body == nil {
				continue
			}
			name := funcQualifiedName(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "SchemaHistoryAnchors", "CDCPositionCommitsAfterRows":
					out = append(out, anchorSite{
						fn:     name,
						symbol: sel.Sel.Name,
						pos:    filepath.ToSlash(path) + ":" + itoa(fset.Position(sel.Pos()).Line),
					})
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/pipeline: %v", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].pos < out[j].pos })
	return out
}

// funcQualifiedName renders a FuncDecl as `Name` or `(*Recv).Name`.
func funcQualifiedName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return fn.Name.Name
	}
	expr := fn.Recv.List[0].Type
	star := ""
	if s, ok := expr.(*ast.StarExpr); ok {
		expr = s.X
		star = "*"
	}
	id, ok := expr.(*ast.Ident)
	if !ok {
		return fn.Name.Name
	}
	return "(" + star + id.Name + ")." + fn.Name.Name
}
