// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// codecArgProvenance is how a chunk-reader call site got its codec argument.
type codecArgProvenance string

const (
	// threadedFromTheRecord — the argument is a variable or field carrying the
	// codec RECORDED for this chunk's segment in lineage.json.
	threadedFromTheRecord codecArgProvenance = "recorded"
	// sniffedWithAReason — the argument came from inspecting the bytes. Legal
	// at exactly one moment (record re-creation) and the reason must say so.
	sniffedWithAReason codecArgProvenance = "sniffed"
)

type codecCallSite struct {
	provenance codecArgProvenance
	reason     string
}

// codecReaderRoster is fail-by-default over every production call to
// blobcodec.NewChunkReader / NewChangeChunkReader in the whole tree. Keys are
// "<pkgdir>/<file>:<the codec argument as written>" so an entry cannot
// silently widen to a different site or a different argument.
var codecReaderRoster = map[string]codecCallSite{
	"backup/chain_compact_smart.go:codec": {
		threadedFromTheRecord,
		"smart compaction resolves the codec per source LINK before opening its change chunks.",
	},
	"backup/chain_restore.go:codec": {
		threadedFromTheRecord,
		"chain restore resolves the codec per link from the lineage record it walked.",
	},
	"backup/export_parquet.go:codec": {
		threadedFromTheRecord,
		"parquet export threads the resolved segment codec through with the CEK and the AAD.",
	},
	"backup/restore.go:r.segCodec": {
		threadedFromTheRecord,
		"the restore's segment codec, resolved once from lineage.json when the segment was opened.",
	},
	"backup/verify_read_depth.go:t.codec": {
		threadedFromTheRecord,
		"the verify task carries the segment's recorded codec alongside its CEK and AAD; both reader " +
			"kinds (chunk + change chunk) read the same field.",
	},
	"pipeline/broker.go:codec": {
		threadedFromTheRecord,
		"the broker resolves the owning manifest's segment codec before replaying its change chunks.",
	},
}

// TestChunkReaderCodecsAreThreadedFromTheRecord is the caller roster the
// "recorded, never sniffed" claim needed (2026-08-08 invariant sweep).
//
// # The claim, and what was actually holding it
//
// blobcodec's package doc says the per-segment codec "is metadata, read from
// the segment, full stop", and explicitly names the corruption it is avoiding:
// a `none` chunk whose first byte resembles a gzip magic prefix, mis-decoded by
// a reader that guessed. It carves out ONE exception — record re-creation, when
// `backup verify --rebuild-catalog` has to re-derive a lost lineage.json from
// chunk magic bytes.
//
// That is a claim about CALL SITES: every per-chunk decode threads a codec
// through from segment metadata, and only the record's rebirth sniffs. Nothing
// checked it. The audit's proposed remedy was "a caller roster + a
// decode-mismatch pin"; the backlog then recorded the entry as substantially
// overtaken by the item-135 provenance work, which is not true of this half —
// TestHexDecodeSitesAreProvenanceDecided walks internal/engines/** and
// explicitly excludes internal/pipeline, and it is about hex-vs-bytes value
// rendering, not compression. What item 135 supplied here was the METHOD (a
// declared lane plus a fail-by-default AST roster), not the coverage.
//
// So: this file is the roster, and
// blobcodec.TestChunkReader_DeclaredCodecMismatchIsLoud is the mismatch pin.
//
// # What it grades
//
// Every call to the two chunk-reader constructors in every non-test file under
// internal/, with the codec argument recorded as SOURCE TEXT. A new call site
// fails until someone classifies it; changing an existing site's argument
// expression also fails, because the argument text is part of the key. That is
// deliberate — "the codec came from the record" is a property of the
// expression, and an argument silently swapped from a threaded field to a
// literal is exactly the regression this exists to catch.
//
// Scope, stated rather than implied: it grades PROVENANCE by classification,
// not by dataflow. It cannot prove the variable named `codec` really was read
// from lineage.json — only that a human said so and that the expression has not
// changed since. A literal codec constant at a call site is refused outright
// (see the literal check below), which is the one shape it can decide alone.
func TestChunkReaderCodecsAreThreadedFromTheRecord(t *testing.T) {
	found := findChunkReaderCallSites(t)

	const siteFloor = 6
	if len(found) < siteFloor {
		t.Fatalf("discovered only %d chunk-reader call sites (floor %d) — the walker is broken, not the "+
			"tree; the roster below would be graded against nothing", len(found), siteFloor)
	}

	keys := make([]string, 0, len(found))
	for k := range found {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		site := found[key]
		entry, listed := codecReaderRoster[key]
		if !listed {
			t.Errorf("%s (%s) opens a chunk reader and is NOT on the roster.\n"+
				"blobcodec's contract is that the codec is READ FROM THE SEGMENT RECORD and never inferred "+
				"from the chunk bytes — a `none` chunk whose first byte looks like gzip magic is the loss "+
				"shape. Add a codecReaderRoster entry naming where this site's codec came from: %q, or %q "+
				"with a reason that says why this is record-(re)creation and not a per-chunk decode.",
				key, site, threadedFromTheRecord, sniffedWithAReason)
			continue
		}
		if entry.provenance != threadedFromTheRecord && entry.provenance != sniffedWithAReason {
			t.Errorf("%s: provenance %q is not one of %q / %q", key, entry.provenance, threadedFromTheRecord, sniffedWithAReason)
		}
		if strings.TrimSpace(entry.reason) == "" {
			t.Errorf("%s: roster entry has no reason; the reason IS the gate", key)
		}
	}

	for key := range codecReaderRoster {
		if _, ok := found[key]; !ok {
			t.Errorf("roster lists %s but no such call site exists any more (the file, or the codec "+
				"argument's spelling, changed) — re-derive it. A stale blessing covers less than its name implies.", key)
		}
	}
}

// findChunkReaderCallSites walks every non-test .go file under internal/ and
// returns "<pkgdir>/<file>:<codec-arg-source>" -> "file:line".
//
// The codec is the 4th argument of both constructors:
//
//	NewChunkReader(src, sha, cek, codec, aad)
//	NewChangeChunkReader(src, sha, cek, codec, aad)
//
// A call site passing a codec LITERAL (blobcodec.CodecGzip, "gzip", …) is
// refused here rather than rostered: a constant cannot have come from the
// record, so no reason could make it correct.
func findChunkReaderCallSites(t *testing.T) map[string]string {
	t.Helper()

	root := internalRootForCodecRoster(t)
	fset := token.NewFileSet()
	out := map[string]string{}

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		src, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		pkgDir := filepath.Base(filepath.Dir(path))
		base := filepath.Base(path)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if sel.Sel.Name != "NewChunkReader" && sel.Sel.Name != "NewChangeChunkReader" {
				return true
			}
			if len(call.Args) < 4 {
				return true
			}
			arg := call.Args[3]
			text := strings.TrimSpace(string(src[fset.Position(arg.Pos()).Offset:fset.Position(arg.End()).Offset]))
			pos := fmt.Sprintf("%s:%d", base, fset.Position(call.Pos()).Line)
			if isCodecLiteral(arg) {
				t.Errorf("%s passes a codec LITERAL (%s) to %s.\n"+
					"A constant cannot have come from lineage.json, so this site decodes chunks under a codec "+
					"nobody read from the record — the one thing blobcodec's contract forbids. Thread the "+
					"segment's recorded codec through instead.", pos, text, sel.Sel.Name)
				return true
			}
			out[pkgDir+"/"+base+":"+text] = pos
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return out
}

// isCodecLiteral reports whether the argument is a compile-time constant: a
// string literal, or a bare/qualified identifier spelled Codec*.
func isCodecLiteral(arg ast.Expr) bool {
	switch v := arg.(type) {
	case *ast.BasicLit:
		return v.Kind == token.STRING
	case *ast.Ident:
		return strings.HasPrefix(v.Name, "Codec")
	case *ast.SelectorExpr:
		return strings.HasPrefix(v.Sel.Name, "Codec")
	}
	return false
}

// internalRootForCodecRoster resolves internal/ from this package's directory
// so the walk covers the whole tree, not just internal/pipeline/backup — the
// roster's value is the site in broker.go that this package cannot see.
func internalRootForCodecRoster(t *testing.T) string {
	t.Helper()
	root := filepath.Join("..", "..")
	if _, err := os.Stat(filepath.Join(root, "pipeline")); err != nil {
		t.Fatalf("cannot resolve internal/ from this package (looked at %s): %v — the walk would silently "+
			"grade a subset of the tree", root, err)
	}
	return root
}
