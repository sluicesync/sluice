// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 142: the per-chunk BYTE ceiling, as a roster over every lane
// that writes a chunk rather than a pin on one of them.
//
// The history is the entire justification. Item 116 P3 added a byte ceiling
// to the DATA lane and its enumeration stopped there. Audit C-3 added one to
// the CHANGE lane and its enumeration stopped at `incremental.go` — while
// `stream.go`'s rollover, which writes most change chunks in practice, kept
// rolling on event count alone. Two fixes, two enumerations, two misses, and
// nothing in the tree compared the lanes. That is what this file is: the
// comparison, derived from the code so a lane written tomorrow is covered
// tomorrow.
//
// WHAT THIS GATE REACHES, stated so the name cannot be read as broader than
// the truth: every PRODUCTION construction of a `blobcodec` chunk writer
// (`NewChunkWriter` for data chunks, `NewChangeChunkWriter` for change
// chunks) under `internal/pipeline`. Discovery is by constructor call site,
// so a new lane in a new file is found; it does NOT reach writers built
// outside that tree, or a lane that buffers without a blobcodec writer at
// all. `RolloverMaxBytes` is deliberately NOT accepted as the ceiling: it
// bounds a whole rollover in COMPRESSED bytes at a transaction boundary,
// which is a different quantity at a different granularity.

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// chunkWriterCtors are the blobcodec constructors that open a chunk. Each
// must still be a declared function in blobcodec, or the walker is stale and
// would report an empty roster.
var chunkWriterCtors = map[string]string{
	"NewChunkWriter":       "data",
	"NewChangeChunkWriter": "change",
}

// byteCeilingMarker is the accessor a lane must compare against to bound a
// chunk by size. countCeilingMarkers are the per-family event counters.
const byteCeilingMarker = "BytesWritten"

var countCeilingMarkers = []string{"RowCount", "ChangeCount"}

// chunkWriteLane is one discovered lane: the unit that owns a chunk writer
// (its receiver type, or the enclosing function when there is none) plus
// which ceilings that unit compares against.
type chunkWriteLane struct {
	unit   string
	file   string
	family string
	bytes  bool
	count  bool
}

// countCeilingExempt lists lanes with no event-count ceiling, and why. A byte
// ceiling is never exempt — that is the property this gate exists for.
var countCeilingExempt = map[string]string{
	"chunkStreamSink": "smart compaction rewrites the collapsed stream into a FIXED set of pre-existing manifest " +
		"slots (roadmap item 130), so there is no event-count semantics to bound — its budget is bytes per slot " +
		"by construction, and rolling on a count would desynchronise the slot cursor from the manifest",
}

// TestChunkWriteLanes_EveryLaneBoundsAChunkByBytes is the item-142 roster.
//
// The independent expected value is the source's own AST: the gate does not
// consult anything a lane reports about itself, and it does not run a
// backup. It is a STRUCTURAL check — it proves a byte comparison exists on
// each lane's roll path, not that the ceiling actually rolls where it should.
//
// KNOWN COVERAGE ASYMMETRY, stated rather than implied, since a gate whose
// scope is narrower than its name is worse than none: only two of the four
// lanes have a BEHAVIOURAL pin behind this structural one. The data lane has
// `TestChunkByteCeiling_*` (backup/backup_chunk_byte_ceiling_test.go) and
// compaction has `chain_compact_smart_output_test.go`. Neither CHANGE-chunk
// lane has an equivalent — nothing today writes a wide change and asserts
// the chunk rolled on bytes rather than on count. That is a real gap and the
// obvious next pin; this gate closes the "one lane and not its sibling"
// class, not the "the threshold is wrong" class.
//
// Mutation-verified: deleting the `BytesWritten()` clause from EITHER change
// lane fails it, and a stale exemption fails it.
func TestChunkWriteLanes_EveryLaneBoundsAChunkByBytes(t *testing.T) {
	fset := token.NewFileSet()

	// The constructors must still exist where the walker looks for them.
	blob := parsePkgDir(t, fset, filepath.Join(".", "blobcodec"))
	for ctor := range chunkWriterCtors {
		if _, ok := blob[ctor]; !ok {
			t.Fatalf("blobcodec no longer declares %q — the walker would discover zero lanes and pass vacuously", ctor)
		}
	}

	// Lanes live in this package and in the backup orchestrator package.
	// Discovery is by call site; the directory list is where to LOOK, not
	// what to find.
	lanes := map[string]*chunkWriteLane{}

	for _, dir := range []string{".", filepath.Join(".", "backup")} {
		decls := parsePkgDirWithPos(t, fset, dir)
		// unitOf groups a declaration with its siblings: methods share their
		// receiver type, because a lane routinely opens the writer in one
		// method and rolls in another (both change lanes and the data lane
		// all do). A plain function is its own unit.
		unitOf := func(fn *ast.FuncDecl) string {
			if fn.Recv != nil && len(fn.Recv.List) > 0 {
				return receiverTypeName(fn.Recv.List[0].Type)
			}
			return fn.Name.Name
		}
		for _, fn := range decls {
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				family, ok := chunkWriterCtors[sel.Sel.Name]
				if !ok {
					return true
				}
				u := unitOf(fn)
				if lanes[u] == nil {
					lanes[u] = &chunkWriteLane{
						unit:   u,
						file:   filepath.Base(fset.Position(fn.Pos()).Filename),
						family: family,
					}
				}
				return true
			})
		}
		// Second pass: for every discovered unit in this directory, look for
		// the ceiling markers used inside a COMPARISON. A marker read into a
		// manifest field is a stamp, not a ceiling, and must not count.
		for _, fn := range decls {
			u := unitOf(fn)
			l := lanes[u]
			if l == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				bin, ok := n.(*ast.BinaryExpr)
				if !ok {
					return true
				}
				switch bin.Op {
				case token.GEQ, token.GTR, token.LSS, token.LEQ:
				default:
					return true
				}
				for _, side := range []ast.Expr{bin.X, bin.Y} {
					switch methodCallName(side) {
					case byteCeilingMarker:
						l.bytes = true
					default:
						for _, m := range countCeilingMarkers {
							if methodCallName(side) == m {
								l.count = true
							}
						}
					}
				}
				return true
			})
		}
	}

	// Anti-vacuity floors. A walker that stops matching must fail loudly
	// rather than pass on zero findings, and the CHANGE family is the one
	// item 142 is about — a roster that found only data lanes would be
	// green for exactly the defect it was built for.
	const wantLanes, wantChangeLanes = 4, 3
	changeLanes := 0
	for _, l := range lanes {
		if l.family == "change" {
			changeLanes++
		}
	}
	t.Logf("discovered %d chunk-write lanes (%d change): %s", len(lanes), changeLanes, describeLanes(lanes))
	if len(lanes) < wantLanes || changeLanes < wantChangeLanes {
		t.Fatalf("discovered %d chunk-write lanes (%d of them change-chunk lanes): %s. Expected at least %d / %d "+
			"(incremental.go's captureWindow, stream.go's changeChunkBuffer, chain_compact_smart.go's "+
			"chunkStreamSink, backup_parallel.go's backupChunkStreamer). The walker has stopped matching and "+
			"this gate is vacuous", len(lanes), changeLanes, describeLanes(lanes), wantLanes, wantChangeLanes)
	}

	for _, l := range lanes {
		if !l.bytes {
			t.Errorf("%s (%s, %s chunks) opens a chunk writer but never compares %s() — the chunk buffers in "+
				"memory bounded only by an event/row count, so one wide TEXT/JSON/BLOB column makes the same "+
				"count mean orders of magnitude more bytes (roadmap items 116 P3 / 142). Roll on bytes as well "+
				"as count. There is no exemption for this half",
				l.unit, l.file, l.family, byteCeilingMarker)
		}
		if l.count {
			continue
		}
		reason, exempt := countCeilingExempt[l.unit]
		if !exempt {
			t.Errorf("%s (%s, %s chunks) bounds a chunk by bytes but by no event count — add the count ceiling, "+
				"or add %q to countCeilingExempt with the reason it has no event-count semantics",
				l.unit, l.file, l.family, l.unit)
			continue
		}
		t.Logf("count-exempt: %s (%s) — %s", l.unit, l.file, reason)
	}
	for name := range countCeilingExempt {
		if lanes[name] == nil {
			t.Errorf("countCeilingExempt names %q, which the walker no longer discovers as a chunk-write lane. "+
				"Either it was renamed/removed (drop the entry) or the walker stopped matching it (fix the "+
				"walker) — a stale exemption silently shrinks this gate", name)
		}
	}
}

// methodCallName returns the method name of a `x.Y()` call expression, or "".
func methodCallName(e ast.Expr) string {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return ""
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	return sel.Sel.Name
}

func receiverTypeName(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.StarExpr:
		return receiverTypeName(v.X)
	case *ast.Ident:
		return v.Name
	case *ast.IndexExpr:
		return receiverTypeName(v.X)
	}
	return ""
}

func describeLanes(lanes map[string]*chunkWriteLane) string {
	out := make([]string, 0, len(lanes))
	for _, l := range lanes {
		out = append(out, l.unit+" ("+l.file+", "+l.family+")")
	}
	return strings.Join(out, ", ")
}

// parsePkgDir returns the declared top-level function names in dir.
func parsePkgDir(t *testing.T, fset *token.FileSet, dir string) map[string]*ast.FuncDecl {
	t.Helper()
	out := map[string]*ast.FuncDecl{}
	for _, fn := range parsePkgDirWithPos(t, fset, dir) {
		out[fn.Name.Name] = fn
	}
	return out
}

// parsePkgDirWithPos parses the non-test sources of dir and returns every
// function with a body, positions intact.
func parsePkgDirWithPos(t *testing.T, fset *token.FileSet, dir string) []*ast.FuncDecl {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var out []*ast.FuncDecl
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		for _, d := range f.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok && fn.Body != nil {
				out = append(out, fn)
			}
		}
	}
	if len(out) == 0 {
		t.Fatalf("parsed no functions from %s — the walker cannot see the source it grades", dir)
	}
	return out
}
