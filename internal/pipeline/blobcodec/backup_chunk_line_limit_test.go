// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// A backup you can take, verify clean, and never restore (Bug 226).
//
// The chunk reader capped a line at 64 MiB; the writer had no cap. A row
// whose serialized form exceeded it was written happily, so:
//
//	backup full    rc=0
//	backup verify  rc=0        <-- reports the backup fine
//	restore        rc=1, 0 rows   "bufio.Scanner: token too long"
//
// The operator finds out at restore time. `verify` cannot see it because it
// rehashes the chunk BYTES and never parses a row — its evidence is the same
// artifact, so it confirms the bytes are intact while saying nothing about
// whether they can be read back. That is the shared-evidence class the
// project's own rule names: a verification whose "all clear" derives from the
// same artifact as the thing it verifies.
//
// Found by the v0.111.1 regression cycle while BOUNDING item 116's new chunk
// byte ceiling — the neighbouring fix is what led someone to push on the
// limit next door. Pre-existing on v0.111.0 and earlier.
//
// # The sibling sweep, and why this file's gate was rewritten
//
// The first fix closed the DATA-chunk path and its gate asserted
// "checkChunkLineLength appears on 2 write paths", counted by substring in
// backup_chunk.go alone. There was a THIRD write core — [ChangeChunkWriter],
// in another file — with no refusal at all, and its reader capped a line at a
// LITERAL 64 MiB rather than the shared constant. So the original defect was
// still fully live on the incremental/CDC path, where a change event carries a
// whole row image and a wide TEXT/JSON/BLOB column reaches the limit by
// exactly the same route.
//
// The gate did worse than miss it: pinned to `== 2`, it would have FAILED on a
// correct fix. That is the "a gate owes the same enumeration" shape, in its
// sharpest form — a check whose name says "both write cores" and whose reach
// is "the two in this file", which stops anyone from looking.
//
// So the roster is no longer a number in a string search. It is DERIVED from
// the package's AST: every function that writes a marshalled record to a chunk
// stream must refuse over-long lines, and every scanner that reads one back
// must be sized from the shared constant. A fourth write core added tomorrow
// fails this test until it is guarded, without anyone remembering to bump a
// count.

package blobcodec

import (
	"bytes"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func lineLimitColumns() []*ir.Column {
	return []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "body", Type: ir.Text{}},
	}
}

// TestChunkLineLimit_WriterRefusesAnUnreadableRow is the fix: a row the
// reader could never scan must not reach the artifact.
func TestChunkLineLimit_WriterRefusesAnUnreadableRow(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewChunkWriter(&buf, []string{"id", "body"}, nil, CodecNone, nil)
	if err != nil {
		t.Fatalf("NewChunkWriter: %v", err)
	}
	// Comfortably past the limit once JSON-encoded.
	row := ir.Row{"id": int64(1), "body": strings.Repeat("x", MaxChunkLineBytes+1024)}

	err = w.WriteRow(row, lineLimitColumns())
	if err == nil {
		t.Fatal("WriteRow ACCEPTED a row larger than the reader's line limit.\n\n" +
			"That produces a backup `backup verify` reports as OK and `restore` cannot read — the " +
			"operator discovers it at the moment they need the backup. Bug 226.")
	}
	for _, want := range []string{"exceeds", "restore", "verify"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q — it must say what would have happened, not just "+
				"that a number was exceeded.\ngot: %v", want, err)
		}
	}
}

// The control, and the half that keeps the refusal from being a regression: a
// large-but-readable row must still round-trip. Wide rows are the workload
// this format is for.
func TestChunkLineLimit_LargeButReadableRowRoundTrips(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewChunkWriter(&buf, []string{"id", "body"}, nil, CodecNone, nil)
	if err != nil {
		t.Fatalf("NewChunkWriter: %v", err)
	}
	// 1 MiB — large, ordinary, and nowhere near the limit.
	body := strings.Repeat("y", 1<<20)
	if err := w.WriteRow(ir.Row{"id": int64(1), "body": body}, lineLimitColumns()); err != nil {
		t.Fatalf("WriteRow refused an ordinary wide row: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewChunkReader(io.NopCloser(bytes.NewReader(buf.Bytes())), "", nil, CodecNone, nil)
	if err != nil {
		t.Fatalf("NewChunkReader: %v", err)
	}
	defer func() { _ = r.Close() }()
	rows := 0
	for {
		got, err := r.ReadRow()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadRow: %v", err)
		}
		if s, _ := got["body"].(string); len(s) != len(body) {
			t.Fatalf("body round-tripped at %d bytes; want %d", len(s), len(body))
		}
		rows++
	}
	if rows != 1 {
		t.Fatalf("read %d rows; want 1", rows)
	}
}

// The CHANGE-chunk half of the same defect — the third write core, and the one
// the original fix missed. An incremental backup's chunks are written by this
// writer, so before this refusal existed the full Bug 226 sequence reproduced
// on `backup incremental` exactly as it did on `backup full`.
func TestChunkLineLimit_ChangeWriterRefusesAnUnreadableRow(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewChangeChunkWriter(&buf, nil, CodecNone, nil)
	if err != nil {
		t.Fatalf("NewChangeChunkWriter: %v", err)
	}
	ins := ir.Insert{
		Position: ir.Position{Engine: "postgres", Token: `{"lsn":"0/100"}`},
		Schema:   "public",
		Table:    "wide",
		Row:      ir.Row{"id": int64(1), "body": strings.Repeat("x", MaxChunkLineBytes+1024)},
	}

	err = w.WriteChange(ins)
	if err == nil {
		t.Fatal("WriteChange ACCEPTED a change whose row exceeds the reader's line limit.\n\n" +
			"An incremental carrying it verifies clean and cannot be restored — Bug 226 on the " +
			"CDC path, which is where the first fix's sibling sweep stopped short.")
	}
	for _, want := range []string{"exceeds", "restore", "verify"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q — it must say what would have happened.\ngot: %v", want, err)
		}
	}
}

// The change-chunk control: a large-but-readable change still round-trips
// through the real reader. Read back with the reader rather than asserting on
// the writer's own bookkeeping — the whole bug was a writer that agreed with
// itself.
func TestChunkLineLimit_ChangeWriterLargeButReadableRoundTrips(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewChangeChunkWriter(&buf, nil, CodecNone, nil)
	if err != nil {
		t.Fatalf("NewChangeChunkWriter: %v", err)
	}
	body := strings.Repeat("y", 1<<20) // 1 MiB
	if err := w.WriteChange(ir.Insert{
		Position: ir.Position{Engine: "postgres", Token: `{"lsn":"0/110"}`},
		Schema:   "public",
		Table:    "wide",
		Row:      ir.Row{"id": int64(1), "body": body},
	}); err != nil {
		t.Fatalf("WriteChange refused an ordinary wide row: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewChangeChunkReader(nopReadCloserFromBytes(buf.Bytes()), w.Hash(), nil, CodecNone, nil)
	if err != nil {
		t.Fatalf("NewChangeChunkReader: %v", err)
	}
	defer func() { _ = r.Close() }()

	c, err := r.ReadChange()
	if err != nil {
		t.Fatalf("ReadChange: %v", err)
	}
	ins, ok := c.(ir.Insert)
	if !ok {
		t.Fatalf("read back %T; want ir.Insert", c)
	}
	if s, _ := ins.Row["body"].(string); len(s) != len(body) {
		t.Fatalf("body round-tripped at %d bytes; want %d", len(s), len(body))
	}
}

// The refusal must be identifiable with errors.Is, because the compaction path
// adds a remedy the codec cannot know about (--smart-compaction-off) and keys
// that off the sentinel. Dropping the %w would leave the refusal correct and
// the hint silently dead, which is the kind of regression nothing else notices.
func TestChunkLineLimit_RefusalWrapsTheSentinel(t *testing.T) {
	err := checkChunkLineLength(MaxChunkLineBytes + 1)
	if err == nil {
		t.Fatal("checkChunkLineLength accepted a line over the limit")
	}
	if !errors.Is(err, ErrChunkLineTooLong) {
		t.Errorf("refusal does not wrap ErrChunkLineTooLong, so callers cannot recognise it: %v", err)
	}
	if err := checkChunkLineLength(MaxChunkLineBytes - 1); err != nil {
		t.Errorf("checkChunkLineLength refused a line under the limit: %v", err)
	}
}

// TestChunkLineLimit_EveryWriteCoreAndReaderShareOneLimit is the structural
// pin, and the reason this bug was possible at all: the reader's scanner cap
// and the writer's refusal were independent numbers, one of which did not
// exist — and after the first fix, one of which was still a literal.
//
// The roster is DERIVED from the package AST rather than counted by substring
// in one file, because the counting version pinned the wrong number and would
// have failed on the correct fix. Two rules, each with an anti-vacuity floor
// so a parse that finds nothing cannot pass silently:
//
//   - every function writing a marshalled record to a chunk stream (the
//     `bufW.Write(b)` shape) must call checkChunkLineLength;
//   - every bufio.Scanner over a chunk stream must be sized from
//     MaxChunkLineBytes, never a literal.
func TestChunkLineLimit_EveryWriteCoreAndReaderShareOneLimit(t *testing.T) {
	files := parsePackageSource(t)

	var (
		writeCores  []string
		unguarded   []string
		scanners    []string
		unsizedScan []string
	)
	for name, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				return true
			}
			where := name + ":" + fn.Name.Name
			if bodyCallsBufWWrite(fn.Body) {
				writeCores = append(writeCores, where)
				if !bodyCallsIdent(fn.Body, "checkChunkLineLength") {
					unguarded = append(unguarded, where)
				}
			}
			if bodyCallsSelector(fn.Body, "bufio", "NewScanner") {
				scanners = append(scanners, where)
				if !bodyScannerSizedFromConstant(fn.Body) {
					unsizedScan = append(unsizedScan, where)
				}
			}
			return true
		})
	}

	// Anti-vacuity floors. Three write cores and two readers exist today; a
	// walker that suddenly finds fewer has stopped matching the code, and a
	// silent zero is precisely how a derived gate rots into a green no-op.
	if len(writeCores) < 3 {
		t.Fatalf("found %d chunk write core(s) %v; expected at least 3 (the fast encoder, "+
			"writeRowLegacy, and the change-chunk writer).\n\nThe walker is no longer matching the "+
			"code, so this gate is vacuous — fix the walker, do not lower the floor.", len(writeCores), writeCores)
	}
	if len(scanners) < 2 {
		t.Fatalf("found %d chunk scanner(s) %v; expected at least 2 (the data-chunk and "+
			"change-chunk readers). The walker is no longer matching the code.", len(scanners), scanners)
	}

	if len(unguarded) > 0 {
		t.Errorf("these chunk write cores do not refuse an over-long line: %v\n\n"+
			"Each one can write a row that `backup verify` reports as OK and `restore` cannot read "+
			"(Bug 226). Call checkChunkLineLength on the marshalled bytes before writing them.", unguarded)
	}
	if len(unsizedScan) > 0 {
		t.Errorf("these chunk scanners are not sized from MaxChunkLineBytes: %v\n\n"+
			"A literal here is how the writer's refusal and the reader's capacity drift apart, which "+
			"is the defect this constant exists to make impossible.", unsizedScan)
	}

	// Belt and braces: the specific literal that was the original defect must
	// not reappear anywhere in the package's production source.
	for name, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			bl, ok := n.(*ast.BasicLit)
			if ok && bl.Kind == token.INT && (bl.Value == "67108864") {
				t.Errorf("%s carries a raw 67108864 literal — use MaxChunkLineBytes", name)
			}
			return true
		})
		if strings.Contains(mustReadFile(t, name), "64*1024*1024") {
			t.Errorf("%s still configures a limit from the LITERAL 64*1024*1024 rather than "+
				"MaxChunkLineBytes; writer and reader can drift again", name)
		}
	}
}

// parsePackageSource parses every non-test .go file in this package.
func parsePackageSource(t *testing.T) map[string]*ast.File {
	t.Helper()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob package sources: %v", err)
	}
	fset := token.NewFileSet()
	out := make(map[string]*ast.File, len(names))
	for _, n := range names {
		if strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", n, err)
		}
		out[n] = f
	}
	if len(out) == 0 {
		t.Fatal("parsed no package sources — the gate would be vacuous")
	}
	return out
}

func mustReadFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// bodyCallsBufWWrite reports whether body emits one marshalled chunk record.
//
// # Dual-signal discovery, because the field name alone was escapable
// (audit 2026-08-05 B-8)
//
// The first version of this walker matched `<recv>.bufW.Write(...)` — the field
// NAME. An executed mutation proved that hollow: a fourth write core spelling
// its buffer `out *bufio.Writer` stayed GREEN, and renaming it to `bufW` went
// RED. So the gate's promise that "a fourth write core added tomorrow fails
// this test until it is guarded" held only under an unstated naming
// convention — the same single-signal undercount this file's own header
// criticises one paragraph earlier. A gate that can be escaped by choosing a
// different identifier is a naming lint wearing a correctness gate's name.
//
// Two independent signals now, unioned:
//
//	(1) the conventional field name `bufW`, and
//	(2) the SHAPE of emitting a line — a `Write(...)` and a `WriteByte('\n')`
//	    on the SAME buffer field, whatever it is called.
//
// Signal (2) is what makes renaming useless. It also stays precise: the
// change-chunk writer's `Close` writes ciphertext to a bare `out` with no
// trailing newline, so requiring both calls on one field keeps Close out of the
// roster rather than demanding a line-length check on a whole-chunk write.
func bodyCallsBufWWrite(body *ast.BlockStmt) bool {
	writesTo := map[string]bool{}   // field name → saw Write(...)
	newlinesTo := map[string]bool{} // field name → saw WriteByte('\n')

	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		outer, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		inner, ok := outer.X.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch outer.Sel.Name {
		case "Write":
			writesTo[inner.Sel.Name] = true
		case "WriteByte":
			if len(call.Args) == 1 && isNewlineLiteral(call.Args[0]) {
				newlinesTo[inner.Sel.Name] = true
			}
		}
		return true
	})

	// Signal 1: the conventional name.
	if writesTo["bufW"] {
		return true
	}
	// Signal 2: the line-emitting shape, under any field name.
	for field := range writesTo {
		if newlinesTo[field] {
			return true
		}
	}
	return false
}

// isNewlineLiteral reports whether e is the char literal '\n'.
func isNewlineLiteral(e ast.Expr) bool {
	bl, ok := e.(*ast.BasicLit)
	return ok && bl.Kind == token.CHAR && bl.Value == `'\n'`
}

// bodyCallsIdent reports whether body calls the named package-level function.
func bodyCallsIdent(body *ast.BlockStmt, name string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == name {
			found = true
		}
		return true
	})
	return found
}

// bodyCallsSelector reports whether body calls pkg.fn(...).
func bodyCallsSelector(body *ast.BlockStmt, pkg, fn string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != fn {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == pkg {
			found = true
		}
		return true
	})
	return found
}

// bodyScannerSizedFromConstant reports whether body contains a
// `<sc>.Buffer(..., MaxChunkLineBytes)` call.
func bodyScannerSizedFromConstant(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Buffer" || len(call.Args) != 2 {
			return true
		}
		if id, ok := call.Args[1].(*ast.Ident); ok && id.Name == "MaxChunkLineBytes" {
			found = true
		}
		return true
	})
	return found
}
