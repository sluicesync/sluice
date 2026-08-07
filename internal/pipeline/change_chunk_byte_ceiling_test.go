// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 130's residual: the change-chunk byte ceiling, BEHAVIOURALLY on
// both lanes, and BOUND to the budget smart compaction assumes it produces.
//
// # Why this file exists, in two halves
//
// **The behavioural half is a gap item 142's own gate names.** That gate
// (`chunk_byte_ceiling_gate_test.go`) is a structural AST roster: it proves a
// `BytesWritten()` comparison EXISTS on every lane that opens a chunk writer.
// Its doc says out loud that only two of the four lanes have a behavioural pin
// behind that structure, and that "nothing today writes a wide change and
// asserts the chunk rolled on bytes rather than on count". A comparison that
// exists and never fires is indistinguishable, to that gate, from one that
// works. These tests drive the REAL roll paths of both change lanes with wide
// events and assert the chunks actually rolled.
//
// **The binding half is a premise item 130 rests on.** `chunkStreamSink`'s
// bound is max(B, I) — B the per-slot output budget, I the largest input chunk.
// It is the ADVERTISED budget only while I <= B, and that holds only because
// the lanes that WRITE change chunks roll on the same constant compaction
// BUDGETS from. Those are two separate facts in two separate packages, each
// pinned, with nothing binding them — the exact shape CLAUDE.md names in the
// VStream-carrier example. When that type shipped in v0.113.0 the premise was
// false (the stream lane rolled on event count alone) and its doc said so; item
// 142 made it true in v0.114.0, and nothing would notice if it drifted back.
//
// # What these reach, stated so the names cannot be read as broader
//
//   - The two CHANGE-chunk production lanes: `stream.go`'s `changeChunkBuffer`
//     (continuous `backup stream run`, which writes most change chunks) and
//     `incremental.go`'s `captureWindow` (`backup incremental`). The DATA lane
//     already has `TestChunkByteCeiling_*`; the compaction lane already has
//     `chain_compact_smart_output_test.go`. Between them, all four lanes on
//     item 142's roster now have a behavioural pin.
//   - The binding is over the SYMBOL, not the value. A value-equality test
//     would pass against two literals that agree today and drift tomorrow,
//     which is precisely the failure being closed — the same reasoning item
//     128 used for `MaxChunkLineBytes`.
//   - Neither half says the ceiling's VALUE is right. They say the roll fires
//     on bytes, and that both writers and the compactor mean one number.

package pipeline

import (
	"context"
	"fmt"
	"go/ast"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// ----- fixtures -----

// wideChange is one INSERT whose payload column is `width` bytes. The event
// count says nothing about its serialized size, which is the whole problem the
// byte ceiling exists for.
func wideChange(id int64, body string) ir.Change {
	return ir.Insert{
		Position: ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"lsn":"0/%X"}`, id+1)},
		Schema:   "public",
		Table:    "wide",
		Row:      ir.Row{"id": id, "body": body},
	}
}

// changeCeilingCorpus sizes a corpus that crosses the byte ceiling several
// times while leaving the event-count cap unreachable.
//
// The ceiling is [backup.DefaultBackupChunkBytes] and is NOT injectable on
// either change lane (unlike the data lane's `Backup.ChunkBytes`), so the
// corpus has to be genuinely that large. It is one repeated payload, so the
// cost is gzip's rather than the allocator's: the body is built once, and
// highly compressible bytes keep the on-store buffer tiny while
// `BytesWritten()` — which counts UNCOMPRESSED bytes, deliberately — climbs.
func changeCeilingCorpus(t *testing.T) (events int, body string, wantChunks int) {
	t.Helper()
	const width = 2 << 20
	n := int(3*backup.DefaultBackupChunkBytes/width) + 2
	return n, strings.Repeat("x", width), 3
}

// assertChangeCountsSumTo checks the recorded per-chunk counts account for
// every event. Rolling must not lose or duplicate one — the failure mode that
// would make an "it rolled!" assertion worthless.
func assertChangeCountsSumTo(t *testing.T, m *irbackup.Manifest, want int64) {
	t.Helper()
	var total int64
	for _, ch := range m.ChangeChunks {
		total += ch.RowCount
	}
	if total != want {
		t.Errorf("chunk event counts sum to %d; want %d — rolling must not lose or duplicate an event", total, want)
	}
}

// ----- behavioural: the stream lane (`backup stream run`) -----

// driveStreamLane feeds changes through the production `processChange` and
// flushes the tail, returning the manifest the flushes recorded. Encryption is
// off and there is no segment-dedup floor: the minimum that reaches the real
// roll path.
func driveStreamLane(t *testing.T, chunkSize, events int, body string) *irbackup.Manifest {
	t.Helper()
	m := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		SourceEngine:  "postgres",
		CreatedAt:     time.Unix(0, 0).UTC(),
		Kind:          irbackup.BackupKindIncremental,
	}
	cb := &changeChunkBuffer{
		b:            &BackupStream{segStore: newMemStore(), segCodec: blobcodec.CodecGzip},
		manifest:     m,
		runNamespace: changeChunkRunNamespace(m),
	}
	out := &captureOutcome{}
	inTx := false
	for i := range events {
		terminate, err := cb.processChange(
			context.Background(), wideChange(int64(i), body), out, &inTx,
			false /*deadlinePassed*/, chunkSize, 0 /*maxChanges*/, 0, /*maxBytes*/
		)
		if err != nil {
			t.Fatalf("processChange %d: %v", i, err)
		}
		if terminate {
			t.Fatalf("processChange %d asked to terminate the window; no window cap should have fired", i)
		}
	}
	if err := cb.flushTo(context.Background(), out); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	if out.TotalChanges != int64(events) {
		t.Fatalf("TotalChanges = %d; want %d — the lane did not see every event", out.TotalChanges, events)
	}
	return m
}

// TestChangeChunkCeiling_StreamLaneRollsOnBytes is the behavioural half for the
// lane that writes most change chunks in practice.
//
// The independent expected value: the event-count cap is set to a number the
// corpus can never reach (100k events against a few hundred), so EVERY chunk
// boundary observed here was produced by the byte ceiling and by nothing else.
// A test that let both caps be reachable could not say which one fired.
func TestChangeChunkCeiling_StreamLaneRollsOnBytes(t *testing.T) {
	events, body, wantChunks := changeCeilingCorpus(t)
	const unreachableCount = 100_000

	m := driveStreamLane(t, unreachableCount, events, body)

	if len(m.ChangeChunks) < wantChunks {
		t.Fatalf("the stream lane wrote %d change chunk(s); want >= %d.\n\n"+
			"%d events of %d bytes is ~%d MiB against a %d MiB ceiling, and the event cap of %d is two "+
			"orders of magnitude away and can never fire. A chunk buffers in memory until it rolls, so "+
			"with no byte ceiling this whole rollover is ONE chunk (roadmap item 142).",
			len(m.ChangeChunks), wantChunks, events, len(body),
			int64(events)*int64(len(body))>>20, backup.DefaultBackupChunkBytes>>20, unreachableCount)
	}
	t.Logf("stream lane: %d events x %d B -> %d chunks", events, len(body), len(m.ChangeChunks))
	assertChangeCountsSumTo(t, m, int64(events))
}

// TestChangeChunkCeiling_StreamLaneNarrowChangesStillRollOnCount is the
// control, and the half that matters most: the byte ceiling must not move a
// boundary on the ordinary shape. Every change chunk in every existing store
// was written by the event cap, and a ceiling that fired early on narrow events
// would repartition them.
func TestChangeChunkCeiling_StreamLaneNarrowChangesStillRollOnCount(t *testing.T) {
	const (
		events    = 30
		chunkSize = 10
	)
	m := driveStreamLane(t, chunkSize, events, "xxxxxxxx")

	if len(m.ChangeChunks) != events/chunkSize {
		t.Fatalf("the stream lane wrote %d change chunk(s); want exactly %d — narrow events must still roll "+
			"on the EVENT count, at exactly the boundaries they always did",
			len(m.ChangeChunks), events/chunkSize)
	}
	for i, ch := range m.ChangeChunks {
		if ch.RowCount != chunkSize {
			t.Errorf("chunk %d holds %d events; want %d", i, ch.RowCount, chunkSize)
		}
	}
}

// ----- behavioural: the incremental lane (`backup incremental`) -----

// driveIncrementalLane runs the production `captureWindow` over a closed
// channel of wide changes. Closing the channel is the orderly window end, which
// flushes the tail — the same exit a CDC reader reaching EOF takes.
func driveIncrementalLane(t *testing.T, chunkSize, events int, body string) *irbackup.Manifest {
	t.Helper()
	m := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		SourceEngine:  "postgres",
		CreatedAt:     time.Unix(0, 0).UTC(),
		Kind:          irbackup.BackupKindIncremental,
	}
	b := &IncrementalBackup{segStore: newMemStore(), segCodec: blobcodec.CodecGzip}

	ch := make(chan ir.Change, events)
	for i := range events {
		ch <- wideChange(int64(i), body)
	}
	close(ch)

	// A deadline an hour out and no maxChanges cap, so the ONLY thing that can
	// close a chunk is the ceiling under test.
	_, total, _, err := b.captureWindow(
		context.Background(), nil /*cdc*/, ch, m, chunkSize,
		time.Now().Add(time.Hour), 0 /*maxChanges*/, ir.Position{}, time.Now, nil, /*chainCEK*/
	)
	if err != nil {
		t.Fatalf("captureWindow: %v", err)
	}
	if total != int64(events) {
		t.Fatalf("captureWindow saw %d changes; want %d", total, events)
	}
	return m
}

// TestChangeChunkCeiling_IncrementalLaneRollsOnBytes is the sibling of the
// stream-lane pin above. Both lanes, not one: the whole reason item 142 exists
// is that audit C-3 fixed this lane and its enumeration stopped here, so a
// behavioural pin on only one of them would repeat that mistake at the test
// layer.
func TestChangeChunkCeiling_IncrementalLaneRollsOnBytes(t *testing.T) {
	events, body, wantChunks := changeCeilingCorpus(t)
	const unreachableCount = 100_000

	m := driveIncrementalLane(t, unreachableCount, events, body)

	if len(m.ChangeChunks) < wantChunks {
		t.Fatalf("the incremental lane wrote %d change chunk(s); want >= %d — with the event cap at %d and "+
			"unreachable, every boundary must have come from the byte ceiling",
			len(m.ChangeChunks), wantChunks, unreachableCount)
	}
	t.Logf("incremental lane: %d events x %d B -> %d chunks", events, len(body), len(m.ChangeChunks))
	assertChangeCountsSumTo(t, m, int64(events))
}

// TestChangeChunkCeiling_IncrementalLaneNarrowChangesStillRollOnCount is this
// lane's boundaries-unmoved control. See the stream-lane twin for why the
// control is the half that matters most.
func TestChangeChunkCeiling_IncrementalLaneNarrowChangesStillRollOnCount(t *testing.T) {
	const (
		events    = 30
		chunkSize = 10
	)
	m := driveIncrementalLane(t, chunkSize, events, "xxxxxxxx")

	if len(m.ChangeChunks) != events/chunkSize {
		t.Fatalf("the incremental lane wrote %d change chunk(s); want exactly %d — narrow events must still "+
			"roll on the EVENT count", len(m.ChangeChunks), events/chunkSize)
	}
	for i, ch := range m.ChangeChunks {
		if ch.RowCount != chunkSize {
			t.Errorf("chunk %d holds %d events; want %d", i, ch.RowCount, chunkSize)
		}
	}
}

// ----- the binding -----

// changeCeilingSymbol is the constant BOTH the change-chunk writers roll on and
// smart compaction budgets its output slots from. The binding is over this
// NAME: two literals that agree today and drift tomorrow is the failure being
// closed, not the one being tested for.
const changeCeilingSymbol = "DefaultBackupChunkBytes"

// compactionBudgetFunc is where compaction resolves its default per-slot
// budget. Named so a rename fails this gate loudly instead of shrinking it.
const compactionBudgetFunc = "applySmartCompactionToIncrementalSized"

// changeLaneCeiling is one discovered production lane and the symbols it rolls
// its change chunk on.
type changeLaneCeiling struct {
	file    string
	symbols []string
}

// TestChangeChunkLanes_RollOnTheSameCeilingCompactionBudgetsFrom binds
// `chunkStreamSink`'s I <= B premise to the code that has to produce it.
//
// # What it compares against, independently
//
// Smart compaction's advertised bound holds only while the largest input chunk
// (I) is within its per-slot output budget (B). B is resolved in
// `chain_compact_smart.go`; I is decided by two writer lanes in two other
// files. The independent expected value here is the WRITERS' ceiling symbol,
// read out of their own source — the gate does not ask compaction what its
// budget is and then check compaction against itself.
//
// # Why a symbol and not a number
//
// `if writer.BytesWritten() >= 64<<20` in a lane and `budget = 64<<20` in the
// compactor agree, until one of them is tuned. Item 128 closed exactly this
// shape for `MaxChunkLineBytes` — the reader's scanner and the writer's refusal
// had been two literals — and its durable fix was asserting they come from ONE
// constant rather than that they happen to be equal.
//
// # Scope, and the one deliberate asymmetry
//
// It grades the change-chunk PRODUCTION lanes: units under `internal/pipeline`
// that construct a `NewChangeChunkWriter` and roll on `BytesWritten()`.
// `chunkStreamSink` is a change-chunk writer too but is deliberately NOT graded
// the same way — it is the CONSUMER of this premise, its budget is a field, and
// requiring it to name the constant inline would forbid the injectable budget
// its own anti-vacuity control depends on. It is checked instead at the place
// that resolves the default, which is what the premise is actually about.
//
// It is a STRUCTURAL check: it proves the two ends name one constant, not that
// either end's roll fires. The four behavioural pins above are that half.
//
// Anti-vacuity: it fails if it does not discover both production lanes by name,
// and if it cannot find the compaction default at all.
func TestChangeChunkLanes_RollOnTheSameCeilingCompactionBudgetsFrom(t *testing.T) {
	fset := token.NewFileSet()
	lanes := map[string]*changeLaneCeiling{}

	decls := parsePkgDirWithPos(t, fset, ".")
	unitOf := func(fn *ast.FuncDecl) string {
		if fn.Recv != nil && len(fn.Recv.List) > 0 {
			return receiverTypeName(fn.Recv.List[0].Type)
		}
		return fn.Name.Name
	}
	for _, fn := range decls {
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			e, ok := n.(ast.Expr)
			if !ok || methodCallName(e) != "NewChangeChunkWriter" {
				return true
			}
			if lanes[unitOf(fn)] == nil {
				lanes[unitOf(fn)] = &changeLaneCeiling{file: filepath.Base(fset.Position(fn.Pos()).Filename)}
			}
			return true
		})
	}
	for _, fn := range decls {
		l := lanes[unitOf(fn)]
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
			// The operand OPPOSITE the BytesWritten() call is the ceiling.
			if methodCallName(bin.X) == "BytesWritten" {
				l.symbols = append(l.symbols, exprSymbolName(bin.Y))
			}
			if methodCallName(bin.Y) == "BytesWritten" {
				l.symbols = append(l.symbols, exprSymbolName(bin.X))
			}
			return true
		})
	}

	// Anti-vacuity: both production lanes, by name, or the walker is stale.
	// The names are the UNIT — a method's receiver type, matching how item
	// 142's roster groups a lane that opens in one method and rolls in
	// another. So the incremental lane appears as `IncrementalBackup`, not as
	// `captureWindow`; that floor caught this file's own first draft naming it
	// the other way, which is the behaviour the floor is for.
	wantLanes := []string{"changeChunkBuffer", "IncrementalBackup"}
	for _, want := range wantLanes {
		if lanes[want] == nil {
			t.Fatalf("the walker did not discover %q as a change-chunk lane — it has been renamed or the "+
				"match has gone stale, and this gate would pass on an empty roster (found: %v)",
				want, laneCeilingNames(lanes))
		}
	}
	t.Logf("change-chunk production lanes: %v", laneCeilingNames(lanes))

	for _, name := range wantLanes {
		l := lanes[name]
		if len(l.symbols) == 0 {
			t.Errorf("%s (%s) opens a change-chunk writer and never bounds it against a BytesWritten() "+
				"ceiling — smart compaction's peak bound is max(budget, largest input chunk), so an "+
				"unbounded input chunk is an unbounded compaction peak (roadmap items 130 / 142)",
				name, l.file)
			continue
		}
		for _, sym := range l.symbols {
			if sym == changeCeilingSymbol {
				continue
			}
			t.Errorf("%s (%s) rolls its change chunk on %q, not on %s.\n\n"+
				"chunkStreamSink's advertised bound is the per-slot budget only while the largest INPUT "+
				"chunk fits inside it, and that holds by construction only while this lane and the compactor "+
				"name the SAME constant. Two literals that agree today drift tomorrow — that is the shape "+
				"item 128 closed for MaxChunkLineBytes.", name, l.file, sym, changeCeilingSymbol)
		}
	}

	// The consumer half: compaction's default budget must resolve to the same
	// symbol, in the function that resolves it.
	found := false
	for _, fn := range parsePkgDirWithPos(t, fset, filepath.Join(".", "backup")) {
		if fn.Name.Name != compactionBudgetFunc {
			continue
		}
		found = true
		bound := false
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, rhs := range as.Rhs {
				if exprSymbolName(rhs) == changeCeilingSymbol {
					bound = true
				}
			}
			return true
		})
		if !bound {
			t.Errorf("%s no longer resolves its default per-slot budget from %s.\n\n"+
				"The whole premise of chunkStreamSink's tight bound is that the budget and the writers' "+
				"ceiling are ONE number. If the budget is now independent, say so in that type's doc — the "+
				"bound it advertises is no longer the one it delivers.", compactionBudgetFunc, changeCeilingSymbol)
		}
	}
	if !found {
		t.Fatalf("%s is no longer declared in internal/pipeline/backup — this gate cannot see the budget it "+
			"grades and would pass vacuously", compactionBudgetFunc)
	}
}

// exprSymbolName renders the identifier an expression names — `X` or `pkg.X` —
// or a marker describing what it found instead. A literal deliberately renders
// as "a literal" rather than as its value, because the failure message's whole
// point is that a literal is not a shared symbol.
func exprSymbolName(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return v.Sel.Name
	case *ast.CallExpr:
		// A conversion such as int64(x) — grade what is inside it.
		if len(v.Args) == 1 {
			return exprSymbolName(v.Args[0])
		}
		return "a call"
	case *ast.BinaryExpr, *ast.BasicLit:
		return "a literal"
	}
	return "an unrecognised expression"
}

func laneCeilingNames(lanes map[string]*changeLaneCeiling) []string {
	out := make([]string, 0, len(lanes))
	for k := range lanes {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
