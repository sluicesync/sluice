// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Smart compaction's EMITTED stream was unbounded — roadmap item 130, the
// named residual of item 127.
//
// Item 127 capped what the compactor RETAINS. This file gates what it EMITS:
// the compactor used to append every emitted event to a slice it held for the
// whole incremental, and step 2 then packed all of it into `chunkBuckets[0]`,
// so the rewritten chunk was one unbounded blob however many chunks the
// incremental had. Peak was O(cap + output), and for an incremental with no
// collapse opportunity output ≈ input — which is the case item 127's cap did
// least for.
//
// SCOPE, stated where the gates are defined so they cannot be read as broader
// than they are:
//
//   - These reach `applySmartCompactionToIncremental`, the only path that
//     re-encodes change-chunk bytes. The naive compaction path moves chunks
//     verbatim (`copyFile`) and `backup prune` rewrites no bytes, so neither
//     buffers an emitted stream at all.
//   - The memory assertions are on `peakChunkBufferBytes`, a real accounting
//     hook on the writer's accepted bytes — NOT runtime.MemStats, which is far
//     too noisy to gate on.
//   - Every memory cell runs PLAINTEXT and ENCRYPTED. Encrypted is not a
//     formality here: [blobcodec.ChangeChunkWriter] buffers the whole
//     compressed chunk and GCM-encrypts it at Close, so bounding the
//     compactor's own slice while still packing every event into one chunk
//     would have been inert exactly where it matters. A plaintext-only gate
//     would have passed on a fix that achieved nothing for an encrypted chain.

package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// ----- fixtures -----

// noPKWideSchema is a table with NO declared primary key and a wide payload
// column. No PK means every event passes through uncollapsed, which is
// deliberately the worst case for the emitted term (output == input, exactly)
// AND the case that makes the output stream DURING the read phase rather than
// only at finalize — which is what exercises the write-behind cursor.
func noPKWideSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Schema: "public",
		Name:   "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "body", Type: ir.Text{}},
		},
	}}}
}

func wideInsert(id int64, width int) ir.Insert {
	return ir.Insert{
		Position: pos(uint64(1000 + id)),
		Schema:   "public",
		Table:    "events",
		Row:      ir.Row{"id": id, "body": strings.Repeat("x", width)},
	}
}

// testCEK is a deterministic content-encryption key for the encrypted arms.
func testCEK(t *testing.T) []byte {
	t.Helper()
	cek := make([]byte, crypto.CEKLen)
	for i := range cek {
		cek[i] = byte(i * 7)
	}
	return cek
}

// seedChangeChunks writes eventsPerChunk events into each of nChunks change
// chunks and returns the manifest describing them.
//
// The chunks come off the REAL [blobcodec.ChangeChunkWriter] with the same AAD
// binding the compaction path re-derives, so the fixture is not a hand-rolled
// approximation of the format under test.
func seedChangeChunks(
	t *testing.T,
	store irbackup.Store,
	schema *ir.Schema,
	cek []byte,
	nChunks, eventsPerChunk, width int,
) *irbackup.Manifest {
	t.Helper()
	im := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		SourceEngine:  "postgres",
		CreatedAt:     time.Now().UTC(),
		Kind:          irbackup.BackupKindIncremental,
		Schema:        schema,
	}
	for c := 0; c < nChunks; c++ {
		im.ChangeChunks = append(im.ChangeChunks, &irbackup.ChunkInfo{
			File: fmt.Sprintf("chunks/_changes/incr-%03d.jsonl.gz", c),
		})
		if cek != nil {
			im.ChangeChunks[c].Encryption = &irbackup.ChunkEncryption{
				Algorithm: crypto.AlgorithmAESGCM, NonceLen: 12, AuthTagLen: 16,
			}
		}
	}
	id := int64(0)
	for c, ch := range im.ChangeChunks {
		var buf bytes.Buffer
		w, err := blobcodec.NewChangeChunkWriter(&buf, cek, blobcodec.CodecGzip, irbackup.ChangeChunkAADFor(im, ch, c))
		if err != nil {
			t.Fatalf("seed chunk %d writer: %v", c, err)
		}
		for i := 0; i < eventsPerChunk; i++ {
			if err := w.WriteChange(wideInsert(id, width)); err != nil {
				t.Fatalf("seed chunk %d write: %v", c, err)
			}
			id++
		}
		if err := w.Close(); err != nil {
			t.Fatalf("seed chunk %d close: %v", c, err)
		}
		if err := store.Put(context.Background(), ch.File, bytes.NewReader(buf.Bytes())); err != nil {
			t.Fatalf("seed chunk %d put: %v", c, err)
		}
		ch.RowCount = w.ChangeCount()
		ch.SHA256 = w.Hash()
	}
	return im
}

// readAllChanges streams every change chunk of im back through the real reader,
// in manifest order — the independent view of what compaction actually wrote,
// as opposed to what the compactor's own tallies say it wrote.
func readAllChanges(t *testing.T, store irbackup.Store, im *irbackup.Manifest, cek []byte) []ir.Change {
	t.Helper()
	var out []ir.Change
	for i, ch := range im.ChangeChunks {
		rc, err := store.Get(context.Background(), ch.File)
		if err != nil {
			t.Fatalf("get chunk %q: %v", ch.File, err)
		}
		cr, err := blobcodec.NewChangeChunkReader(rc, ch.SHA256, cek, blobcodec.CodecGzip, irbackup.ChangeChunkAADFor(im, ch, i))
		if err != nil {
			t.Fatalf("open chunk %q: %v", ch.File, err)
		}
		for {
			c, err := cr.ReadChange()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("read chunk %q: %v", ch.File, err)
			}
			out = append(out, c)
		}
		if err := cr.Close(); err != nil {
			t.Fatalf("close chunk %q: %v", ch.File, err)
		}
	}
	return out
}

// renderChanges is a stable, readable rendering of a change stream, including
// each event's POSITION — order and identity both, which is what a comparison
// between two compaction runs has to cover.
func renderChanges(evs []ir.Change) []string {
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		switch ev := e.(type) {
		case ir.Insert:
			out = append(out, fmt.Sprintf("I %s %v len=%d", ev.Position.Token, ev.Row["id"], len(fmt.Sprint(ev.Row["body"]))))
		case ir.Update:
			out = append(out, fmt.Sprintf("U %s %v", ev.Position.Token, ev.After["id"]))
		case ir.Delete:
			out = append(out, fmt.Sprintf("D %s %v", ev.Position.Token, ev.Before["id"]))
		default:
			out = append(out, fmt.Sprintf("%T %s", e, e.Pos().Token))
		}
	}
	return out
}

// chunkOccupancy is the per-slot recorded change count — the distribution the
// budget produces, and the encrypted arm's stand-in for byte comparison.
func chunkOccupancy(im *irbackup.Manifest) []int64 {
	out := make([]int64, 0, len(im.ChangeChunks))
	for _, ch := range im.ChangeChunks {
		out = append(out, ch.RowCount)
	}
	return out
}

// openHandleStore refuses a Put to a path that currently has an open read
// handle, and records the violation.
//
// This is the in-memory stand-in for the constraint that makes item 130 hard:
// step 1 READS every staged chunk and step 3 WRITES back over the same paths,
// so streaming the output during the read phase can overwrite a staged input
// chunk that has not been read yet. On Windows a real store fails that loudly
// ("Access is denied" on a rename-replace over an open handle); on Linux it is
// silent input corruption, which is why this needs a gate rather than a
// platform.
type openHandleStore struct {
	irbackup.Store

	mu         sync.Mutex
	open       map[string]int
	violations []string
}

func newOpenHandleStore(inner irbackup.Store) *openHandleStore {
	return &openHandleStore{Store: inner, open: map[string]int{}}
}

func (s *openHandleStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	rc, err := s.Store.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.open[path]++
	s.mu.Unlock()
	return &trackedReadCloser{ReadCloser: rc, store: s, path: path}, nil
}

func (s *openHandleStore) Put(ctx context.Context, path string, r io.Reader) error {
	s.mu.Lock()
	held := s.open[path]
	if held > 0 {
		s.violations = append(s.violations, path)
		s.mu.Unlock()
		return fmt.Errorf("openHandleStore: Put(%q) while %d read handle(s) are open on it", path, held)
	}
	s.mu.Unlock()
	return s.Store.Put(ctx, path, r)
}

type trackedReadCloser struct {
	io.ReadCloser

	store  *openHandleStore
	path   string
	closed bool
}

func (t *trackedReadCloser) Close() error {
	if !t.closed {
		t.closed = true
		t.store.mu.Lock()
		t.store.open[t.path]--
		t.store.mu.Unlock()
	}
	return t.ReadCloser.Close()
}

// ----- the gates -----

// TestSmartCompactOutput_PeakChunkBufferStaysUnderTheBudget is the item-130
// gate, in the shape item 127 used: a measured ceiling plus an ANTI-VACUITY
// control that must blow past it.
//
// The independent expected value the ceiling is compared against is the SAME
// corpus run with the roll disabled — the pre-item-130 shape, which is the only
// thing that packs every event into the first chunk. Without that half a corpus
// too small to reach the budget would pass this test while proving nothing,
// which is the failure mode a memory gate is most prone to.
func TestSmartCompactOutput_PeakChunkBufferStaysUnderTheBudget(t *testing.T) {
	const (
		budget         = 64 << 10
		nChunks        = 24
		eventsPerChunk = 48
		width          = 1 << 10
		// One event's serialized size, generously over-estimated. The budget
		// is checked BEFORE each write, so a slot may overshoot by one event.
		slack = 4 << 10
	)

	for _, mode := range []struct {
		name string
		enc  bool
	}{{"plaintext", false}, {"encrypted", true}} {
		t.Run(mode.name, func(t *testing.T) {
			var cek []byte
			if mode.enc {
				cek = testCEK(t)
			}

			run := func(chunkBudget int64) (*smartCompactResult, []ir.Change, []int64) {
				store := newMemStore()
				im := seedChangeChunks(t, store, noPKWideSchema(), cek, nChunks, eventsPerChunk, width)
				res, err := applySmartCompactionToIncrementalSized(
					context.Background(), store, im, blobcodec.CodecGzip, cek, PKStrategyPK, chunkBudget,
				)
				if err != nil {
					t.Fatalf("compact (budget %d): %v", chunkBudget, err)
				}
				return res, readAllChanges(t, store, im, cek), chunkOccupancy(im)
			}

			budgeted, budgetedEvents, budgetedOccupancy := run(budget)
			unbounded, unboundedEvents, unboundedOccupancy := run(smartCompactUnboundedChunkBudget)

			if budgeted.peakChunkBufferBytes > budget+slack {
				t.Errorf("peak chunk buffer %d B exceeded the %d-byte budget (+%d slack).\n\n"+
					"This is the bound roadmap item 130 exists for: without it the emitted stream "+
					"is held for the whole incremental and the peak is O(output).",
					budgeted.peakChunkBufferBytes, budget, slack)
			}
			if unbounded.peakChunkBufferBytes <= budget+slack {
				t.Fatalf("the UNBOUNDED run peaked at %d B, at or below the %d-byte budget — the corpus "+
					"does not actually exercise the ceiling, so the assertion above proves nothing",
					unbounded.peakChunkBufferBytes, budget)
			}
			t.Logf("%s peak chunk buffer: budgeted %d B, unbounded %d B (%.1f×)",
				mode.name, budgeted.peakChunkBufferBytes, unbounded.peakChunkBufferBytes,
				float64(unbounded.peakChunkBufferBytes)/float64(budgeted.peakChunkBufferBytes))

			// The unbounded run is the old shape, and naming it here is what
			// makes the control specific rather than merely large: every event
			// in chunk 0, every other slot empty.
			if unboundedOccupancy[0] != int64(nChunks*eventsPerChunk) {
				t.Errorf("unbounded occupancy[0] = %d; want %d — the control is supposed to reproduce "+
					"the pre-item-130 shape exactly", unboundedOccupancy[0], nChunks*eventsPerChunk)
			}
			if budgetedOccupancy[0] == int64(nChunks*eventsPerChunk) {
				t.Errorf("the budgeted run also packed everything into chunk 0 (%v) — the budget did "+
					"not distribute anything", budgetedOccupancy)
			}

			// Distribution must not change the stream. This is the load-bearing
			// half: a bound that lost or reordered an event would be worse than
			// the unbounded memory it replaced.
			gotEvents, wantEvents := renderChanges(budgetedEvents), renderChanges(unboundedEvents)
			if len(gotEvents) != len(wantEvents) {
				t.Fatalf("budgeted run emitted %d events; unbounded emitted %d", len(gotEvents), len(wantEvents))
			}
			for i := range wantEvents {
				if gotEvents[i] != wantEvents[i] {
					t.Fatalf("event %d differs: budgeted %q, unbounded %q", i, gotEvents[i], wantEvents[i])
				}
			}
			if budgeted.eventsAfter != unbounded.eventsAfter {
				t.Errorf("eventsAfter = %d budgeted vs %d unbounded", budgeted.eventsAfter, unbounded.eventsAfter)
			}
			t.Logf("%s occupancy: budgeted %v", mode.name, budgetedOccupancy)
		})
	}
}

// TestSmartCompactOutput_BelowTheBudgetIsUnchanged is the other half of the
// contract, and the one an operator feels: an incremental that never reaches
// the budget must be rewritten EXACTLY as it was before the budget existed.
//
// Byte-identical for plaintext. For ENCRYPTED it cannot be byte-identical and
// saying so is the point: [blobcodec.ChangeChunkWriter] draws a fresh random
// GCM nonce per chunk, so two encryptions of the same plaintext differ by
// construction. The encrypted arm therefore compares everything that is not the
// nonce — the decrypted event stream and the per-slot occupancy — which is
// byte-identity modulo the one field that is required to vary.
func TestSmartCompactOutput_BelowTheBudgetIsUnchanged(t *testing.T) {
	for _, mode := range []struct {
		name string
		enc  bool
	}{{"plaintext", false}, {"encrypted", true}} {
		t.Run(mode.name, func(t *testing.T) {
			var cek []byte
			if mode.enc {
				cek = testCEK(t)
			}

			run := func(chunkBudget int64) (*memStore, *irbackup.Manifest) {
				store := newMemStore()
				im := seedChangeChunks(t, store, usersLikeSchema(), cek, 3, 4, 32)
				if _, err := applySmartCompactionToIncrementalSized(
					context.Background(), store, im, blobcodec.CodecGzip, cek, PKStrategyPK, chunkBudget,
				); err != nil {
					t.Fatalf("compact (budget %d): %v", chunkBudget, err)
				}
				return store, im
			}

			budgetedStore, budgetedIM := run(0) // 0 == DefaultBackupChunkBytes
			plainStore, plainIM := run(smartCompactUnboundedChunkBudget)

			gotOcc, wantOcc := chunkOccupancy(budgetedIM), chunkOccupancy(plainIM)
			if fmt.Sprint(gotOcc) != fmt.Sprint(wantOcc) {
				t.Errorf("occupancy changed under the budget: %v vs %v.\n\n"+
					"The budget must be invisible until it is reached — otherwise it rewrites every "+
					"existing chain differently, not just the ones using too much memory.", gotOcc, wantOcc)
			}
			gotEvents := renderChanges(readAllChanges(t, budgetedStore, budgetedIM, cek))
			wantEvents := renderChanges(readAllChanges(t, plainStore, plainIM, cek))
			if fmt.Sprint(gotEvents) != fmt.Sprint(wantEvents) {
				t.Errorf("event stream changed under the budget:\n budgeted: %v\n plain:    %v", gotEvents, wantEvents)
			}

			if mode.enc {
				// Byte comparison is meaningless here; assert that it is, so a
				// future reader does not "fix" this by adding one.
				if bytes.Equal(budgetedStore.data[budgetedIM.ChangeChunks[0].File], plainStore.data[plainIM.ChangeChunks[0].File]) {
					t.Error("two encryptions of the same chunk produced identical bytes — the GCM nonce " +
						"is supposed to be fresh per chunk; a deterministic nonce would be a real defect")
				}
				return
			}
			for i := range budgetedIM.ChangeChunks {
				a := budgetedStore.data[budgetedIM.ChangeChunks[i].File]
				b := plainStore.data[plainIM.ChangeChunks[i].File]
				if !bytes.Equal(a, b) {
					t.Errorf("chunk %d is not byte-identical under the budget (%d B vs %d B)", i, len(a), len(b))
				}
				if budgetedIM.ChangeChunks[i].SHA256 != plainIM.ChangeChunks[i].SHA256 {
					t.Errorf("chunk %d SHA differs under the budget", i)
				}
			}
		})
	}
}

// usersLikeSchema is a collapsible PK'd table, so the below-the-budget control
// exercises the ordinary collapse path rather than the passthrough one.
func usersLikeSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Schema: "public",
		Name:   "events",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "body", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}
}

// TestSmartCompactOutput_NeverWritesAChunkItHasNotRead pins the write-behind
// cursor, which is the property that lets the output stream in place at all.
//
// Without it, streaming output during the read phase overwrites staged input
// chunks step 1 has not reached — silently on Linux, loudly on Windows. The
// corpus is deliberately the passthrough (no-PK) shape with a budget far below
// one input chunk, so the sink is under constant pressure to roll early.
func TestSmartCompactOutput_NeverWritesAChunkItHasNotRead(t *testing.T) {
	for _, mode := range []struct {
		name string
		enc  bool
	}{{"plaintext", false}, {"encrypted", true}} {
		t.Run(mode.name, func(t *testing.T) {
			var cek []byte
			if mode.enc {
				cek = testCEK(t)
			}
			inner := newMemStore()
			store := newOpenHandleStore(inner)
			im := seedChangeChunks(t, store, noPKWideSchema(), cek, 12, 32, 1<<10)

			if _, err := applySmartCompactionToIncrementalSized(
				context.Background(), store, im, blobcodec.CodecGzip, cek, PKStrategyPK, 1<<10,
			); err != nil {
				t.Fatalf("compact: %v", err)
			}
			if len(store.violations) > 0 {
				t.Fatalf("compaction wrote %d chunk(s) while their staged input was still open: %v.\n\n"+
					"Output slot i may only be written once input chunk i has been fully read and "+
					"closed — that write-behind rule is what makes the in-place rewrite safe.",
					len(store.violations), store.violations)
			}
			if got := len(readAllChanges(t, store, im, cek)); got != 12*32 {
				t.Errorf("read back %d events; want %d", got, 12*32)
			}
		})
	}
}

// TestSmartCompactOutput_PreCeilingChainDegradesToOneChunk is the honest half
// of the bound, named rather than assumed.
//
// The write-behind rule means a slot cannot be written early even when it is
// full, so the emitted bytes for a STILL-OPEN input chunk pile into the current
// slot. For a chain written by a binary that caps a change chunk at
// [DefaultBackupChunkBytes] that is bounded by the budget. For an OLDER chain
// whose chunks predate that ceiling it is bounded by the chain's largest chunk
// instead — still O(one chunk) rather than O(the incremental), and larger than
// the budget advertises.
//
// This pins BOTH halves of the degraded case: the peak really does exceed the
// budget (so the residual is not imaginary), and nothing is lost when the
// output needs more slots than the incremental has — the last slot absorbs the
// remainder rather than the events being dropped or the compaction refusing.
func TestSmartCompactOutput_PreCeilingChainDegradesToOneChunk(t *testing.T) {
	const (
		budget         = 4 << 10
		nChunks        = 4
		eventsPerChunk = 40
		width          = 1 << 10
	)
	run := func(chunkBudget int64) (*memStore, *irbackup.Manifest, *smartCompactResult) {
		store := newMemStore()
		im := seedChangeChunks(t, store, noPKWideSchema(), nil, nChunks, eventsPerChunk, width)
		res, err := applySmartCompactionToIncrementalSized(
			context.Background(), store, im, blobcodec.CodecGzip, nil, PKStrategyPK, chunkBudget,
		)
		if err != nil {
			t.Fatalf("compact (budget %d): %v", chunkBudget, err)
		}
		return store, im, res
	}
	store, im, res := run(budget)
	_, _, unbounded := run(smartCompactUnboundedChunkBudget)

	if res.peakChunkBufferBytes <= budget {
		t.Fatalf("peak chunk buffer was %d B, within the %d-byte budget — this corpus is supposed to "+
			"REACH the degraded case (an input chunk far larger than the budget), so it proves nothing "+
			"about it", res.peakChunkBufferBytes, budget)
	}
	// The independent expected value: the SAME corpus with no roll at all,
	// which buffers the whole incremental. The degraded bound is one chunk of
	// four, so it must come in near a quarter of that and nowhere near it.
	if res.peakChunkBufferBytes*2 >= unbounded.peakChunkBufferBytes {
		t.Errorf("degraded peak %d B is not comfortably below the whole-incremental peak %d B — the "+
			"degraded bound is supposed to be ONE chunk, not the incremental",
			res.peakChunkBufferBytes, unbounded.peakChunkBufferBytes)
	}
	t.Logf("degraded peak: %d B against a %d-byte budget; whole-incremental peak %d B (%d chunks)",
		res.peakChunkBufferBytes, budget, unbounded.peakChunkBufferBytes, nChunks)

	// And nothing was lost when the output outgrew its slots.
	got := readAllChanges(t, store, im, nil)
	if len(got) != nChunks*eventsPerChunk {
		t.Fatalf("read back %d events; want %d — the last slot must absorb the remainder rather than "+
			"the events going missing", len(got), nChunks*eventsPerChunk)
	}
	for i, e := range got {
		ins, ok := e.(ir.Insert)
		if !ok {
			t.Fatalf("event %d is %T; want ir.Insert", i, e)
		}
		if ins.Row["id"] != int64(i) {
			t.Fatalf("event %d carries id %v; want %d — order must survive the overflow", i, ins.Row["id"], i)
		}
	}
}

// TestSmartCompactOutput_ClosingCommitStaysLastAcrossEveryFlusher is the F3
// invariant, generalised from the one flusher that had it to all of them.
//
// A collapsed event carries its chain's FIRST position, so one emitted after
// the incremental's closing TxCommit leaves the stream ending BELOW its own
// recorded EndPosition — which chain_restore's F1 backstop refuses as
// truncated (roadmap item 101). `flushCollapsedInsideClosingTx` enforced that
// at FINALIZE; item 127's eviction path got out from under it by appending to
// the same slice and had to grow its own splice.
//
// Two more flushers were in the same position and had NOT been fixed: the
// PK-change barrier (audit A-3) and the DDL barrier both appended collapsed
// events after a trailing commit. With the output streaming there is no slice
// to splice, so the one-slot lookahead in [smartCompactor.pushCollapsed] is the
// enforcement, for every flusher at once. This pins all three.
func TestSmartCompactOutput_ClosingCommitStaysLastAcrossEveryFlusher(t *testing.T) {
	commit := ir.TxCommit{Position: pos(500)}

	cases := []struct {
		name   string
		schema *ir.Schema
		// trailing is appended after the closing commit; the pending chain
		// below it started INSIDE the transaction, so its collapsed event
		// carries a position below the commit's.
		trailing []ir.Change
	}{
		{
			name:   "pk-change barrier",
			schema: usersSchema(),
			trailing: []ir.Change{
				// Re-keys row 1 → 2, which flushes both keys' chains.
				ir.Update{
					Position: pos(600), Schema: "public", Table: "users",
					Before: ir.Row{"id": int64(1), "name": "a"},
					After:  ir.Row{"id": int64(2), "name": "a"},
				},
			},
		},
		{
			name:     "ddl barrier",
			schema:   usersSchema(),
			trailing: []ir.Change{ir.SchemaSnapshot{Position: pos(600)}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSmartCompactor(PKStrategyPK, tc.schema)
			corpus := []ir.Change{
				ir.TxBegin{Position: pos(100)},
				ir.Insert{Position: pos(110), Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "name": "a"}},
				ir.Update{
					Position: pos(120), Schema: "public", Table: "users",
					Before: ir.Row{"id": int64(1), "name": "a"},
					After:  ir.Row{"id": int64(1), "name": "b"},
				},
				commit,
			}
			corpus = append(corpus, tc.trailing...)
			for _, e := range corpus {
				if err := s.process(e); err != nil {
					t.Fatalf("process: %v", err)
				}
			}
			out, _ := finalizeCollected(t, s)

			commitAt := -1
			for i, e := range out {
				if _, ok := e.(ir.TxCommit); ok {
					commitAt = i
				}
			}
			if commitAt < 0 {
				t.Fatalf("the closing TxCommit is missing from the output: %v", kindsOf(out))
			}
			for i := commitAt + 1; i < len(out); i++ {
				if !isPerRowEvent(out[i]) {
					continue
				}
				if lsnOf(t, out[i].Pos()) < lsnOf(t, commit.Position) {
					t.Fatalf("event %d (%T at %s) sits AFTER the closing commit at %s but carries an "+
						"EARLIER position.\n\nA stream ending below its own recorded EndPosition is what "+
						"chain_restore's F1 backstop refuses as truncated — roadmap item 101, reachable "+
						"through every flusher, not just eviction.",
						i, out[i], out[i].Pos().Token, commit.Position.Token)
				}
			}
		})
	}
}

// lsnOf parses the numeric LSN back out of a bookmark token built by [lsnToken].
// Parsed rather than string-compared: the token renders the LSN in hex without
// leading zeros, so a lexicographic compare is wrong across digit widths — the
// exact shape of mistake that would make this gate silently vacuous.
func lsnOf(t *testing.T, p ir.Position) uint64 {
	t.Helper()
	const marker = `"lsn":"0/`
	i := strings.Index(p.Token, marker)
	if i < 0 {
		t.Fatalf("token %q has no lsn field", p.Token)
	}
	rest := p.Token[i+len(marker):]
	j := strings.IndexByte(rest, '"')
	if j < 0 {
		t.Fatalf("token %q has an unterminated lsn field", p.Token)
	}
	var v uint64
	if _, err := fmt.Sscanf(rest[:j], "%X", &v); err != nil {
		t.Fatalf("token %q: parse lsn %q: %v", p.Token, rest[:j], err)
	}
	return v
}
