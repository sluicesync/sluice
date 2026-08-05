// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// `--smart-compaction` buffered an ENTIRE incremental in RAM (roadmap item 127).
//
// The compactor's only accumulator barriers are TRUNCATE, SchemaSnapshot and
// end-of-incremental, so an incremental carrying none of those retained every
// event it had read. Measured by the 2026-08-04 audit: 100k events → 261 MiB,
// and a 100:1-COLLAPSING corpus → 248 MiB — i.e. no relief from collapsing,
// because the inputs are held until flush however well they collapse.
//
// These tests gate the retained-INPUT term ONLY, and the name is narrower than
// "retention" on purpose. The EMITTED term — the other structure the audit
// named — is a separate bound with its own gates in
// chain_compact_smart_output_test.go (roadmap item 130); nothing here says
// anything about it.

package backup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// retentionSchema is a two-column table with a wide payload column — the shape
// that reaches a byte ceiling on a plausible row count rather than a synthetic
// one.
func retentionSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "wide",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "body", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}
}

func retentionInsert(id int64, width int) ir.Insert {
	return ir.Insert{
		Position: ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"lsn":"0/%d"}`, id)},
		Table:    "wide",
		Row:      ir.Row{"id": id, "body": strings.Repeat("x", width)},
	}
}

func retentionUpdate(id int64, width int, gen string) ir.Update {
	return ir.Update{
		Position: ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"lsn":"0/%d"}`, id)},
		Table:    "wide",
		Before:   ir.Row{"id": id},
		After:    ir.Row{"id": id, "body": strings.Repeat(gen, width)},
	}
}

// TestSmartCompactRetention_PeakStaysUnderTheCap is the item-127 gate: drive
// the real compactor with wide events and assert peak retention stays bounded.
//
// The control that keeps it from being vacuous is the second half — the SAME
// corpus with the cap disabled must blow well past the cap. Without that, a
// corpus too small to reach the ceiling would pass this test while proving
// nothing, which is the failure mode a memory gate is most prone to.
func TestSmartCompactRetention_PeakStaysUnderTheCap(t *testing.T) {
	const (
		cap0   = 256 << 10 // 256 KiB — small enough to reach quickly
		rows   = 400
		width  = 4 << 10 // 4 KiB payload per event
		perPK  = 3       // three events per row: a genuinely collapsible corpus
		expect = rows * perPK
	)

	build := func() []ir.Change {
		out := make([]ir.Change, 0, expect)
		for gen := 0; gen < perPK; gen++ {
			for id := int64(0); id < rows; id++ {
				if gen == 0 {
					out = append(out, retentionInsert(id, width))
					continue
				}
				out = append(out, retentionUpdate(id, width, string(rune('a'+gen))))
			}
		}
		return out
	}

	capped := newSmartCompactor(PKStrategyPK, retentionSchema())
	capped.maxRetainedBytes = cap0
	for _, e := range build() {
		if err := capped.process(e); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	cappedOut, cappedRes := finalizeCollected(t, capped)

	if capped.peakRetainedBytes > cap0 {
		t.Errorf("peak retention %d bytes exceeded the %d-byte cap.\n\n"+
			"The cap is what stops an incremental with no TRUNCATE and no DDL from retaining "+
			"itself entirely (item 127).", capped.peakRetainedBytes, cap0)
	}
	if cappedRes.chainsEvicted == 0 {
		t.Error("no chains were evicted, so the cap was never reached and this test is vacuous — " +
			"the corpus must be big enough to exercise the ceiling")
	}

	// The anti-vacuity control: uncapped, the same corpus retains far more.
	uncapped := newSmartCompactor(PKStrategyPK, retentionSchema())
	uncapped.maxRetainedBytes = 0 // disabled
	for _, e := range build() {
		if err := uncapped.process(e); err != nil {
			t.Fatalf("process (uncapped): %v", err)
		}
	}
	uncappedOut, uncappedRes := finalizeCollected(t, uncapped)

	if uncapped.peakRetainedBytes <= cap0 {
		t.Fatalf("uncapped peak retention was %d bytes, at or below the %d-byte cap — the corpus "+
			"does not actually exercise the ceiling, so the capped assertion above proves nothing",
			uncapped.peakRetainedBytes, cap0)
	}
	if uncappedRes.chainsEvicted != 0 {
		t.Errorf("uncapped run evicted %d chains; the cap was supposed to be disabled", uncappedRes.chainsEvicted)
	}
	t.Logf("peak retained: capped %d B, uncapped %d B (%.1f×); chains evicted %d",
		capped.peakRetainedBytes, uncapped.peakRetainedBytes,
		float64(uncapped.peakRetainedBytes)/float64(capped.peakRetainedBytes), cappedRes.chainsEvicted)

	// Eviction costs collapse, never correctness: the capped run emits at
	// least as many events as the uncapped one (some chains collapsed in two
	// groups instead of one) and both end at the same row state.
	if len(cappedOut) < len(uncappedOut) {
		t.Errorf("capped run emitted FEWER events (%d) than uncapped (%d) — eviction must lose "+
			"collapse, never events", len(cappedOut), len(uncappedOut))
	}
	assertSameFinalRowState(t, cappedOut, uncappedOut)
}

// The property that makes eviction safe to do at all: whatever the cap costs
// in collapse, the row state a restore would land on is identical.
func assertSameFinalRowState(t *testing.T, got, want []ir.Change) {
	t.Helper()
	replay := func(evs []ir.Change) map[string]ir.Row {
		state := map[string]ir.Row{}
		for _, e := range evs {
			switch ev := e.(type) {
			case ir.Insert:
				state[fmt.Sprint(ev.Row["id"])] = ev.Row
			case ir.Update:
				k := fmt.Sprint(ev.After["id"])
				state[k] = mergeAfterImage(state[k], ev.After)
			case ir.Delete:
				delete(state, fmt.Sprint(ev.Before["id"]))
			}
		}
		return state
	}
	g, w := replay(got), replay(want)
	if len(g) != len(w) {
		t.Fatalf("final row state has %d rows; want %d", len(g), len(w))
	}
	for k, wr := range w {
		gr, ok := g[k]
		if !ok {
			t.Fatalf("row %s missing from the capped run's final state", k)
		}
		for col, wv := range wr {
			if fmt.Sprint(gr[col]) != fmt.Sprint(wv) {
				t.Fatalf("row %s column %q is %v after the capped run; want %v", k, col, gr[col], wv)
			}
		}
	}
}

// The other half of the contract, and the one an operator feels: a corpus that
// never reaches the ceiling must collapse EXACTLY as it did before the cap
// existed. Byte-identical, not merely equivalent.
func TestSmartCompactRetention_BelowTheCapOutputIsUnchanged(t *testing.T) {
	corpus := []ir.Change{
		ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"lsn":"0/1"}`}},
		retentionInsert(1, 32),
		retentionUpdate(1, 32, "b"),
		retentionInsert(2, 32),
		retentionUpdate(2, 32, "c"),
		retentionUpdate(1, 32, "d"),
		ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"lsn":"0/9"}`}},
	}

	run := func(capBytes int64) []ir.Change {
		s := newSmartCompactor(PKStrategyPK, retentionSchema())
		s.maxRetainedBytes = capBytes
		for _, e := range corpus {
			if err := s.process(e); err != nil {
				t.Fatalf("process: %v", err)
			}
		}
		out, res := finalizeCollected(t, s)
		if res.chainsEvicted != 0 {
			t.Fatalf("cap %d evicted %d chains; this corpus must stay under every cap tested",
				capBytes, res.chainsEvicted)
		}
		return out
	}

	withCap := run(defaultMaxRetainedBytes)
	without := run(0)

	a, err := json.Marshal(summariseChanges(withCap))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	b, err := json.Marshal(summariseChanges(without))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Errorf("a corpus below the ceiling collapsed DIFFERENTLY with the cap enabled.\n\n"+
			"The cap must be invisible until it is reached — otherwise it changes every existing "+
			"chain's output, not just the ones that were using too much memory.\n with cap: %s\n"+
			"without: %s", a, b)
	}
}

// summariseChanges renders an event stream in a form that is stable to compare
// and readable when it differs.
func summariseChanges(evs []ir.Change) []string {
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		switch ev := e.(type) {
		case ir.Insert:
			out = append(out, fmt.Sprintf("insert %v %v", ev.Row["id"], ev.Row["body"]))
		case ir.Update:
			out = append(out, fmt.Sprintf("update %v %v", ev.After["id"], ev.After["body"]))
		case ir.Delete:
			out = append(out, fmt.Sprintf("delete %v", ev.Before["id"]))
		default:
			out = append(out, fmt.Sprintf("%T", e))
		}
	}
	return out
}

// A corpus with NO collapse opportunity is the worst case for retention, and
// it is also the case where eviction should cost nothing at all: every chain
// is a single event, so draining it early emits exactly what finalize would
// have emitted, in exactly the order flushAll would have used.
//
// This pins the eviction ORDER policy, not just the ceiling. If single-event
// chains stopped being evicted first, this corpus would start reordering.
func TestSmartCompactRetention_NoCollapseCorpusIsUnreordered(t *testing.T) {
	const (
		cap0 = 64 << 10
		rows = 300
	)
	// Widths must VARY, or this test cannot tell the two policies apart: with
	// every chain the same size, sort.SliceStable leaves them in insertion
	// order and a largest-first eviction is indistinguishable from the correct
	// one. Caught by mutating the policy and watching this pass.
	width := func(id int64) int { return 1<<10 + int(id%7)*512 }

	corpus := make([]ir.Change, 0, rows)
	for id := int64(0); id < rows; id++ {
		corpus = append(corpus, retentionInsert(id, width(id)))
	}

	s := newSmartCompactor(PKStrategyPK, retentionSchema())
	s.maxRetainedBytes = cap0
	for _, e := range corpus {
		if err := s.process(e); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	out, res := finalizeCollected(t, s)

	if res.chainsEvicted == 0 {
		t.Fatal("no eviction happened, so this test proves nothing about eviction order")
	}
	if s.peakRetainedBytes > cap0 {
		t.Errorf("peak retention %d exceeded cap %d", s.peakRetainedBytes, cap0)
	}
	if len(out) != rows {
		t.Fatalf("emitted %d events; want %d — a single-event chain must pass through verbatim", len(out), rows)
	}
	for i, e := range out {
		ins, ok := e.(ir.Insert)
		if !ok {
			t.Fatalf("event %d is %T; want ir.Insert", i, e)
		}
		if got := ins.Row["id"]; got != int64(i) {
			t.Fatalf("event %d carries id %v; want %d.\n\n"+
				"Single-event chains must be evicted in INSERTION order, which is the order "+
				"flushAll would have emitted them in. Evicting largest-first here would reorder "+
				"a corpus the cap is supposed to leave alone.", i, got, i)
		}
	}
}

// The F3 invariant under eviction — roadmap item 101's failure mode, reachable
// again the moment chains start draining mid-pass.
//
// A collapsed event carries its chain's FIRST position, so if one is emitted
// after the incremental's closing TxCommit the rewritten stream ENDS BELOW its
// own recorded EndPosition, and chain_restore.go's F1 backstop refuses the
// chain as truncated (SLUICE-E-BACKUP-INCOMPLETE). `flushCollapsedInsideClosingTx`
// enforces that at finalize; eviction has to preserve it throughout.
//
// The shape that reaches it: an incremental whose window ends mid-transaction,
// so row events follow the last TxCommit. A naive append during eviction would
// put a collapsed row event after the closing commit, finalize would see a
// non-TxCommit tail and take the plain flushAll path, and the stream would end
// on a row event.
func TestSmartCompactRetention_EvictionKeepsTheClosingCommitLast(t *testing.T) {
	const cap0 = 32 << 10

	corpus := []ir.Change{
		ir.TxBegin{Position: ir.Position{Engine: "postgres", Token: `{"lsn":"0/1"}`}},
		retentionInsert(1, 1<<10),
		ir.TxCommit{Position: ir.Position{Engine: "postgres", Token: `{"lsn":"0/500"}`}},
	}
	// Rows AFTER the closing commit, enough of them to force eviction.
	for id := int64(2); id < 120; id++ {
		corpus = append(corpus, retentionInsert(id, 1<<10))
	}

	s := newSmartCompactor(PKStrategyPK, retentionSchema())
	s.maxRetainedBytes = cap0
	for _, e := range corpus {
		if err := s.process(e); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	out, res := finalizeCollected(t, s)

	if res.chainsEvicted == 0 {
		t.Fatal("no eviction happened, so this test proves nothing about the closing commit")
	}
	if len(out) == 0 {
		t.Fatal("no output")
	}
	if _, ok := out[len(out)-1].(ir.TxCommit); !ok {
		t.Errorf("the rewritten stream ends on a %T, not the closing TxCommit.\n\n"+
			"A collapsed event carries its chain's FIRST position, so a stream ending on one ends "+
			"BELOW its own recorded EndPosition — chain_restore's F1 backstop refuses that chain as "+
			"truncated. This is roadmap item 101, reachable again through eviction.", out[len(out)-1])
	}
}

// TestEstimateChangeBytes_TracksRealMarshalledSize is the independent expected
// value the retention cap is otherwise missing.
//
// The cap is enforced on an ESTIMATE, so a gate that only checked the estimate
// against itself would be the shared-evidence shape this project keeps paying
// for: it would confirm the compactor agrees with its own arithmetic while
// saying nothing about whether the arithmetic tracks reality. The independent
// number is what the event actually serializes to, and it is cheaply available
// — encoding/json will tell us.
func TestEstimateChangeBytes_TracksRealMarshalledSize(t *testing.T) {
	cases := []struct {
		name string
		ev   ir.Change
	}{
		{"narrow insert", retentionInsert(1, 8)},
		{"wide insert", retentionInsert(2, 64<<10)},
		{"narrow update", retentionUpdate(3, 8, "a")},
		{"wide update", retentionUpdate(4, 64<<10, "b")},
		{"delete", ir.Delete{Table: "wide", Before: ir.Row{"id": int64(5)}}},
		{"bytes payload", ir.Insert{
			Table: "wide",
			Row:   ir.Row{"id": int64(6), "body": make([]byte, 32<<10)},
		}},
		{"null payload", ir.Insert{Table: "wide", Row: ir.Row{"id": int64(7), "body": nil}}},
		{"many narrow columns", func() ir.Change {
			r := ir.Row{}
			for i := 0; i < 64; i++ {
				r[fmt.Sprintf("col_%02d", i)] = int64(i)
			}
			return ir.Insert{Table: "wide", Row: r}
		}()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.ev)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			marshalled := int64(len(b))
			est := estimateChangeBytes(tc.ev)

			// The estimate must stay within an order of magnitude of the
			// truth in BOTH directions. Too low and the cap admits far more
			// than it claims; too high and it evicts chains that were costing
			// nothing. The band is deliberately loose — this is a memory
			// heuristic, not a length prefix — but it is not unbounded, which
			// is the property that makes the cap mean something.
			if est*8 < marshalled {
				t.Errorf("estimate %d is more than 8× BELOW the real marshalled size %d — the cap "+
					"would admit far more than it advertises", est, marshalled)
			}
			if est > marshalled*8+512 {
				t.Errorf("estimate %d is more than 8× ABOVE the real marshalled size %d — the cap "+
					"would evict chains that were costing nothing", est, marshalled)
			}
		})
	}
}
