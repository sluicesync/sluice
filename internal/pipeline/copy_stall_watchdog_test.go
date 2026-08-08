// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"go/ast"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/progress"
)

// The item-146 watchdog's three-way contract. All three cases matter and the
// last two are what keep the first from being noise:
//
//	1. a STALLED copy is reported,
//	2. a legitimately SLOW-but-progressing copy is not,
//	3. a deliberate ADR-0110 grow-gate quiesce is not.
//
// Every case here carries its own bound. A test that reproduced a hang by
// hanging would not be a test.

// capturingSink records what the watchdog emits. progress.Sink is documented
// safe for concurrent use, and the watchdog emits from its own goroutine.
type capturingSink struct {
	progress.Nop

	mu    sync.Mutex
	warns []string
}

func (s *capturingSink) Warn(msg string, attrs ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.warns = append(s.warns, msg+fmt.Sprint(attrs...))
}

func (s *capturingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.warns)
}

func (s *capturingSink) first() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.warns) == 0 {
		return ""
	}
	return s.warns[0]
}

// waitFor polls until cond or the bound elapses. The bound is the test's own
// guarantee of termination.
func waitFor(t *testing.T, bound time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func TestCopyStallWatchdog_ReportsAStalledCopy(t *testing.T) {
	sink := &capturingSink{}
	var rows atomic.Int64
	rows.Store(1000) // some rows landed, then everything stopped

	w := startCopyStallWatchdog(context.Background(), &copyStallWatchdog{
		table:     "users",
		chunk:     3,
		hasChunk:  true,
		units:     "rows",
		progress:  rows.Load,
		warnAfter: 40 * time.Millisecond,
		poll:      4 * time.Millisecond,
		sink:      sink,
	})
	defer w.Stop()

	if !waitFor(t, 5*time.Second, func() bool { return sink.count() > 0 }) {
		t.Fatal("a copy that made no progress at all was never reported — this is the Bug 228 / 138 / 229 silence")
	}
	got := sink.first()
	for _, want := range []string{"NO forward progress", "users", "chunk", "progress_rows"} {
		if !strings.Contains(got, want) {
			t.Errorf("warning does not name %q; an operator has to be able to tell WHICH lane stalled.\ngot: %s", want, got)
		}
	}
	if !strings.Contains(got, "will not abort") {
		t.Errorf("warning does not say sluice keeps waiting; an operator reading it must know whether the run died.\ngot: %s", got)
	}
}

// The re-warn escalates instead of repeating at a fixed interval: a wedged
// overnight run must not emit a line every warnAfter, because a flood is how
// a warning gets filtered and a filtered watchdog catches nothing.
func TestCopyStallWatchdog_ReWarnsOnAWideningInterval(t *testing.T) {
	sink := &capturingSink{}
	var rows atomic.Int64

	w := startCopyStallWatchdog(context.Background(), &copyStallWatchdog{
		table:     "events",
		chunk:     -1,
		units:     "rows",
		progress:  rows.Load,
		warnAfter: 20 * time.Millisecond,
		poll:      2 * time.Millisecond,
		sink:      sink,
	})
	defer w.Stop()

	// In 10 warnAfter-widths a FIXED-interval watchdog emits ~10 warnings; the
	// escalating one emits ~3 (at 1×, 3×, 7×).
	time.Sleep(200 * time.Millisecond)
	if n := sink.count(); n == 0 {
		t.Fatal("no warning at all in 10 threshold-widths")
	} else if n > 6 {
		t.Errorf("emitted %d warnings in 10 threshold-widths; the re-warn interval is not widening — that is the log "+
			"flood that gets the watchdog filtered", n)
	}
}

// The false-positive half, and the one that keeps this from being noise: a
// copy that is SLOW but advancing must be silent. The watchdog reports
// STOPPED, never SLOW.
func TestCopyStallWatchdog_SilentOnASlowButProgressingCopy(t *testing.T) {
	sink := &capturingSink{}
	var rows atomic.Int64

	w := startCopyStallWatchdog(context.Background(), &copyStallWatchdog{
		table:     "big_table",
		chunk:     -1,
		units:     "rows",
		progress:  rows.Load,
		warnAfter: 60 * time.Millisecond,
		poll:      3 * time.Millisecond,
		sink:      sink,
	})
	defer w.Stop()

	// One row every ~40ms: far slower than the poll, and slower relative to
	// the threshold than any real copy — but never STOPPED.
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		rows.Add(1)
		time.Sleep(40 * time.Millisecond)
	}
	if n := sink.count(); n != 0 {
		t.Fatalf("warned %d time(s) on a copy that was advancing the whole time (%s). A watchdog that fires on SLOW "+
			"rather than STOPPED cries wolf, gets suppressed, and then catches nothing", n, sink.first())
	}
}

// The third case, and the one the roadmap entry singles out: an ADR-0110
// grow-gate window stops every lane ON PURPOSE for up to 20 minutes. It must
// not be reported. Driven through the REAL migcore.GrowGate, not a stub that
// would satisfy the interface by construction.
func TestCopyStallWatchdog_SilentDuringADeliberateGrowGateQuiesce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Hold the window open for the whole test: long backoff, no early reopen.
	// BEFORE the gate is constructed — NewGrowGate snapshots the timing
	// envelope onto the instance (the -race safety property), so a later
	// override would not reach this gate.
	restore := setGrowGateWindowForTest(t, time.Minute)
	defer restore()
	gate := migcore.NewGrowGate(ctx, nil)

	sink := &capturingSink{}
	var rows atomic.Int64

	qctx := withCopyQuiesceSource(ctx, migcore.GrowGateOrNil(gate))
	if copyQuiesceSourceFromContext(qctx) == nil {
		t.Fatal("the grow gate did not bind as a quiesce source; the watchdog would report every ADR-0110 window as a stall")
	}

	gate.Trip("test: simulated target storage grow", ir.GrowEvidenceNone)

	w := startCopyStallWatchdog(qctx, &copyStallWatchdog{
		table:     "orders",
		chunk:     -1,
		units:     "rows",
		progress:  rows.Load,
		warnAfter: 30 * time.Millisecond,
		poll:      3 * time.Millisecond,
		sink:      sink,
	})
	defer w.Stop()

	// Ten threshold-widths of a fully quiesced, zero-progress copy.
	time.Sleep(300 * time.Millisecond)
	if n := sink.count(); n != 0 {
		t.Fatalf("warned %d time(s) during a deliberate grow-gate quiesce (%s). A watchdog that cries wolf on every "+
			"normal grow pause gets suppressed, and then catches nothing", n, sink.first())
	}

	// And the other direction, which is what proves the exclusion is scoped to
	// the pause rather than being a permanent mute: once the gate reopens and
	// the copy STILL does not move, the stall is reported.
	cancel() // unwinds the gate's owner goroutine → finishWindow reopens it
	if !waitFor(t, 5*time.Second, func() bool { return !gate.QuiescedSince(time.Now()) }) {
		t.Fatal("gate never reopened after ctx cancel")
	}
	post := &capturingSink{}
	w2 := startCopyStallWatchdog(context.Background(), &copyStallWatchdog{
		table:     "orders",
		chunk:     -1,
		units:     "rows",
		progress:  rows.Load,
		quiesced:  gate.QuiescedSince,
		warnAfter: 30 * time.Millisecond,
		poll:      3 * time.Millisecond,
		sink:      post,
	})
	defer w2.Stop()
	if !waitFor(t, 5*time.Second, func() bool { return post.count() > 0 }) {
		t.Fatal("a stall AFTER the grow-gate window reopened was not reported — the quiesce exclusion has become a permanent mute")
	}
}

// QuiescedSince is the seam the exclusion above rests on; pin its three
// answers directly so a regression there is attributed here rather than
// showing up as an unexplained silent watchdog.
func TestGrowGateQuiescedSince_AnswersTheThreeCases(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	restore := setGrowGateWindowForTest(t, time.Minute)
	defer restore()

	gate := migcore.NewGrowGate(ctx, nil)
	var _ ir.GrowGateQuiesceObserver = gate

	if gate.QuiescedSince(time.Now().Add(-time.Hour)) {
		t.Error("a gate that never closed reports quiesced")
	}
	gate.Trip("test", ir.GrowEvidenceNone)
	if !gate.QuiescedSince(time.Now()) {
		t.Error("a CLOSED gate does not report quiesced — every grow window would be read as a stall")
	}
	cancel()
	if !waitFor(t, 5*time.Second, func() bool { return !gate.QuiescedSince(time.Now()) }) {
		t.Fatal("gate never reopened")
	}
	// The window is in the past now: an interval that CONTAINS it still
	// reports quiesced (that is the "was any of this idle time deliberate?"
	// question), an interval that starts after it does not.
	if !gate.QuiescedSince(time.Now().Add(-time.Hour)) {
		t.Error("an interval containing a closed-then-reopened window does not report quiesced")
	}
	if gate.QuiescedSince(time.Now().Add(time.Hour)) {
		t.Error("an interval starting AFTER the last reopen reports quiesced — the exclusion would never expire")
	}
}

// setGrowGateWindowForTest widens the gate's per-window hold so a tripped
// window stays closed for the length of a test. Restores on cleanup.
func setGrowGateWindowForTest(t *testing.T, hold time.Duration) func() {
	t.Helper()
	base, cap0, maxHold := migcore.GrowGateBackoffBase, migcore.GrowGateBackoffCap, migcore.GrowGateMaxHold
	evFree := migcore.GrowGateEvidenceFreeHoldCap
	migcore.GrowGateBackoffBase, migcore.GrowGateBackoffCap, migcore.GrowGateMaxHold = hold, hold, hold
	// The evidence-free cap (item 157) is a fourth bound on the hold, so this
	// helper has to move it too or its name stops being true: without this the
	// caller below asks for a one-minute window, trips with GrowEvidenceNone,
	// and silently gets a 250ms one. Setting it here keeps the watchdog test
	// exercising the EVIDENCE-FREE path, which is the stronger property — the
	// exclusion must hold for every deliberate quiesce, not just evidenced ones.
	migcore.GrowGateEvidenceFreeHoldCap = hold
	return func() {
		migcore.GrowGateBackoffBase, migcore.GrowGateBackoffCap, migcore.GrowGateMaxHold = base, cap0, maxHold
		migcore.GrowGateEvidenceFreeHoldCap = evFree
	}
}

// The wiring, end to end through the REAL ticker constructor: a lane built
// the way every copy path builds one, left idle, warns. The lane roster below
// is a structural check; this is the behavioural one behind it, and without
// it the roster would be proving that a call exists rather than that it works.
func TestProgressTicker_CarriesALiveStallWatchdog(t *testing.T) {
	orig := copyStallWarnAfter
	copyStallWarnAfter = 40 * time.Millisecond
	defer func() { copyStallWarnAfter = orig }()

	sink := &capturingSink{}
	ctx := progress.NewContext(context.Background(), sink)

	pt := newProgressTickerForChunk(ctx, time.Hour, "invoices", 2)
	defer pt.Stop(ctx, nil)

	if pt.stall == nil {
		t.Fatal("the ticker has no watchdog")
	}
	if !waitFor(t, 5*time.Second, func() bool { return sink.count() > 0 }) {
		t.Fatal("a ticker whose row count never moved emitted nothing — the ticker's own loop is deliberately silent " +
			"on a stuck copy, so the watchdog is the ONLY thing that speaks there")
	}
	if !strings.Contains(sink.first(), "invoices") {
		t.Errorf("the warning does not name the ticker's table: %s", sink.first())
	}

	// And Stop must reap it: a watchdog outliving its lane would warn about a
	// table that finished.
	pt.Stop(ctx, nil)
	before := sink.count()
	time.Sleep(150 * time.Millisecond)
	if after := sink.count(); after != before {
		t.Errorf("the watchdog emitted %d more warning(s) after Stop; it is not being reaped with its lane",
			after-before)
	}
}

// THE LANE ROSTER — the sibling-sweep half.
//
// The watchdog rides inside [newProgressTicker] / [newProgressTickerForChunk],
// so every copy lane that builds a ticker is covered by construction. The
// danger is the lane that does NOT build one: the ADR-0078 raw byte-pipe has
// no rows to observe and had no ticker, so a fix that stopped at the ticker
// would have left the one lane whose stall is hardest to see uncovered — and
// a gate whose coverage is narrower than its name is worse than no gate.
//
// This walks the package by CALL SITE rather than trusting a written list:
// every function that constructs a progressTicker is a lane, and
// runRawCopyChunk is the one lane that must construct a watchdog directly.
func TestCopyStallWatchdog_ReachesEveryCopyLane(t *testing.T) {
	fset, files := parsePipelineFiles(t)

	tickerLanes := map[string]bool{}   // fn name -> builds a progressTicker
	watchdogLanes := map[string]bool{} // fn name -> builds a copyStallWatchdog
	for name, f := range files {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				id, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				switch id.Name {
				case "newProgressTicker", "newProgressTickerForChunk":
					tickerLanes[fn.Name.Name] = true
				case "newCopyStallWatchdog":
					watchdogLanes[fn.Name.Name] = true
				}
				return true
			})
			_ = name
			_ = fset
		}
	}

	// Anti-vacuity floors in BOTH dimensions. A walker that stopped matching,
	// or one that found only the constructors, would otherwise be green for
	// exactly the defect it exists to catch.
	if len(tickerLanes) < 8 {
		t.Fatalf("found %d function(s) constructing a progressTicker (%v); floor 8 — the walk is vacuous, re-point it",
			len(tickerLanes), sortedKeys(tickerLanes))
	}
	// The ticker constructors themselves must each start a watchdog; that is
	// what makes the ticker lanes covered by construction.
	for _, ctor := range []string{"newProgressTicker", "newProgressTickerForChunk"} {
		if !watchdogLanes[ctor] {
			t.Errorf("%s does not start a copyStallWatchdog — every lane that builds a ticker through it is then "+
				"unwatched, which is the whole class item 146 exists to close", ctor)
		}
	}
	// And the tickerless lane, named explicitly because it cannot be inferred.
	if !watchdogLanes["runRawCopyChunk"] {
		t.Error("runRawCopyChunk does not start a copyStallWatchdog. The ADR-0078 raw byte-pipe lane has no " +
			"progressTicker (no rows to observe), so it is the one lane a ticker-mounted watchdog misses — and a " +
			"stalled byte pipe looks exactly like a slow one from outside")
	}
}
