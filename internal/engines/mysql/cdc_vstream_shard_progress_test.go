// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordStalls is a test onShardStall that records every shard it is
// called with, in order — the once-per-spell latch means a shard appears
// at most once per stall spell.
type recordStalls struct{ shards []string }

func (r *recordStalls) cb(shard string) { r.shards = append(r.shards, shard) }

// TestShardProgressWatchdog_Scan_AsymmetricStallFiresOnceThenReArms pins
// the CORE wedge logic (item 23, B-1) directly against [scan] — a pure
// function over the per-shard state, so the must/must-not assertions are
// fully deterministic (no goroutine, no clock race). It exercises:
//   - shard S stale while a PEER is fresh ⇒ WARN S exactly once (latched);
//   - a second scan with no intervening advance of S ⇒ NO second WARN;
//   - S advancing (latch cleared, clock seeded fresh) then re-stalling
//     while a different peer is fresh ⇒ a NEW spell WARNs the now-stale
//     peer.
func TestShardProgressWatchdog_Scan_AsymmetricStallFiresOnceThenReArms(t *testing.T) {
	w := &shardProgressWatchdog{}
	const window = 100 * time.Millisecond
	base := time.Unix(0, 0)

	lastAdvance := map[string]time.Time{}
	warned := map[string]bool{}
	var rec recordStalls

	// t=0: both shards advance → both fresh.
	lastAdvance["-80"] = base
	lastAdvance["80-"] = base

	// t=150ms: 80- advances again (fresh); -80 is stale ⇒ asymmetric wedge.
	now := base.Add(150 * time.Millisecond)
	lastAdvance["80-"] = now
	w.scan(now, window, lastAdvance, warned, rec.cb)
	if len(rec.shards) != 1 || rec.shards[0] != "-80" {
		t.Fatalf("first scan WARNs = %v; want exactly [-80]", rec.shards)
	}

	// A second scan, no intervening -80 advance → latch holds, no new WARN.
	w.scan(now, window, lastAdvance, warned, rec.cb)
	if len(rec.shards) != 1 {
		t.Fatalf("second scan WARNs = %v; want still exactly [-80] (once-per-spell latch broken)", rec.shards)
	}

	// -80 advances → latch cleared, -80 seeded fresh. Now let 80- go stale.
	now = now.Add(50 * time.Millisecond) // t=200ms
	lastAdvance["-80"] = now
	warned["-80"] = false // the watchdog clears the latch on advance

	now = now.Add(150 * time.Millisecond) // t=350ms; -80 fresh, 80- stale
	lastAdvance["-80"] = now
	warned["-80"] = false
	w.scan(now, window, lastAdvance, warned, rec.cb)
	if len(rec.shards) != 2 || rec.shards[1] != "80-" {
		t.Fatalf("third scan WARNs = %v; want a NEW spell warning [..., 80-]", rec.shards)
	}
}

// TestShardProgressWatchdog_Scan_GlobalStallDoesNotFire pins that a global
// stall — EVERY shard stale, no fresh peer — does NOT fire the per-shard
// WARN. That asymmetric requirement is the whole discriminator: a global
// stall is the whole-stream soft idle-WARN / Phase-2 watchdog's job.
func TestShardProgressWatchdog_Scan_GlobalStallDoesNotFire(t *testing.T) {
	w := &shardProgressWatchdog{}
	const window = 100 * time.Millisecond
	base := time.Unix(0, 0)

	lastAdvance := map[string]time.Time{"-80": base, "80-": base}
	warned := map[string]bool{}
	var rec recordStalls

	// Both stale (now well past window from both last-advances), no fresh peer.
	now := base.Add(500 * time.Millisecond)
	w.scan(now, window, lastAdvance, warned, rec.cb)
	if len(rec.shards) != 0 {
		t.Fatalf("global stall WARNs = %v; want none (asymmetric-only signal)", rec.shards)
	}
}

// TestShardProgressWatchdog_Scan_AllFreshDoesNotFire pins that when every
// shard is fresh (all advancing normally) nothing warns.
func TestShardProgressWatchdog_Scan_AllFreshDoesNotFire(t *testing.T) {
	w := &shardProgressWatchdog{}
	const window = 100 * time.Millisecond
	now := time.Unix(0, 100) // both seeded at now
	lastAdvance := map[string]time.Time{"-80": now, "80-": now}
	var rec recordStalls
	w.scan(now, window, lastAdvance, map[string]bool{}, rec.cb)
	if len(rec.shards) != 0 {
		t.Fatalf("all-fresh WARNs = %v; want none", rec.shards)
	}
}

// TestShardProgressWatchdog_Scan_ThreeShardsOneStale pins the >2-shard
// case: one stale shard among several fresh peers warns exactly that one.
func TestShardProgressWatchdog_Scan_ThreeShardsOneStale(t *testing.T) {
	w := &shardProgressWatchdog{}
	const window = 100 * time.Millisecond
	base := time.Unix(0, 0)
	now := base.Add(150 * time.Millisecond)
	lastAdvance := map[string]time.Time{
		"-40":   now,  // fresh
		"40-80": now,  // fresh
		"80-":   base, // stale ⇒ the wedge
	}
	var rec recordStalls
	w.scan(now, window, lastAdvance, map[string]bool{}, rec.cb)
	if len(rec.shards) != 1 || rec.shards[0] != "80-" {
		t.Fatalf("three-shard scan WARNs = %v; want exactly [80-]", rec.shards)
	}
}

// pumpScansUntilWarn fires the fake timer repeatedly (re-arming each time)
// until a WARN lands or a generous bound elapses, returning the shard
// warned. It is order-INDEPENDENT: a buffered observeAdvance queued just
// before is guaranteed to be drained by the single watchdog goroutine
// within a few fire/await cycles regardless of select scheduling, so the
// test never depends on observe-vs-fire ordering (no real-clock race; the
// scan reads the injected fake clock). Returns "" if nothing fires.
func pumpScansUntilWarn(t *testing.T, ft *fakeLivenessTimer, warns <-chan string) string {
	t.Helper()
	for i := 0; i < 50; i++ {
		ft.fire <- time.Now()
		select {
		case s := <-warns:
			return s
		case <-ft.resets: // scan ran, no warn this round — fire again
		case <-time.After(2 * time.Second):
			t.Fatal("watchdog did not re-arm after a fire")
		}
	}
	return ""
}

// TestShardProgressWatchdog_EndToEnd_AsymmetricStallWarns pins the full
// goroutine path (item 23, B-1): a real watchdog, fed a per-shard
// advancement set via observeAdvance and serving-proven, WARNs the
// asymmetric wedge exactly the shard that froze. The detailed latch /
// latch-clear lifecycle is pinned deterministically by the scan-level
// tests above (scan is a pure function); this test proves the run loop
// wires observeAdvance → the per-shard clock → scan → onShardStall end to
// end. Uses a hand-advanced clock + a fire-until-warn barrier so it never
// depends on real-clock timing or observe-vs-fire select ordering.
func TestShardProgressWatchdog_EndToEnd_AsymmetricStallWarns(t *testing.T) {
	ft := newFakeLivenessTimer()
	clk := newFakeClock()
	warns := make(chan string, 8)
	w := startShardProgressWatchdogWithDeps(context.Background(), 100*time.Millisecond,
		func(shard string) { warns <- shard },
		ft.factory(), clk.now)
	defer w.stop()

	w.markServingProven()

	// Both fresh at t=0.
	w.observeAdvance([]string{"-80", "80-"})
	// Drain the first observe through the goroutine (one fire/await turn) so
	// the next observe's buffered send can't be coalesced away.
	ft.fire <- time.Now()
	ft.awaitReset(t, 100*time.Millisecond)

	// t=150ms: only 80- advances → -80 stale while a peer is fresh ⇒ wedge.
	clk.advance(150 * time.Millisecond)
	w.observeAdvance([]string{"80-"})
	if got := pumpScansUntilWarn(t, ft, warns); got != "-80" {
		t.Fatalf("asymmetric-stall WARN = %q; want -80", got)
	}

	// Latched: further scans with no -80 advance must NOT warn again.
	for i := 0; i < 5; i++ {
		ft.fire <- time.Now()
		<-ft.resets
	}
	select {
	case s := <-warns:
		t.Fatalf("WARN fired again while latched (shard %q) — once-per-spell broken", s)
	case <-time.After(100 * time.Millisecond):
	}
}

// fakeClock is a hand-advanced wall clock for the end-to-end watchdog test:
// the test owns "now", so per-shard last-advance AGES are driven
// deterministically instead of racing the real clock — the same
// flake-avoidance discipline as the liveness fake-timer tests. Mutated by
// the test goroutine and read by the watchdog goroutine, so it locks.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(0, 0)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// TestShardProgressWatchdog_NeverBeforeServingProven pins that the WARN is
// a Phase-2 concept end-to-end: with serving NOT proven, even an
// asymmetric stall + a timer fire produces no WARN (the goroutine's proven
// gate). Fake-timer drives the scan deterministically; the gate is the
// thing under test, not timing.
func TestShardProgressWatchdog_NeverBeforeServingProven(t *testing.T) {
	ft := newFakeLivenessTimer()
	w := startShardProgressWatchdogWithDeps(context.Background(), 100*time.Millisecond,
		func(shard string) {
			t.Errorf("per-shard WARN fired for %q before serving was proven — it must be Phase-2 only", shard)
		},
		ft.factory(), func() time.Time { return time.Unix(0, 0).Add(time.Hour) })
	defer w.stop()

	// Asymmetric advancement at "now"=0, but serving is never proven.
	w.observeAdvance([]string{"-80", "80-"})
	// Fire repeatedly; the scan is gated off (proven==false), and each fire
	// re-arms — observe the re-arm as the barrier proving the fire was handled.
	for i := 0; i < 3; i++ {
		ft.fire <- time.Now()
		ft.awaitReset(t, 100*time.Millisecond)
	}
}

// TestShardProgressWatchdog_DisabledWindow pins that window<=0 disables the
// watchdog entirely: no goroutine (no timer ever requested), and
// observeAdvance / markServingProven / stop are all safe no-ops.
func TestShardProgressWatchdog_DisabledWindow(t *testing.T) {
	requested := false
	w := startShardProgressWatchdogWithDeps(context.Background(), 0,
		func(shard string) {
			t.Errorf("per-shard WARN fired for %q with window<=0 — it must be disabled", shard)
		},
		func(time.Duration) livenessTimer {
			requested = true
			return newFakeLivenessTimer()
		},
		time.Now)

	if requested {
		t.Fatal("disabled watchdog requested a timer (started a goroutine); it must not")
	}
	w.markServingProven()
	w.observeAdvance([]string{"-80", "80-"})
	w.stop()
	w.stop() // idempotent
	time.Sleep(50 * time.Millisecond)
}

// TestShardProgressWatchdog_StopTearsDown pins that stop() exits the
// goroutine: after stop, the deferred timer.Stop lands (observed via the
// fake), and observe is a safe no-op.
func TestShardProgressWatchdog_StopTearsDown(t *testing.T) {
	ft := newFakeLivenessTimer()
	w := startShardProgressWatchdogWithDeps(context.Background(), time.Minute,
		func(shard string) { t.Errorf("per-shard WARN fired for %q after teardown", shard) },
		ft.factory(), time.Now)
	w.stop()
	ft.awaitStop(t)                 // the deferred timer.Stop ⇒ the goroutine exited
	w.observeAdvance([]string{"x"}) // safe no-op after stop
	w.markServingProven()           // safe no-op after stop
}

// TestShardProgressWatchdog_NilSafe pins that BOTH disabled forms are safe:
// a nil receiver (the reader's r.shardProgress field outside of pump — some
// tests call dispatch directly, which would deref it) AND the bare
// zero-value form. Every method must short-circuit.
func TestShardProgressWatchdog_NilSafe(_ *testing.T) {
	var nilW *shardProgressWatchdog
	nilW.markServingProven()
	nilW.observeAdvance([]string{"x"})
	nilW.stop()

	zeroW := &shardProgressWatchdog{}
	zeroW.markServingProven()
	zeroW.observeAdvance([]string{"x"})
	zeroW.stop()
}

// TestAdvancedShards pins the per-VGTID diff feeding the watchdog: a shard
// whose Gtid changed (or is newly present) is "advanced"; an unchanged
// shard is not; the TablePKs cursor is irrelevant to the diff.
func TestAdvancedShards(t *testing.T) {
	prev := []shardGtid{
		{Shard: "-80", Gtid: "MySQL56/a:1-10"},
		{Shard: "80-", Gtid: "MySQL56/b:1-20"},
	}
	next := []shardGtid{
		{Shard: "-80", Gtid: "MySQL56/a:1-10"}, // unchanged
		{Shard: "80-", Gtid: "MySQL56/b:1-25"}, // advanced
		{Shard: "c0-", Gtid: "MySQL56/c:1-1"},  // newly present ⇒ advanced
	}
	got := advancedShards(prev, next)
	want := map[string]bool{"80-": true, "c0-": true}
	if len(got) != len(want) {
		t.Fatalf("advancedShards = %v; want shards %v", got, want)
	}
	for _, s := range got {
		if !want[s] {
			t.Errorf("advancedShards returned unexpected shard %q (it did not advance)", s)
		}
	}
}

// TestVStreamShardStallWarnWindowFromDSN pins the DSN parse: absent ⇒
// default; valid ⇒ pass-through; 0 ⇒ disabled; malformed ⇒ loud error
// naming the param.
func TestVStreamShardStallWarnWindowFromDSN(t *testing.T) {
	cases := []struct {
		name    string
		val     string
		want    time.Duration
		wantErr bool
	}{
		{name: "default (absent)", val: "", want: defaultVStreamShardStallWarnWindow},
		{name: "explicit 90s", val: "90s", want: 90 * time.Second},
		{name: "zero disables", val: "0s", want: 0},
		{name: "malformed ⇒ loud error", val: "soonish", wantErr: true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			params := map[string]string{}
			if c.val != "" {
				params["vstream_shard_stall_warn_timeout"] = c.val
			}
			cfg, _ := minimalConfig("host:3306", params)
			got, err := vstreamShardStallWarnWindowFromDSN(cfg)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want loud error for %q; got nil (%v)", c.val, got)
				}
				if !strings.Contains(err.Error(), "vstream_shard_stall_warn_timeout") {
					t.Errorf("error does not name the param: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %v; want %v", got, c.want)
			}
		})
	}
}

// TestStripVStreamParams_StripsShardStallWarn pins (Bug 126 class) that the
// new vstream_shard_stall_warn_timeout knob is stripped before a MySQL
// session — it is a sluice-internal VStream knob, never a MySQL system
// variable, so openDB must not emit SET vstream_shard_stall_warn_timeout
// (vtgate's MySQL parser would reject it). It rides the vstream_ prefix, but
// pin it explicitly so a future change to the strip mechanism can't silently
// drop coverage for this knob.
func TestStripVStreamParams_StripsShardStallWarn(t *testing.T) {
	cfg, err := parseDSN("u:p@tcp(h:3306)/db?vstream_shard_stall_warn_timeout=90s")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	stripped := stripVStreamParams(cfg)
	if _, ok := stripped.Params["vstream_shard_stall_warn_timeout"]; ok {
		t.Error("stripped.Params still contains vstream_shard_stall_warn_timeout; openDB would emit SET … and vtgate would reject it (Bug 126 class)")
	}
	// Non-mutation: the caller's cfg keeps the param (openVStreamReader reads
	// it out of its own cfg before any openDB call).
	if _, ok := cfg.Params["vstream_shard_stall_warn_timeout"]; !ok {
		t.Error("stripVStreamParams mutated the caller's cfg (lost vstream_shard_stall_warn_timeout); it must Clone()")
	}
}

// TestVStreamShardStallWarnMessage_Actionable pins the operator-facing
// contract: the heads-up names the wedged shard, the keyspace, BOTH
// candidate causes (a genuine per-shard source stall via
// SHOW VITESS_THROTTLED_APPS, OR a normal MinimizeSkew catch-up hold), and
// states the stream stays connected.
func TestVStreamShardStallWarnMessage_Actionable(t *testing.T) {
	msg := vstreamShardStallWarnMessage(60*time.Second, "main", "-80")
	for _, want := range []string{
		`"-80"`, `"main"`, "has not advanced",
		"SHOW VITESS_THROTTLED_APPS", "MinimizeSkew",
		"stays connected",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("shard-stall message missing %q: %v", want, msg)
		}
	}
}
