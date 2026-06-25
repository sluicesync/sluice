// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
)

// defaultVStreamShardStallWarnWindow is the per-shard progress watchdog's
// wall-clock window: how long ONE shard of a multi-shard VStream may go
// without its GTID component advancing — WHILE a PEER shard keeps
// advancing — before the watchdog emits a single rate-limited WARN naming
// the wedged shard. It is OBSERVABILITY ONLY (item 23, increment B-1): it
// never fails, cancels, or alters the stream's resilience; it closes a
// SILENT gap the whole-stream watchdog (vstreamLiveness) cannot see.
//
// WHY THE WHOLE-STREAM WATCHDOG MISSES THIS: the VStream CDC reader merges
// every shard into ONE channel, so the Phase-2 progress timer (and even
// the soft idle-WARN) re-arm on ANY event from ANY shard. A PER-SHARD
// wedge — one shard's vtgate vstreamer stalls (source-tablet throttle, a
// large transaction, or a tablet issue) while the OTHER shard's ROW events
// plus the ~5s heartbeats keep flowing — keeps the whole-stream watchdog
// re-armed indefinitely, so the stream "looks healthy" while one shard's
// position is frozen. Live testing hit exactly this (roadmap item 23):
// shard `-80` froze for HOURS while `80-` advanced and no whole-stream
// signal fired.
//
// THE FALSE-WARN NUANCE (MinimizeSkew): the stream is opened with
// MinimizeSkew=true (see buildVStreamRequest), which DELIBERATELY holds
// back the AHEAD shard's events so the merged stream stays commit-time
// ordered while a BEHIND shard drains a backlog. During a legitimate
// catch-up the held (ahead) shard's GTID will not advance for a long time
// — and that is NORMAL, not a wedge. We cannot distinguish the two purely
// in-reader without a source probe, so we pick the cleanest available
// discriminator and accept a benign WARN during a long skew-hold:
//
//	WARN about shard S only when (a) S has not advanced for the window,
//	AND (b) SOME OTHER shard HAS advanced within the same window (so the
//	stream is provably alive and shard-ASYMMETRIC), AND (c) Phase-2
//	serving has been proven. (a)+(b) is the wedge signature ("S frozen
//	while a peer advances"); it ALSO matches the skew-hold ("S is ahead
//	and the behind peer is draining"). Because the WARN is observability
//	only (no behavior change), a benign skew-hold WARN is ACCEPTABLE — and
//	is itself useful signal — so the message names BOTH possibilities.
//
// Sizing: 60s ≈ 12× the 5s heartbeat cadence and well above both the 30s
// soft idle-WARN window and the 45s hard CDC-tail Phase-2 window, so this
// per-shard heads-up is the LAST of the Phase-2 signals to fire (the
// whole-stream signals get first crack at a global stall; this one is
// specifically for the shard-asymmetric case they cannot see). Generous
// enough to ride out a normal brief per-shard pause without crying wolf on
// every transaction boundary.
//
// Overridable per-DSN via vstream_shard_stall_warn_timeout (a Go duration
// string); 0 or negative disables THIS WARN only (the whole-stream
// watchdog is unaffected). Absent ⇒ this default.
const defaultVStreamShardStallWarnWindow = 60 * time.Second

// shardProgressWatchdog is the per-shard progress watchdog (item 23,
// increment B-1). It mirrors vstreamLiveness's structure and
// race-discipline exactly: a SINGLE watchdog goroutine owns the timer and
// ALL per-shard state; the pump never touches the timer — it only sends
// per-VGTID advancement observations over a buffered, non-blocking channel
// via [observeAdvance]. Keeping the timer single-goroutine is the
// deliberate race-free pattern (the same reasoning as vstreamLiveness).
//
// Unlike the whole-stream watchdog (one re-arm signal), each observation
// carries WHICH shards advanced on that VGTID, so the watchdog can keep a
// per-shard last-advance clock and detect the shard-ASYMMETRIC wedge that
// a whole-stream timer cannot.
//
// The watchdog is purely OBSERVABILITY: onShardStall WARNs and nothing
// else. It is disabled (no goroutine; observe/stop safe no-ops) when
// window<=0, matching vstreamLiveness's disabled pattern — and the zero
// value of the reader's window field (a bare struct literal in a unit
// test) therefore safely means "off", which is the correct default for an
// observability-only signal.
type shardProgressWatchdog struct {
	// advances carries per-VGTID observations from the pump to the
	// watchdog goroutine: the set of shards whose GTID component changed
	// on that VGTID (empty for a heartbeat-only / no-progress batch).
	// Buffered + a non-blocking send in [observeAdvance] so the hot
	// dispatch path never blocks on the watchdog.
	advances chan []string

	// proven signals the Phase-1→2 transition (serving proven). The
	// per-shard WARN is a Phase-2 concept — a frozen shard before serving
	// is proven is the whole-stream Phase-1 guard's job, not this one — so
	// the watchdog stays quiescent until this fires once.
	proven chan struct{}

	// done is closed by [stop] to tear the goroutine down on pump exit. A
	// nil-safe sentinel: a disabled watchdog leaves every channel nil and
	// observe/markServingProven/stop short-circuit.
	done chan struct{}
}

// startShardProgressWatchdog arms the per-shard progress watchdog. window
// is the per-shard stall window; knownShards is the resolved shard layout the
// stream subscribes to (e.g. ["-80","80-"]) — PRE-SEEDED at start so a shard
// that is wedged from the very first CDC event (never delivers one advancing
// VGTID) is still detectable; without pre-seeding, the watchdog only ever knew
// about a shard AFTER its first advance, so a from-start-frozen shard was
// invisible (the live item-23 B-1 blind spot). onShardStall(shard) is called
// ONCE per stall spell per shard (latched, cleared when that shard advances
// again), in the watchdog goroutine, when the shard has not advanced for the
// window WHILE a peer advanced within the window AND serving has been proven.
// onShardStall MUST be observability only (a WARN) — it must NOT fail or
// cancel the stream.
//
// window <= 0 disables the watchdog: returns a no-op whose
// observeAdvance/markServingProven/stop are all safe to call. Production
// always gets the real timer via [newRealLivenessTimer]; only unit tests
// pass anything else (and an injectable clock) through
// [startShardProgressWatchdogWithDeps].
func startShardProgressWatchdog(ctx context.Context, window time.Duration, knownShards []string, onShardStall func(shard string)) *shardProgressWatchdog {
	return startShardProgressWatchdogWithDeps(ctx, window, knownShards, onShardStall, newRealLivenessTimer, time.Now)
}

// startShardProgressWatchdogWithDeps is the test seam behind
// [startShardProgressWatchdog]: identical semantics with the timer factory
// and the wall clock injectable. The watchdog reuses the [livenessTimer]
// seam from cdc_vstream_liveness.go (do not duplicate it). The clock lets
// a test drive per-shard last-advance ages deterministically instead of
// racing the real clock — the same flake-avoidance discipline as the
// liveness fake-timer tests (the v0.99.31 windows-latest flake).
func startShardProgressWatchdogWithDeps(ctx context.Context, window time.Duration, knownShards []string, onShardStall func(shard string), newTimer func(time.Duration) livenessTimer, now func() time.Time) *shardProgressWatchdog {
	w := &shardProgressWatchdog{}
	if window <= 0 {
		// Disabled: nil channels make every method a no-op; no goroutine.
		return w
	}
	// Buffer of 1 is sufficient: per-VGTID advancement sets coalesce in the
	// watchdog (it folds each into the per-shard clock as it drains them),
	// and a non-blocking send means a momentarily-full buffer just drops a
	// re-arm-equivalent signal — never a correctness issue for an
	// observability-only stall detector.
	w.advances = make(chan []string, 1)
	w.proven = make(chan struct{}, 1)
	w.done = make(chan struct{})
	go w.run(ctx, window, knownShards, onShardStall, newTimer, now)
	return w
}

// run is the watchdog goroutine: the SOLE owner of the timer and ALL
// per-shard state. It scans for the shard-asymmetric wedge on a periodic
// timer re-armed to window. The pump never touches the timer.
//
// State:
//   - lastAdvance[shard] = wall-clock time that shard last advanced. ALL
//     knownShards are PRE-SEEDED to the start time, so a shard that is wedged
//     from the very first CDC event — and therefore never delivers an
//     advancing VGTID — still has a clock that goes stale after the window and
//     is detected (the live item-23 B-1 blind spot: before pre-seeding, the
//     map was populated only on a shard's FIRST advance, so a from-start-
//     frozen shard was never even considered by [scan]). A shard not in
//     knownShards (e.g. one that appears only after a reshard) is still added
//     lazily on its first observed advance.
//   - warned[shard] latches a fired WARN so it fires at most once per
//     stall spell; cleared when that shard advances again.
//   - proven gates the whole scan: nothing fires until serving is proven
//     (the per-shard WARN is strictly a Phase-2 concept) — which, with the
//     pre-seed + the "a peer must be fresh" requirement, is what keeps a
//     just-started stream from warning during warm-up (no peer has advanced
//     yet, so the asymmetry can't trip).
//
// On each timer fire (every window), for each known shard S: if S is
// "stale" (now - lastAdvance[S] >= window) AND some OTHER shard P is
// "fresh" (now - lastAdvance[P] < window) AND S is not already latched,
// WARN once and latch S. A stale shard with NO fresh peer is a GLOBAL
// stall (the whole-stream watchdog's job) — deliberately NOT this WARN.
func (w *shardProgressWatchdog) run(ctx context.Context, window time.Duration, knownShards []string, onShardStall func(shard string), newTimer func(time.Duration) livenessTimer, now func() time.Time) {
	timer := newTimer(window)
	defer timer.Stop()

	lastAdvance := make(map[string]time.Time)
	warned := make(map[string]bool)
	proven := false

	// Pre-seed every known shard at the start time so a shard that never
	// advances (wedged from the first event) goes stale after the window and
	// is detectable — the from-start-frozen case the lazy "seed on first
	// advance" map missed. Seeding at now() (not the zero time) gives every
	// shard a full window of grace from start before it can be called stale.
	startedAt := now()
	for _, s := range knownShards {
		if s == "" {
			continue
		}
		lastAdvance[s] = startedAt
	}

	// resetTimer stops+drains then re-arms the timer for window. Same
	// race-free stop/drain/reset as vstreamLiveness.run (single-goroutine
	// ownership makes it safe).
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C():
			default:
			}
		}
		timer.Reset(window)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		case <-w.proven:
			proven = true
		case shards := <-w.advances:
			t := now()
			for _, s := range shards {
				lastAdvance[s] = t
				warned[s] = false // progress resumed → re-arm the WARN for S
			}
		case <-timer.C():
			if proven {
				w.scan(now(), window, lastAdvance, warned, onShardStall)
			}
			resetTimer()
		}
	}
}

// scan evaluates the shard-asymmetric wedge once. For each known shard S
// that is stale (not advanced for >= window) and not already latched, it
// WARNs iff SOME OTHER shard is fresh (advanced within the window) — the
// signature that the stream is alive but S specifically is frozen. A stale
// shard with no fresh peer is a global stall (whole-stream watchdog's job)
// and is left alone. Pure function over the passed-in state so the unit
// tests pin it deterministically with an injected clock.
func (w *shardProgressWatchdog) scan(t time.Time, window time.Duration, lastAdvance map[string]time.Time, warned map[string]bool, onShardStall func(shard string)) {
	anyFresh := false
	for _, last := range lastAdvance {
		if t.Sub(last) < window {
			anyFresh = true
			break
		}
	}
	if !anyFresh {
		// Either everything is stale (a global stall — not our signal) or
		// nothing has advanced yet. Nothing shard-asymmetric to report.
		return
	}
	for s, last := range lastAdvance {
		if warned[s] {
			continue
		}
		if t.Sub(last) < window {
			continue // S itself is fresh
		}
		// S is stale AND a peer is fresh ⇒ shard-asymmetric wedge (or a
		// MinimizeSkew catch-up hold — see onShardStall's message). Fire once.
		warned[s] = true
		onShardStall(s)
	}
}

// observeAdvance records the set of shards whose GTID component advanced on
// a VGTID. The pump computes this set by diffing r.currentVgtid before/
// after a VGTID event (heartbeat-only batches advance no shard → an empty
// or omitted call). Cheap and NON-BLOCKING so the single-goroutine
// dispatch path never parks on the watchdog. Safe after [stop]; a no-op on
// a nil or disabled watchdog (the reader's field is nil outside of pump, so
// the nil-receiver guard matters — some tests call dispatch directly). An
// empty/nil shard set is dropped (nothing to record).
func (w *shardProgressWatchdog) observeAdvance(shards []string) {
	if w == nil || w.advances == nil || len(shards) == 0 {
		return // nil/disabled watchdog, or no advancement to record
	}
	select {
	case w.advances <- shards:
	case <-w.done:
		// Watchdog torn down; nothing to observe.
	default:
		// Buffer full: a re-arm-equivalent advancement is already pending.
		// Dropping is safe — the watchdog only needs the per-shard clock to
		// be RECENT, and the queued signal already makes it so. (Worst case
		// a single shard's freshness is delayed by one scan tick, which only
		// risks a marginally-early benign WARN on an observability signal.)
	}
}

// markServingProven tells the watchdog that a serving tablet has been
// proven (the whole-stream Phase-1→2 transition). The per-shard WARN stays
// quiescent until this is called — a frozen shard before serving is proven
// is the whole-stream Phase-1 guard's concern. Non-blocking and idempotent
// (the buffered channel + the watchdog's set-once flag); safe after [stop]
// and a no-op on a disabled watchdog.
func (w *shardProgressWatchdog) markServingProven() {
	if w == nil || w.proven == nil {
		return // nil/disabled watchdog
	}
	select {
	case w.proven <- struct{}{}:
	default:
		// Already signalled (buffer holds the pending one) — idempotent.
	}
}

// stop tears the watchdog goroutine down on pump teardown. Idempotent and
// safe on a disabled watchdog.
func (w *shardProgressWatchdog) stop() {
	if w == nil || w.done == nil {
		return // nil/disabled watchdog
	}
	select {
	case <-w.done:
		// Already stopped.
	default:
		close(w.done)
	}
}

// advancedShards returns the names of the shards whose GTID component
// changed from prev to next — the per-VGTID advancement set the dispatch
// path feeds [shardProgressWatchdog.observeAdvance]. A shard present in
// next with a different (or newly-present) Gtid string counts as advanced;
// the TablePKs COPY-cursor is deliberately ignored (only the streamed
// position advancing is "progress" for the wedge signal). Cheap: prev is at
// most a handful of shards, so a linear lookup is fine.
func advancedShards(prev, next []shardGtid) []string {
	var out []string
	for _, n := range next {
		old, ok := gtidForShard(prev, n.Shard)
		if !ok || old != n.Gtid {
			out = append(out, n.Shard)
		}
	}
	return out
}

// gtidForShard finds the Gtid string recorded for shard in sgs.
func gtidForShard(sgs []shardGtid, shard string) (string, bool) {
	for _, sg := range sgs {
		if sg.Shard == shard {
			return sg.Gtid, true
		}
	}
	return "", false
}

// vstreamShardStallWarnMessage builds the loud, rate-limited heads-up the
// watchdog logs when one shard's GTID component has frozen for the window
// while a peer shard keeps advancing (item 23, B-1). It is OBSERVABILITY
// ONLY: the stream stays connected (the whole-stream watchdog is still
// re-armed by the peer's events + heartbeats) and will catch up when the
// shard resumes. It names BOTH plausible causes, because the reader cannot
// distinguish them without a source probe:
//
//   - a GENUINE per-shard source stall on the named shard (a source-tablet
//     throttle, a large transaction, or a tablet issue) — check
//     `SHOW VITESS_THROTTLED_APPS` on that shard's primary; OR
//   - a NORMAL MinimizeSkew catch-up hold: vtgate is deliberately holding
//     this (ahead) shard's events back so the merged stream stays
//     commit-time ordered while a BEHIND peer drains a backlog. This is
//     expected during a legitimate catch-up and resolves itself.
//
// relaxSkew (ADR-0120) drops the MinimizeSkew-hold cause from the message: when
// the stream was opened with MinimizeSkew=false (--vstream-relax-skew), vtgate is
// NOT holding the ahead shard, so the only remaining cause is a genuine per-shard
// stall — naming the non-existent hold would point the operator at the wrong
// thing. The "S frozen while a peer advances" signature is then a more reliable
// wedge indicator.
func vstreamShardStallWarnMessage(window time.Duration, keyspace, shard string, relaxSkew bool) string {
	if relaxSkew {
		return fmt.Sprintf(
			"mysql/vstream: shard %q of keyspace %q has not advanced for %s while a peer shard keeps advancing — "+
				"a genuine per-shard source stall (source-tablet throttle, a large transaction, or a tablet issue; "+
				"check `SHOW VITESS_THROTTLED_APPS` on shard %q's primary). MinimizeSkew is relaxed "+
				"(--vstream-relax-skew), so this is not a skew catch-up hold "+
				"(the stream stays connected and will catch up when the shard resumes)",
			shard, keyspace, window, shard,
		)
	}
	return fmt.Sprintf(
		"mysql/vstream: shard %q of keyspace %q has not advanced for %s while a peer shard keeps advancing — "+
			"either a genuine per-shard source stall (source-tablet throttle, a large transaction, or a tablet issue; "+
			"check `SHOW VITESS_THROTTLED_APPS` on shard %q's primary), or a normal MinimizeSkew catch-up hold "+
			"(vtgate holding this ahead shard back while a behind peer drains a backlog) "+
			"(the stream stays connected and will catch up when the shard resumes)",
		shard, keyspace, window, shard,
	)
}

// vstreamShardStallWarnWindowFromDSN reads the optional
// vstream_shard_stall_warn_timeout DSN parameter (a Go duration string) —
// the per-shard progress WARN window (item 23, B-1). Absent ⇒
// [defaultVStreamShardStallWarnWindow]. A 0/negative duration disables the
// per-shard WARN only (the whole-stream watchdog is unaffected). Malformed
// ⇒ loud error.
func vstreamShardStallWarnWindowFromDSN(cfg *gomysql.Config) (time.Duration, error) {
	return vstreamDurationParam(cfg, "vstream_shard_stall_warn_timeout", defaultVStreamShardStallWarnWindow)
}
