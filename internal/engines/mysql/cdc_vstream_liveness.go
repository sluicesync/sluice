// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/topodata"
)

// defaultVStreamLivenessWindow is the Phase-1 wall-clock window a VStream
// pump waits for the first NON-HEARTBEAT VEvent — a VGTID / FIELD / ROW /
// DDL / COPY_COMPLETED / JOURNAL, i.e. any event that proves a tablet is
// actually SERVING the keyspace — after the stream opens, before it
// surfaces a LOUD reader error instead of hanging.
//
// This is the loud-failure-tenet fix for the primary-only-cluster wedge
// (ADR-0073 (b2)): sluice's CDC tail requests a REPLICA tablet by
// default, and against a primary-only Vitess cluster (PlanetScale
// development branches, minimal self-hosted setups) vtgate has no REPLICA
// tablet to serve the VStream. vtgate logs `failed to find a REPLICA
// tablet for VStream in <ks>/<shard>` — but, crucially, it KEEPS SENDING
// HEARTBEATS every ~5s (the request's HeartbeatInterval=5) while never
// emitting a single data/position event. So the pump's Recv keeps
// returning, but only ever HEARTBEAT events; no rows, no VGTID, no
// progress — Err() stays nil forever: a SILENT hang. This watchdog
// converts that into a loud, actionable error.
//
// PHASE-A GROUND TRUTH (primary-only `vitesscluster` harness, Vitess 24):
//   - DEAD stream (no REPLICA tablet): Recv returns `[HEARTBEAT]` every
//     ~5s, NOTHING ELSE, indefinitely.
//   - HEALTHY stream: the VERY FIRST Recv returns `[VGTID OTHER]`
//     immediately on attach (vtgate stamps the starting position before
//     any row and before the first heartbeat), even when the source is
//     completely idle.
//
// That is the clean discriminator: a HEARTBEAT does NOT prove a serving
// tablet exists (vtgate sends it regardless), but ANY non-heartbeat event
// does. So Phase 1 keys on the absence of any NON-HEARTBEAT event — not
// the absence of all events. A healthy idle source proves serving within
// the first second (its initial VGTID) and the watchdog transitions to
// Phase 2; legitimate long-idle workloads never false-time-out. 30s ≈ 6×
// the 5s heartbeat: generous enough to ride out a slow vtgate cold-start,
// short enough to fail loudly rather than wedge a sync.
//
// Overridable per-DSN via vstream_liveness_timeout (a Go duration string);
// 0 or negative disables the watchdog entirely (an explicit opt-out for
// pathological setups).
const defaultVStreamLivenessWindow = 30 * time.Second

// defaultVStreamProgressWindow is the Phase-2 (mid-stream) wall-clock
// window for the CDC-TAIL pumps — the standalone CDC reader and the
// snapshot's post-COPY CDC pump. After the stream has proven a serving
// tablet (Phase 1 cleared), the watchdog re-arms on EVERY Recv that
// yields ≥1 event (data OR heartbeat) and fires LOUD if TOTAL SILENCE
// outlasts this window.
//
// PHASE-A GROUND TRUTH (Vitess-24 chaos harness, hard primary failover /
// EmergencyReparentShard): after a hard failover the gRPC Recv goes
// completely dead-silent — 43 normal data batches flowed, then NOTHING:
// no data, no error, and NO heartbeats (vtgate heartbeats only when the
// stream is IDLE; a hung post-failover stream sends nothing at all). The
// first-event watchdog had long since disarmed (data flowed), so it
// couldn't catch this mid-stream hang — the reader sat at Err()==nil
// forever (a silent partial; a silent-loss-tenet violation). Phase 2
// closes that gap.
//
// Sizing: a HEALTHY idle stream gets a heartbeat every ~5s
// (HeartbeatInterval=5), each of which re-arms Phase 2. So ANY event —
// even a lone heartbeat — keeps the tail alive indefinitely; only genuine
// total silence trips it. 45s ≈ 9× the heartbeat cadence: comfortably
// rides out a few dropped/late heartbeats while still flipping a wedged
// post-failover stream to a LOUD failure in under a minute (so a `sync`
// run's retry reconnects promptly).
//
// Overridable per-DSN via vstream_progress_timeout (a Go duration
// string); 0 or negative disables Phase 2 only (Phase 1 still guards the
// first event). Absent ⇒ this default.
const defaultVStreamProgressWindow = 45 * time.Second

// defaultVStreamIdleWarnWindow is the Phase-2 SOFT sub-window: how long a
// PROVEN stream may receive ONLY heartbeat-only observations (heartbeats
// flowing, zero change events) before the watchdog emits a single
// rate-limited WARN per quiet spell. It is OBSERVABILITY ONLY — it never
// fails the stream, never fires the hard Phase-2 timeout, and never alters
// the resilient wait. It exists because of the verified Vitess throttle
// gap (roadmap item 19(a)): when a source tablet's throttler engages
// mid-stream, vtgate withholds ROW/change events but keeps sending its ~5s
// synthesized heartbeats AND strips the tablet's in-band `VEvent.throttled`
// flag — so the hard Phase-2 watchdog stays re-armed by the heartbeats
// (correctly resilient) and the stall is SILENT: unbounded lag, zero
// diagnostic. This soft WARN turns that into a loud heads-up ("alive but no
// change events — the source may be throttled or genuinely idle") without
// disturbing the resilient behavior.
//
// Sizing: 30s ≈ 6× the 5s heartbeat cadence and well under the 45s hard
// CDC-tail Phase-2 window, so the heads-up always precedes the (different,
// failover-shaped) hard timeout on the tail; on the COPY pump (10-min hard
// window) it is purely an informational heads-up during a legitimately long
// warmup. Re-armed and its warn-latch cleared by the next REAL
// (non-heartbeat) event, so a stream that resumes progress goes quiet
// again until the next spell.
//
// Overridable per-DSN via vstream_idle_warn_timeout (a Go duration string);
// 0 or negative disables the soft WARN only (both hard phases unaffected).
// Absent ⇒ this default.
const defaultVStreamIdleWarnWindow = 30 * time.Second

// defaultVStreamCopyProgressWindow is the Phase-2 window for the snapshot
// COPY pump specifically — DELIBERATELY far more generous than the
// CDC-tail Phase-2 window.
//
// WHY SO LARGE (the COPY slow-start, measured): the COPY phase can
// legitimately take MINUTES before its first COPY row — vreplication +
// schema-engine warmup on the serving PRIMARY. A restored PlanetScale
// production branch was measured at ~2.5 min between the stream's initial
// attach VGTID and the first buffered COPY row. During that window the
// ONLY event may be the initial attach VGTID (which clears Phase 1 — it
// proves the tablet is serving — and arms Phase 2). Whether vtgate emits
// heartbeats during COPY warmup is not something we can rely on across
// versions (we can't run the cluster to confirm), so we take the SAFE
// option: a Phase-2 window large enough to ride out the worst measured
// warmup with generous margin, rather than risk a false LOUD failure that
// aborts a healthy cold-start. 10 min ≈ 4× the measured ~2.5 min warmup.
//
// The COPY pump also has its own in-place reconnect machinery
// (reconnectCopy, ADR-0072 Phase C) for retriable Recv errors; this
// watchdog is the backstop for the SILENT case where Recv neither errors
// nor returns — the same failover-wedge class the CDC tail's Phase 2
// guards, but with the COPY warmup tolerance baked in.
//
// Overridable per-DSN via vstream_copy_progress_timeout (a Go duration
// string); 0 or negative disables Phase 2 for the COPY pump only. Absent
// ⇒ this default.
const defaultVStreamCopyProgressWindow = 10 * time.Minute

// eventsProveLiveness reports whether a Recv batch contains at least one
// VEvent that proves a tablet is actually SERVING the stream — i.e. any
// event that is NOT a bare heartbeat. vtgate emits HEARTBEAT events on the
// configured cadence even when it has no serving tablet for the keyspace
// (the primary-only wedge; ADR-0073 (b2)), so a heartbeat alone must NOT
// clear Phase 1. Every other event type (VGTID, FIELD, ROW, DDL,
// BEGIN/COMMIT, COPY_COMPLETED, JOURNAL, …) only flows when a tablet is
// serving, so any one of them is liveness proof. An empty batch proves
// nothing.
func eventsProveLiveness(evs []*binlogdata.VEvent) bool {
	for _, ev := range evs {
		if ev.GetType() != binlogdata.VEventType_HEARTBEAT {
			return true
		}
	}
	return false
}

// vstreamLiveness is the CONTINUOUS two-phase progress watchdog shared by
// the three VStream pumps (the standalone CDC tail, the snapshot COPY
// pump, and the snapshot post-COPY CDC pump). It guards each pump for its
// WHOLE life — not just the first event — so a mid-stream silent wedge
// (the post-failover dead-Recv class, ADR-0073 (F3)) fails LOUDLY instead
// of hanging at Err()==nil forever.
//
// A single watchdog goroutine owns the timer and ALL phase state. The
// pump never touches the timer; it only sends observations over a
// buffered channel via [observe]. Keeping the timer single-goroutine is
// the deliberate race-free pattern: the re-arm (timer reset on every
// observation) is exactly where a race between the pump goroutine and the
// watchdog would hide, so the pump is kept out of the timer entirely.
//
// The two phases (keyed on whether a serving tablet has been proven):
//
//   - PHASE 1 — before the first NON-HEARTBEAT event. Fires LOUD if no
//     serving-proof event arrives within phase1Window. This is the
//     original v0.99.7 primary-only guard: heartbeats alone do NOT clear
//     it (vtgate heartbeats even with no serving tablet), so a
//     heartbeats-but-no-data-ever stream trips it. Observations that are
//     heartbeat-only ([observe](false)) do NOT re-arm Phase 1 — the
//     window is an absolute deadline from stream-open for the first
//     serving proof, exactly as before.
//
//   - PHASE 2 — after the first NON-HEARTBEAT event ([observe](true)).
//     The stream has proven a serving tablet, so total silence now means
//     a hung/wedged stream (the failover case). Fires LOUD if NO event of
//     ANY kind (incl. heartbeat) arrives within phase2Window. Re-arms on
//     EVERY observation (data OR heartbeat), so a healthy idle stream —
//     whose ~5s heartbeats keep arriving — stays alive indefinitely; only
//     genuine silence trips it.
//
//   - PHASE-2 SOFT sub-window (softWindow) — a SECOND, shorter deadline
//     that runs alongside the hard Phase-2 timer and is OBSERVABILITY ONLY.
//     Heartbeat-only observations re-arm the hard Phase-2 timer (keeping
//     the stream alive — correct) but DO NOT re-arm the soft timer, so a
//     spell of "heartbeats flowing, zero change events" — the verified
//     throttle/idle signature (item 19(a)) — lets the soft timer elapse and
//     fires onSoftIdle ONCE per spell (latched). A REAL (non-heartbeat)
//     event re-arms the soft timer and clears the latch. onSoftIdle MUST
//     NOT fail or cancel the stream — it only emits a WARN; the hard
//     Phase-2 timer is the only path that ever errors.
//
// Windows:
//   - phase1Window <= 0 disables the WHOLE watchdog (no goroutine; observe
//     and stop are safe no-ops). The explicit opt-out.
//   - phase2Window <= 0 disables PHASE 2 ONLY: Phase 1 still guards the
//     first event, but once it clears the watchdog goes quiescent (a
//     proven stream is never timed out mid-stream). Lets an operator keep
//     the primary-only guard while opting out of the progress guard.
//   - softWindow <= 0 disables the SOFT WARN only (both hard phases
//     unaffected). softWindow is ignored entirely while Phase 1 is
//     un-cleared (the soft window is a Phase-2 concept).
type vstreamLiveness struct {
	// events carries observations from the pump to the watchdog goroutine.
	// A bool payload: true == this batch proved a serving tablet (a
	// non-heartbeat event), false == heartbeat-only. Buffered + a
	// non-blocking send in [observe] so the hot pump path never blocks on
	// the watchdog (a full buffer just means a re-arm is already pending,
	// which is as good as another).
	events chan bool

	// done is closed by [stop] to tear the watchdog goroutine down on pump
	// exit. A nil-safe sentinel: a disabled watchdog (phase1Window<=0)
	// leaves events/done nil and observe/stop short-circuit.
	done chan struct{}
}

// livenessTimer abstracts the watchdog goroutine's single timer so unit
// tests can substitute a hand-fired fake: the "must NOT fire while
// re-armed" assertions are pinned against a deterministic re-arm count
// instead of racing a real clock — real-clock windows lost that race on
// slow CI runners (the v0.99.31 windows-latest flake in
// TestVStreamLiveness_Phase2_ReArmsAcrossManyEvents). Production always
// gets the real time.Timer wrapper via [startVStreamLiveness].
type livenessTimer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration)
}

// realLivenessTimer adapts *time.Timer to [livenessTimer].
type realLivenessTimer struct{ t *time.Timer }

func (r realLivenessTimer) C() <-chan time.Time   { return r.t.C }
func (r realLivenessTimer) Stop() bool            { return r.t.Stop() }
func (r realLivenessTimer) Reset(d time.Duration) { r.t.Reset(d) }

func newRealLivenessTimer(d time.Duration) livenessTimer {
	return realLivenessTimer{time.NewTimer(d)}
}

// startVStreamLiveness arms the continuous two-phase watchdog. The two
// callbacks run in the watchdog goroutine when their phase's window
// elapses without the required event; each should record a loud error and
// cancel the stream so the pump's parked Recv unblocks. EXACTLY ONE fires
// at most (the goroutine returns after firing):
//
//   - onPhase1Timeout — no serving-proof event within phase1Window (the
//     primary-only / dead-stream-from-open wedge).
//   - onPhase2Timeout — total silence within phase2Window after a serving
//     tablet was proven (the mid-stream / post-failover wedge).
//   - onSoftIdle — a Phase-2 spell of heartbeat-only observations outlasted
//     softWindow (the throttle/idle heads-up; item 19(a)). OBSERVABILITY
//     ONLY: it WARNs, it must NOT fail or cancel the stream. Fires once per
//     quiet spell; a real event re-arms and re-latches it.
//
// Windows:
//   - phase1Window <= 0 disables the whole watchdog (returns a no-op whose
//     observe/stop are safe to call).
//   - phase2Window <= 0 keeps Phase 1 but disables Phase 2 (a proven
//     stream is never timed out mid-stream). The soft WARN still runs if
//     softWindow > 0 — they are independent.
//   - softWindow <= 0 (or onSoftIdle nil) disables the soft WARN only.
func startVStreamLiveness(ctx context.Context, phase1Window, phase2Window, softWindow time.Duration, onPhase1Timeout, onPhase2Timeout, onSoftIdle func()) *vstreamLiveness {
	return startVStreamLivenessWithTimer(ctx, phase1Window, phase2Window, softWindow, onPhase1Timeout, onPhase2Timeout, onSoftIdle, newRealLivenessTimer)
}

// startVStreamLivenessWithTimer is the test seam behind
// [startVStreamLiveness]: identical semantics, with the timer factory
// injectable. Only the unit tests ever pass anything other than
// [newRealLivenessTimer]. The factory is called once per timer the watchdog
// owns (the hard phase timer and, when the soft WARN is enabled, the soft
// timer), each with its initial duration.
func startVStreamLivenessWithTimer(ctx context.Context, phase1Window, phase2Window, softWindow time.Duration, onPhase1Timeout, onPhase2Timeout, onSoftIdle func(), newTimer func(time.Duration) livenessTimer) *vstreamLiveness {
	l := &vstreamLiveness{}
	if phase1Window <= 0 {
		// Disabled: nil channels make observe/stop no-ops; no goroutine.
		return l
	}
	// Buffer of 1 is sufficient: a pending re-arm signal is as good as any
	// number of them, and the watchdog drains one per loop iteration.
	l.events = make(chan bool, 1)
	l.done = make(chan struct{})
	go l.run(ctx, phase1Window, phase2Window, softWindow, onPhase1Timeout, onPhase2Timeout, onSoftIdle, newTimer)
	return l
}

// run is the watchdog goroutine: the SOLE owner of BOTH timers (the hard
// phase timer and the soft idle-WARN timer) and ALL phase state. The pump
// never touches a timer — keeping every timer single-goroutine is the
// race-free pattern (the re-arm is exactly where a pump/watchdog race would
// hide). It starts in Phase 1 with phase1Window armed and loops on
// {observation, hard timer, soft timer, done, ctx}.
//
// The first serving-proof observation transitions to Phase 2 (re-arming the
// hard timer with phase2Window, or going quiescent if Phase 2 is disabled,
// AND arming the soft timer if softWindow>0); thereafter every observation
// re-arms the hard Phase-2 timer, while only a REAL (non-heartbeat) event
// re-arms the soft timer and clears the soft-warn latch. A hard-timer fire
// calls the active phase's callback once and RETURNS (terminal); a
// soft-timer fire emits the WARN callback once per quiet spell and KEEPS
// LOOPING (observability only — never terminal).
func (l *vstreamLiveness) run(ctx context.Context, phase1Window, phase2Window, softWindow time.Duration, onPhase1Timeout, onPhase2Timeout, onSoftIdle func(), newTimer func(time.Duration) livenessTimer) {
	timer := newTimer(phase1Window)
	defer timer.Stop()

	// The soft timer is only created when the soft WARN is enabled
	// (softWindow>0 AND a callback is provided). When disabled it stays nil
	// and every soft-path branch below is a guarded no-op — softEnabled is
	// the single gate.
	softEnabled := softWindow > 0 && onSoftIdle != nil
	var softTimer livenessTimer
	if softEnabled {
		// Created disarmed (the soft window is a Phase-2 concept; nothing to
		// warn about until a serving tablet is proven). Stop()ped at once so
		// it does not fire during Phase 1.
		softTimer = newTimer(softWindow)
		softTimer.Stop()
		defer softTimer.Stop()
	}

	// phase2 tracks whether a serving tablet has been proven (Phase 1
	// cleared). armed tracks whether the hard timer is currently a live
	// deadline — once Phase 2 is disabled (phase2Window<=0) and Phase 1
	// has cleared, the watchdog is quiescent and hard-timer fires are
	// ignored. softWarned latches a fired soft WARN so it fires at most
	// once per quiet spell; a real event clears it.
	phase2 := false
	armed := true
	softWarned := false

	// resetTimer stops+drains t and re-arms it for d, leaving it disarmed
	// when d<=0. Single-goroutine ownership makes the stop/drain/reset
	// sequence race-free. The drain reads from t's OWN channel (passed in)
	// so the same helper serves both timers.
	resetTimer := func(t livenessTimer, d time.Duration) bool {
		if !t.Stop() {
			// Drain a possibly-already-fired timer so the next Reset is
			// clean. Non-blocking: the value may have already been
			// consumed by the select below.
			select {
			case <-t.C():
			default:
			}
		}
		if d > 0 {
			t.Reset(d)
			return true
		}
		return false
	}

	// reset re-arms the HARD phase timer and tracks armed.
	reset := func(d time.Duration) { armed = resetTimer(timer, d) }

	// armSoft re-arms the SOFT timer (Phase-2 only) and clears the warn
	// latch — called on a real event and on the Phase-1→2 transition.
	armSoft := func() {
		if !softEnabled {
			return
		}
		resetTimer(softTimer, softWindow)
		softWarned = false
	}

	// softC is the soft timer's fire channel, or nil when disabled so the
	// select arm is statically dead (a nil channel never becomes ready).
	softC := func() <-chan time.Time {
		if softEnabled {
			return softTimer.C()
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-l.done:
			return
		case proof := <-l.events:
			switch {
			case !phase2 && proof:
				// First serving proof: clear Phase 1, enter Phase 2. Arm
				// both the hard progress timer and the soft idle-WARN timer.
				phase2 = true
				reset(phase2Window)
				armSoft()
			case phase2 && proof:
				// Mid-stream REAL event: re-arm the hard progress deadline
				// AND the soft timer, clearing any prior idle WARN latch —
				// progress resumed, so the next idle spell starts fresh.
				reset(phase2Window)
				armSoft()
			case phase2:
				// Mid-stream HEARTBEAT-ONLY: re-arm the hard progress
				// deadline (the stream is still alive — stay resilient) but
				// DELIBERATELY do NOT touch the soft timer. A sustained
				// heartbeat-only spell is exactly what the soft WARN exists
				// to surface (the throttle/idle signature, item 19(a)).
				reset(phase2Window)
			default:
				// Phase 1, heartbeat-only observation: does NOT re-arm.
				// The Phase-1 window is an absolute deadline from
				// stream-open for the first serving proof.
			}
		case <-timer.C():
			if !armed {
				// Quiescent (Phase 2 disabled, Phase 1 cleared): a stale
				// fire we chose not to act on. Keep looping so observe/stop
				// stay serviced.
				continue
			}
			if phase2 {
				onPhase2Timeout()
			} else {
				onPhase1Timeout()
			}
			return
		case <-softC():
			// Soft idle-WARN fire: a Phase-2 spell of heartbeat-only (or no)
			// observations outlasted softWindow. Emit the heads-up ONCE per
			// spell and KEEP LOOPING — observability only, never terminal,
			// never re-armed here (the latch holds until a real event clears
			// it via armSoft). Guard on phase2 so a stale fire racing the
			// Phase-1→2 transition can't warn before serving is proven.
			if phase2 && !softWarned {
				softWarned = true
				onSoftIdle()
			}
		}
	}
}

// observe records that a Recv batch yielded ≥1 event. provesServing is
// the [eventsProveLiveness] verdict for the batch: true when the batch
// contains a non-heartbeat event (proves a serving tablet → clears Phase
// 1), false for a heartbeat-only batch. The pump calls this on every Recv
// that returns at least one event. Cheap and NON-BLOCKING so the hot pump
// path never parks on the watchdog. Safe to call after [stop].
//
// Coalescing on a full buffer (capacity 1): a heartbeat-only signal is
// DROPPED (a re-arm is already pending — equivalent outcome). A
// serving-proof signal must never be lost behind a queued heartbeat (it
// drives the Phase-1→Phase-2 transition), so it drains the pending signal
// and replaces it with the proof. The drain+replace is itself
// non-blocking; worst case the watchdog already consumed the buffered
// signal between our drain and replace, which only re-arms — never wrong.
func (l *vstreamLiveness) observe(provesServing bool) {
	if l.events == nil {
		return // disabled watchdog
	}
	select {
	case l.events <- provesServing:
	case <-l.done:
		// Watchdog torn down; nothing to observe.
	default:
		// Buffer full.
		if !provesServing {
			return // pending re-arm already covers a heartbeat-only signal
		}
		select { // make room for the proof
		case <-l.events:
		default:
		}
		select { // enqueue the proof; never block
		case l.events <- true:
		default:
		}
	}
}

// stop tears the watchdog goroutine down on pump teardown so it exits
// even if no timeout ever fired (the onTimeout path already ran, or the
// pump is shutting down cleanly). Idempotent and safe to call on a
// disabled watchdog.
func (l *vstreamLiveness) stop() {
	if l.done == nil {
		return // disabled watchdog
	}
	select {
	case <-l.done:
		// Already stopped.
	default:
		close(l.done)
	}
}

// vstreamLivenessTimeoutError builds the loud, actionable error the
// watchdog records when no serving-proof event flows within the PHASE-1
// window. It names the tablet type, the keyspace, and BOTH candidate causes
// with their one-line remedies so an operator gets an actionable diagnosis
// rather than a silent hang:
//
//   - the primary-only wedge (no serving REPLICA tablet) → set
//     vstream_tablet_type=primary in the DSN (ADR-0073 (b2)); and
//   - a source-tablet throttler denying the stream at open (item 19(a)) →
//     check `SHOW VITESS_THROTTLED_APPS` on the primary. The throttler
//     suppresses ROW/change events while vtgate still heartbeats, which is
//     exactly the no-serving-proof-event signature Phase 1 keys on, so it
//     is a genuine candidate cause for this timeout — not only topology.
//
// RETRIABLE (Bug 141, ADR-0038): wrapped as an [ir.RetriableError] so the
// pipeline's bounded exponential-backoff retry rides it out IN-PROCESS
// instead of exiting (which, under a process supervisor like systemd,
// turned a transient source-tablet throttle into a tight crash-loop that
// never converged — the headline finding of the first real PlanetScale
// soak). The dominant real cause at stream-open is a source-tablet
// throttle (item 19(a)), which heals on release within a few backed-off
// reconnects; the primary-only-wedge case does NOT heal, but the retry
// budget is bounded, so it still fails loud after exhaustion — just not in
// a tight loop. The actionable message (both candidate causes + remedies)
// is preserved for that terminal-after-budget failure.
func vstreamLivenessTimeoutError(window time.Duration, tabletType topodata.TabletType, keyspace string, shards []string) error {
	return &retriableMySQLError{err: fmt.Errorf(
		"mysql/vstream: no events within %s of opening the stream; vtgate may have no %s tablet for keyspace %q shards %v "+
			"(if the cluster is primary-only — e.g. a PlanetScale dev branch or a minimal self-hosted Vitess — set vstream_tablet_type=primary in the DSN), "+
			"or the source tablet throttler is denying the stream — check `SHOW VITESS_THROTTLED_APPS` on the primary",
		window, tabletType, keyspace, shards,
	)}
}

// vstreamProgressTimeoutError builds the loud, actionable error the
// watchdog records when a PROVEN stream then goes totally silent for the
// PHASE-2 window — the mid-stream wedge (ADR-0073 (F3)). Two real causes,
// indistinguishable from the stream alone (vtgate erases the in-band
// throttle signal — item 19(a)): a tablet failover/reparent that left the
// gRPC Recv hung, OR a sustained source-tablet throttle / large-transaction
// stall that withholds events past the window.
//
// RETRIABLE (Bug 141, ADR-0038): wrapped as an [ir.RetriableError] so the
// pipeline's bounded exponential-backoff retry reconnects from the last
// position IN-PROCESS — the right recovery for BOTH causes (a fresh
// StreamChanges reconnects to the new primary after a failover, and rides
// out a throttle until it clears). This replaces the prior terminal exit,
// which under a supervisor became a tight crash-loop that never converged.
func vstreamProgressTimeoutError(window time.Duration, tabletType topodata.TabletType, keyspace string, shards []string) error {
	return &retriableMySQLError{err: fmt.Errorf(
		"mysql/vstream: stream produced no events for %s after data had been flowing; the %s stream for keyspace %q shards %v "+
			"may have hung after a tablet failover/reparent (EmergencyReparentShard / PlannedReparentShard), or a sustained "+
			"source-tablet throttle / large-transaction stall is withholding events — reconnecting from the last position",
		window, tabletType, keyspace, shards,
	)}
}

// vstreamIdleWarnMessage builds the loud, rate-limited heads-up the
// watchdog logs when a PROVEN stream receives only heartbeats — no change
// events — for the SOFT window (item 19(a)). It is OBSERVABILITY ONLY: the
// stream stays alive (the hard Phase-2 timer is still re-armed by the
// heartbeats). It names BOTH plausible causes (the source may be throttled
// OR genuinely idle — heartbeats alone can't tell them apart, because
// vtgate strips the tablet's in-band `VEvent.throttled` flag) and points
// the operator at the out-of-band check on the PRIMARY.
//
// (item 19(a) part 3, DEFERRED): a best-effort `SHOW VITESS_THROTTLED_APPS`
// poll on a primary-routed connection could name the specific denied
// streamer app (vstreamer/rowstreamer/vreplication) here — but the verified
// finding is that it only reflects an EXPLICIT app-deny, NOT the common
// threshold/lag throttle, so it is an enrichment, not the primary signal.
// It needs a DB connection threaded into the watchdog (which today owns no
// conn), so it is left out to keep the watchdog dependency-free; the
// heuristic WARN is the core deliverable and stands alone.
func vstreamIdleWarnMessage(window time.Duration, keyspace string, shards []string) string {
	return fmt.Sprintf(
		"mysql/vstream: alive (heartbeats flowing) but NO change events for %s on keyspace %q shards %v — "+
			"the source may be throttled or genuinely idle; check `SHOW VITESS_THROTTLED_APPS` on the primary tablet "+
			"(the stream stays connected and will catch up when events resume)",
		window, keyspace, shards,
	)
}

// vstreamLivenessWindowFromDSN reads the optional vstream_liveness_timeout
// DSN parameter (a Go duration string, e.g. "45s") — the PHASE-1 window.
// Absent ⇒ the default window. A 0/negative duration disables the
// watchdog entirely. A malformed value is a loud error rather than a
// silent fallback (the loud-failure tenet: an operator who set the knob
// deserves to know it didn't parse).
func vstreamLivenessWindowFromDSN(cfg *gomysql.Config) (time.Duration, error) {
	return vstreamDurationParam(cfg, "vstream_liveness_timeout", defaultVStreamLivenessWindow)
}

// vstreamProgressWindowFromDSN reads the optional vstream_progress_timeout
// DSN parameter (a Go duration string) — the PHASE-2 (mid-stream
// progress) window for the CDC-TAIL pumps. Absent ⇒
// [defaultVStreamProgressWindow]. A 0/negative duration disables Phase 2
// only (Phase 1 still guards the first event). Malformed ⇒ loud error.
func vstreamProgressWindowFromDSN(cfg *gomysql.Config) (time.Duration, error) {
	return vstreamDurationParam(cfg, "vstream_progress_timeout", defaultVStreamProgressWindow)
}

// vstreamIdleWarnWindowFromDSN reads the optional vstream_idle_warn_timeout
// DSN parameter (a Go duration string) — the Phase-2 SOFT idle-WARN
// sub-window (item 19(a)). Absent ⇒ [defaultVStreamIdleWarnWindow]. A
// 0/negative duration disables the soft WARN only (both hard phases
// unaffected). Malformed ⇒ loud error.
func vstreamIdleWarnWindowFromDSN(cfg *gomysql.Config) (time.Duration, error) {
	return vstreamDurationParam(cfg, "vstream_idle_warn_timeout", defaultVStreamIdleWarnWindow)
}

// vstreamCopyProgressWindowFromDSN reads the optional
// vstream_copy_progress_timeout DSN parameter — the PHASE-2 window for the
// snapshot COPY pump (deliberately generous; see
// [defaultVStreamCopyProgressWindow]). Absent ⇒ that default. 0/negative
// disables Phase 2 for the COPY pump only. Malformed ⇒ loud error.
func vstreamCopyProgressWindowFromDSN(cfg *gomysql.Config) (time.Duration, error) {
	return vstreamDurationParam(cfg, "vstream_copy_progress_timeout", defaultVStreamCopyProgressWindow)
}

// vstreamDurationParam is the shared parse for the liveness/progress DSN
// duration knobs: absent ⇒ def; present ⇒ parsed (0/negative passes
// through to mean "disable"); malformed ⇒ loud error naming the param.
func vstreamDurationParam(cfg *gomysql.Config, param string, def time.Duration) (time.Duration, error) {
	v := cfg.Params[param]
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("mysql/vstream: invalid %s %q (want a Go duration like 45s, or 0 to disable): %w", param, v, err)
	}
	return d, nil
}

// vstreamTabletTypeFromDSN reads the optional vstream_tablet_type DSN
// parameter and maps it to the proto tablet type for the pure-CDC-tail
// VStream request. Valid values: primary | replica | rdonly (default
// replica — unchanged for PlanetScale production, which reads from
// replicas). An unrecognized value is a LOUD error (the loud-failure
// tenet) rather than a silent fallback to the default.
//
// This is the usability half of the primary-only fix: a primary-only
// cluster works via vstream_tablet_type=primary. The COPY-resume PRIMARY
// override in [buildVStreamRequest] still wins when a cursor is present —
// this only selects the tablet for pure CDC tailing.
func vstreamTabletTypeFromDSN(cfg *gomysql.Config) (topodata.TabletType, error) {
	switch cfg.Params["vstream_tablet_type"] {
	case "", "replica":
		return topodata.TabletType_REPLICA, nil
	case "primary":
		return topodata.TabletType_PRIMARY, nil
	case "rdonly":
		return topodata.TabletType_RDONLY, nil
	default:
		return topodata.TabletType_UNKNOWN, fmt.Errorf(
			"mysql/vstream: unknown vstream_tablet_type %q (want primary, replica, or rdonly)",
			cfg.Params["vstream_tablet_type"],
		)
	}
}
