// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"sync"
	"time"
)

// SyncRunner is the unit of work the [Supervisor] manages: one full
// continuous-sync stream (cold-start → CDC, with its own internal
// retry). *Streamer satisfies it via Run(ctx). It is an interface so
// the supervisor's failure-isolation logic can be unit-tested with a
// stub that fails / blocks on demand, without booting a real pipeline.
type SyncRunner interface {
	Run(ctx context.Context) error
}

// SupervisedSync pairs a stream id with its runner. The id is the
// stream-id under which position is persisted and the key the
// supervisor reports status under; it must be unique across the fleet
// (the config loader refuses duplicates — a shared id clobbers the
// per-target position row, ADR-0122 §4).
//
// Fingerprint is a stable hash of the resolved spec the runner was
// built from. It is consumed ONLY by [Supervisor.Reconcile] to decide
// whether a sync that is present in both the live and the reloaded fleet
// CHANGED (different fingerprint → stop+restart) or is UNCHANGED (same
// fingerprint → leave running untouched). It is optional: the initial
// [Supervisor.Run] path never reads it, and two empty fingerprints
// compare equal (treated as unchanged), so callers that don't hot-reload
// can ignore it.
type SupervisedSync struct {
	ID          string
	Runner      SyncRunner
	Fingerprint string
}

// SyncState is the lifecycle phase the supervisor reports for one
// supervised sync.
type SyncState string

const (
	// SyncStarting is the pre-first-run state.
	SyncStarting SyncState = "starting"
	// SyncRunning means the runner's Run is executing.
	SyncRunning SyncState = "running"
	// SyncBackoff means the runner failed and the supervisor is
	// waiting out the backoff before restarting it.
	SyncBackoff SyncState = "backoff"
	// SyncFailed is terminal: the runner exhausted
	// RestartPolicy.MaxConsecutiveFailures and the supervisor gave up
	// restarting it. Peers are unaffected (failure isolation).
	SyncFailed SyncState = "failed"
	// SyncStopped is terminal-clean: the runner returned nil (graceful
	// drain) or the supervisor's ctx was cancelled.
	SyncStopped SyncState = "stopped"
)

// RestartPolicy governs how a failed supervised sync is restarted.
// The zero value is NOT the intended default — use withDefaults (the
// supervisor applies it). MaxConsecutiveFailures=0 is deliberately the
// safe default: restart forever with the capped backoff, so a sync
// whose source recovers comes back on its own.
type RestartPolicy struct {
	// BackoffBase is the first restart delay; it doubles each
	// consecutive failure up to BackoffCap.
	BackoffBase time.Duration
	// BackoffCap bounds the exponential backoff.
	BackoffCap time.Duration
	// HealthyRunThreshold is the run duration past which a sync's
	// consecutive-failure counter resets before counting the new
	// failure — a sync that ran healthy for a long time then died
	// doesn't carry restart debt (mirrors ADR-0038's reset-on-progress).
	HealthyRunThreshold time.Duration
	// MaxConsecutiveFailures is the number of back-to-back failures
	// after which the supervisor gives up and marks the sync failed
	// (isolated; peers continue). 0 = unbounded (restart forever).
	MaxConsecutiveFailures int
}

// withDefaults returns p with any zero field filled from the standard
// policy. MaxConsecutiveFailures intentionally has no non-zero default
// (0 = unbounded is the safe behaviour).
func (p RestartPolicy) withDefaults() RestartPolicy {
	if p.BackoffBase <= 0 {
		p.BackoffBase = time.Second
	}
	if p.BackoffCap <= 0 {
		p.BackoffCap = 30 * time.Second
	}
	if p.BackoffCap < p.BackoffBase {
		p.BackoffCap = p.BackoffBase
	}
	if p.HealthyRunThreshold <= 0 {
		p.HealthyRunThreshold = time.Minute
	}
	if p.MaxConsecutiveFailures < 0 {
		p.MaxConsecutiveFailures = 0
	}
	return p
}

// SyncStatusSnapshot is an immutable point-in-time view of one
// supervised sync's runtime state, returned by [Supervisor.Snapshot].
type SyncStatusSnapshot struct {
	ID                  string
	State               SyncState
	ConsecutiveFailures int
	Restarts            int
	LastError           string
	LastStart           time.Time
	Since               time.Time
}

// supervisedState is the mutable per-sync state, guarded by the
// supervisor's mutex. Each sync goroutine writes only its own entry;
// Snapshot reads every entry under the same lock.
type supervisedState struct {
	state       SyncState
	consecutive int
	restarts    int
	lastErr     error
	lastStart   time.Time
	since       time.Time
}

// syncHandle is a live sync goroutine's control surface: cancel its
// derived context to stop it, then wait on done for the goroutine to
// fully unwind (so a restarted sync's predecessor has released its
// resources — e.g. a PG replication slot — before the replacement
// starts).
type syncHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Supervisor runs N independent syncs in one process, each in its own
// goroutine, each failure-isolated and restarted on a bounded backoff
// (ADR-0122). A clean ctx-cancel stops all of them. On SIGHUP the CLI
// re-reads the fleet config and calls [Supervisor.Reconcile] to add /
// remove / restart syncs WITHOUT a full process restart.
type Supervisor struct {
	policy RestartPolicy
	clock  func() time.Time

	// initial is the fleet Run starts with; Reconcile works against the
	// live `running` set thereafter.
	initial []SupervisedSync

	// reconcileMu serializes Reconcile against itself AND against Run's
	// idle-completion decision, so a stop-then-start reconcile can never
	// be mistaken for the fleet naturally going idle (which would make
	// Run return mid-reload). It is the coarse "fleet-shape change" lock;
	// mu remains the fine-grained state lock.
	reconcileMu sync.Mutex

	mu       sync.Mutex
	fleetCtx context.Context //nolint:containedctx // parent of every per-sync ctx; set once by Run, read by start/Reconcile
	running  map[string]*syncHandle
	states   map[string]*supervisedState
	order    []string // stable display order for Snapshot; reconcile appends/removes
	fps      map[string]string
	wg       sync.WaitGroup
	wake     chan struct{} // buffered(1); a goroutine pings it on exit so Run can re-check for completion
}

// NewSupervisor builds a supervisor over the given syncs and restart
// policy (defaults applied). Duplicate ids are the config loader's
// responsibility to refuse; if two slip through they share a status
// entry (the run loops are still independent).
func NewSupervisor(syncs []SupervisedSync, policy RestartPolicy) *Supervisor {
	s := &Supervisor{
		policy:  policy.withDefaults(),
		clock:   time.Now,
		initial: syncs,
		running: make(map[string]*syncHandle, len(syncs)),
		states:  make(map[string]*supervisedState, len(syncs)),
		fps:     make(map[string]string, len(syncs)),
		wake:    make(chan struct{}, 1),
	}
	for _, sy := range syncs {
		s.registerLocked(sy)
	}
	return s
}

// Run launches every initial sync in its own goroutine and blocks until
// the fleet stops. On ctx cancel (Ctrl-C / SIGTERM) every sync's loop
// stops and Run returns nil. If ctx is still live but every sync has
// ended on its own (all drained or permanently failed), Run returns the
// aggregated error of the failed syncs — so a single-sync fleet that
// can't start exits non-zero, while a fleet where one sync fails and
// others run on keeps blocking (the failed peer is isolated, not fatal).
func (s *Supervisor) Run(ctx context.Context) error {
	s.mu.Lock()
	if len(s.initial) == 0 {
		s.mu.Unlock()
		return errors.New("supervisor: no syncs configured")
	}
	s.fleetCtx = ctx
	for _, sy := range s.initial {
		s.launchLocked(sy)
	}
	n := len(s.initial)
	s.mu.Unlock()

	slog.InfoContext(ctx, "supervisor: starting fleet", slog.Int("syncs", n))

	for {
		select {
		case <-ctx.Done():
			// A ctx cancel (Ctrl-C / SIGTERM) is a clean fleet shutdown:
			// wait for every goroutine (incl. any started by a concurrent
			// reconcile) to unwind, then return nil rather than ctx.Err().
			s.wg.Wait()
			slog.InfoContext(ctx, "supervisor: fleet stopped (context cancelled)")
			return nil //nolint:nilerr // ctx-cancel is a clean stop, not a failure
		case <-s.wake:
			// A sync goroutine exited. If the fleet has naturally gone idle
			// (no syncs left and no reconcile in flight) we're done.
			if s.idleComplete() {
				s.wg.Wait()
				return s.aggregateFailures()
			}
		}
	}
}

// idleComplete reports whether the fleet has run dry — every sync ended
// on its own and no reconcile is mid-flight. Holding reconcileMu is what
// makes a stop-then-start reconcile atomic with respect to this check:
// if a reconcile is applying, idleComplete blocks until it finishes, by
// which point `running` reflects the post-reconcile set (non-empty for a
// valid reload), so Run does not spuriously return mid-reload.
func (s *Supervisor) idleComplete() bool {
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.running) == 0
}

// ReconcileResult reports what a [Supervisor.Reconcile] applied. Slices
// are sorted for deterministic logging and testing.
type ReconcileResult struct {
	Started   []string
	Stopped   []string
	Restarted []string
	Unchanged []string
}

// changed reports whether the reconcile actually mutated the fleet.
func (r ReconcileResult) changed() bool {
	return len(r.Started)+len(r.Stopped)+len(r.Restarted) > 0
}

// Reconcile diffs newSyncs (the freshly-reloaded fleet) against the live
// set by stream-id and applies the difference WITHOUT a process restart:
//
//   - present in newSyncs, not running → START
//   - running, absent from newSyncs   → STOP (graceful drain)
//   - present in both, fingerprint changed → RESTART (stop old, start new)
//   - present in both, fingerprint equal   → untouched
//
// THE load-bearing property (ADR-0122 §3, hot-reload): if newSyncs is
// invalid (duplicate or empty stream-id), Reconcile refuses the WHOLE
// reload — it returns an error and leaves the live fleet running exactly
// as it was. The malformed set is validated UP FRONT, before any sync is
// stopped or started, so a bad reload can never half-apply or take the
// live fleet down. (The CLI also re-runs the full slot-name + stream-id
// config validators before calling Reconcile; this is the supervisor's
// own defense-in-depth invariant.)
//
// Stops complete fully — the goroutine exits and its resources release —
// before the matching start runs, so a restarted Postgres sync releases
// and reacquires its replication slot cleanly.
func (s *Supervisor) Reconcile(newSyncs []SupervisedSync) (ReconcileResult, error) {
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()

	// Validate the new set BEFORE touching the live fleet. Any violation
	// returns here with the live set wholly unmutated.
	newByID := make(map[string]SupervisedSync, len(newSyncs))
	for _, sy := range newSyncs {
		if sy.ID == "" {
			return ReconcileResult{}, errors.New("supervisor: reload refused (live fleet unchanged): a sync has an empty stream-id")
		}
		if _, dup := newByID[sy.ID]; dup {
			return ReconcileResult{}, fmt.Errorf(
				"supervisor: reload refused (live fleet unchanged): duplicate stream-id %q in the new fleet",
				sy.ID,
			)
		}
		newByID[sy.ID] = sy
	}

	s.mu.Lock()
	if s.fleetCtx == nil {
		s.mu.Unlock()
		return ReconcileResult{}, errors.New("supervisor: Reconcile called before Run")
	}
	var res ReconcileResult
	for _, sy := range newSyncs {
		switch _, live := s.running[sy.ID]; {
		case !live:
			// Not currently running: a brand-new sync, or one that ended on
			// its own (failed/drained) and is back in the config — (re)start.
			res.Started = append(res.Started, sy.ID)
		case s.fps[sy.ID] != sy.Fingerprint:
			res.Restarted = append(res.Restarted, sy.ID)
		default:
			res.Unchanged = append(res.Unchanged, sy.ID)
		}
	}
	// Anything currently known (running OR already terminal) that the new
	// config drops is removed from the fleet. stopAndWait gracefully drains
	// a live one and is a no-op for an already-exited one; either way
	// forgetLocked clears it from the view.
	for _, id := range s.order {
		if _, keep := newByID[id]; !keep {
			res.Stopped = append(res.Stopped, id)
		}
	}
	s.mu.Unlock()

	sort.Strings(res.Started)
	sort.Strings(res.Stopped)
	sort.Strings(res.Restarted)
	sort.Strings(res.Unchanged)

	// Apply: removals first (free any slots), then restarts (each stops
	// then starts its own stream), then fresh starts.
	for _, id := range res.Stopped {
		s.stopAndWait(id)
		s.mu.Lock()
		s.forgetLocked(id)
		s.mu.Unlock()
	}
	for _, id := range res.Restarted {
		s.stopAndWait(id)
		s.mu.Lock()
		s.registerLocked(newByID[id])
		s.launchLocked(newByID[id])
		s.mu.Unlock()
	}
	for _, id := range res.Started {
		s.mu.Lock()
		s.registerLocked(newByID[id])
		s.launchLocked(newByID[id])
		s.mu.Unlock()
	}

	if !res.changed() {
		slog.Info("supervisor: reload applied — no changes", slog.Int("running", len(res.Unchanged)))
	} else {
		slog.Info(
			"supervisor: reload applied",
			slog.Any("started", res.Started),
			slog.Any("stopped", res.Stopped),
			slog.Any("restarted", res.Restarted),
			slog.Int("unchanged", len(res.Unchanged)),
		)
	}
	return res, nil
}

// registerLocked (re)initialises a sync's status to starting, records
// its fingerprint, and appends its id to the display order if new. Caller
// holds s.mu.
func (s *Supervisor) registerLocked(sy SupervisedSync) {
	if _, ok := s.states[sy.ID]; !ok {
		s.order = append(s.order, sy.ID)
	}
	s.states[sy.ID] = &supervisedState{state: SyncStarting, since: s.clock()}
	s.fps[sy.ID] = sy.Fingerprint
}

// launchLocked starts a sync's supervise goroutine under a context
// derived from the fleet ctx, so either a fleet-wide cancel OR a
// per-sync reconcile-stop unwinds it. Caller holds s.mu.
func (s *Supervisor) launchLocked(sy SupervisedSync) {
	ctx, cancel := context.WithCancel(s.fleetCtx)
	h := &syncHandle{cancel: cancel, done: make(chan struct{})}
	s.running[sy.ID] = h
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(h.done)
		defer s.goroutineExited(sy.ID)
		s.superviseOne(ctx, sy)
	}()
}

// stopAndWait cancels a live sync and blocks until its goroutine has
// fully unwound (releasing its resources). The s.mu lock is NOT held
// while waiting — the goroutine needs it to record its final state. A
// no-op if the id isn't running.
func (s *Supervisor) stopAndWait(id string) {
	s.mu.Lock()
	h := s.running[id]
	s.mu.Unlock()
	if h == nil {
		return
	}
	h.cancel()
	<-h.done
}

// forgetLocked drops every trace of a removed sync so it disappears from
// the fleet view. Caller holds s.mu. (Its running entry is already gone —
// goroutineExited removed it when the goroutine exited.)
func (s *Supervisor) forgetLocked(id string) {
	delete(s.states, id)
	delete(s.fps, id)
	delete(s.running, id)
	for i, oid := range s.order {
		if oid == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

// goroutineExited removes a sync from the live set and, if the fleet has
// gone empty, pings Run to re-check for completion. Runs as the sync
// goroutine's deferred cleanup.
func (s *Supervisor) goroutineExited(id string) {
	s.mu.Lock()
	delete(s.running, id)
	empty := len(s.running) == 0
	s.mu.Unlock()
	if empty {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

// superviseOne is the per-sync supervise loop: run, classify the exit,
// and either stop (clean / ctx-cancel), give up (failure cap reached,
// isolated), or back off and restart.
func (s *Supervisor) superviseOne(ctx context.Context, sy SupervisedSync) {
	for {
		if ctx.Err() != nil {
			s.setState(sy.ID, SyncStopped, nil)
			return
		}

		s.markRunning(sy.ID)
		start := s.clock()
		err := runGuarded(ctx, sy.Runner)
		ran := s.clock().Sub(start)

		// A ctx cancel during the run is a clean shutdown regardless of
		// what the runner returned (the error is just the unwinding).
		if ctx.Err() != nil {
			s.setState(sy.ID, SyncStopped, nil)
			return
		}
		// A nil return is a graceful drain (operator `sync stop`): done.
		if err == nil {
			slog.InfoContext(ctx, "supervisor: sync drained cleanly", slog.String("stream_id", sy.ID))
			s.setState(sy.ID, SyncStopped, nil)
			return
		}

		// Failure with a live ctx: count it, isolating the failure to
		// this goroutine. Reset the consecutive counter first if the
		// sync ran healthy for long enough before dying.
		consecutive := s.recordFailure(sy.ID, err, ran >= s.policy.HealthyRunThreshold)

		if s.policy.MaxConsecutiveFailures > 0 && consecutive >= s.policy.MaxConsecutiveFailures {
			slog.ErrorContext(
				ctx, "supervisor: sync permanently failed; isolating (peers unaffected)",
				slog.String("stream_id", sy.ID),
				slog.Int("consecutive_failures", consecutive),
				slog.String("err", err.Error()),
			)
			s.setState(sy.ID, SyncFailed, err)
			return
		}

		backoff := backoffFor(consecutive, s.policy)
		slog.WarnContext(
			ctx, "supervisor: sync failed; restarting after backoff",
			slog.String("stream_id", sy.ID),
			slog.Int("consecutive_failures", consecutive),
			slog.Duration("backoff", backoff),
			slog.String("err", err.Error()),
		)
		s.setState(sy.ID, SyncBackoff, err)

		select {
		case <-ctx.Done():
			s.setState(sy.ID, SyncStopped, nil)
			return
		case <-time.After(backoff):
		}
	}
}

// runGuarded invokes the runner and converts a panic into an error so a
// panicking sync can NEVER crash the process and take down its peers —
// the single most important isolation guarantee. The stack is logged at
// ERROR; the returned error carries the panic value for the restart log.
func runGuarded(ctx context.Context, r SyncRunner) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			slog.ErrorContext(
				ctx, "supervisor: sync panicked (recovered; isolated)",
				slog.Any("panic", rec),
				slog.String("stack", string(debug.Stack())),
			)
			err = fmt.Errorf("supervisor: sync panicked: %v", rec)
		}
	}()
	return r.Run(ctx)
}

// backoffFor computes the exponential backoff for the consecutive-th
// failure, capped at policy.BackoffCap. consecutive is 1-based.
func backoffFor(consecutive int, policy RestartPolicy) time.Duration {
	d := policy.BackoffBase
	for i := 1; i < consecutive; i++ {
		d *= 2
		if d >= policy.BackoffCap {
			return policy.BackoffCap
		}
	}
	if d > policy.BackoffCap {
		return policy.BackoffCap
	}
	return d
}

// markRunning transitions a sync to running and stamps its start time.
func (s *Supervisor) markRunning(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.states[id]
	if st == nil {
		return
	}
	st.state = SyncRunning
	st.lastStart = s.clock()
	st.since = st.lastStart
}

// recordFailure increments the failure counters for a sync (resetting
// the consecutive count first when the prior run was healthy) and
// returns the new consecutive-failure count.
func (s *Supervisor) recordFailure(id string, err error, healthyRun bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.states[id]
	if st == nil {
		return 0
	}
	if healthyRun {
		st.consecutive = 0
	}
	st.consecutive++
	st.restarts++
	st.lastErr = err
	return st.consecutive
}

// setState transitions a sync to a new state, recording err (when
// non-nil) as its last error.
func (s *Supervisor) setState(id string, state SyncState, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.states[id]
	if st == nil {
		return
	}
	st.state = state
	st.since = s.clock()
	if err != nil {
		st.lastErr = err
	}
}

// Snapshot returns an immutable copy of every supervised sync's current
// state, in the fleet's display order. Safe to call concurrently with
// the supervise loops and with Reconcile.
func (s *Supervisor) Snapshot() []SyncStatusSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SyncStatusSnapshot, 0, len(s.order))
	for _, id := range s.order {
		st := s.states[id]
		if st == nil {
			continue
		}
		snap := SyncStatusSnapshot{
			ID:                  id,
			State:               st.state,
			ConsecutiveFailures: st.consecutive,
			Restarts:            st.restarts,
			LastStart:           st.lastStart,
			Since:               st.since,
		}
		if st.lastErr != nil {
			snap.LastError = st.lastErr.Error()
		}
		out = append(out, snap)
	}
	return out
}

// aggregateFailures joins the last errors of every sync that ended in
// the failed state. Returns nil when none failed.
func (s *Supervisor) aggregateFailures() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for _, id := range s.order {
		st := s.states[id]
		if st != nil && st.state == SyncFailed && st.lastErr != nil {
			errs = append(errs, fmt.Errorf("sync %q: %w", id, st.lastErr))
		}
	}
	return errors.Join(errs...)
}
