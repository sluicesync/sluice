// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
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
type SupervisedSync struct {
	ID     string
	Runner SyncRunner
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

// Supervisor runs N independent syncs in one process, each in its own
// goroutine, each failure-isolated and restarted on a bounded backoff
// (ADR-0122). A clean ctx-cancel stops all of them.
type Supervisor struct {
	policy RestartPolicy
	syncs  []SupervisedSync

	clock func() time.Time

	mu     sync.Mutex
	states map[string]*supervisedState
}

// NewSupervisor builds a supervisor over the given syncs and restart
// policy (defaults applied). Duplicate ids are the config loader's
// responsibility to refuse; if two slip through they share a status
// entry (the run loops are still independent).
func NewSupervisor(syncs []SupervisedSync, policy RestartPolicy) *Supervisor {
	states := make(map[string]*supervisedState, len(syncs))
	now := time.Now()
	for _, sy := range syncs {
		states[sy.ID] = &supervisedState{state: SyncStarting, since: now}
	}
	return &Supervisor{
		policy: policy.withDefaults(),
		syncs:  syncs,
		clock:  time.Now,
		states: states,
	}
}

// Run launches every sync in its own goroutine and blocks until all of
// them exit. On ctx cancel (Ctrl-C / SIGTERM) every sync's loop stops
// and Run returns nil. If ctx is still live but every sync ended on its
// own (all drained or permanently failed), Run returns the aggregated
// error of the failed syncs — so a single-sync fleet that can't start
// exits non-zero, while a fleet where one sync fails and others run on
// keeps blocking (the failed peer is isolated, not fatal).
func (s *Supervisor) Run(ctx context.Context) error {
	if len(s.syncs) == 0 {
		return errors.New("supervisor: no syncs configured")
	}

	slog.InfoContext(ctx, "supervisor: starting fleet", slog.Int("syncs", len(s.syncs)))

	var wg sync.WaitGroup
	for _, sy := range s.syncs {
		wg.Add(1)
		go func(sy SupervisedSync) {
			defer wg.Done()
			s.superviseOne(ctx, sy)
		}(sy)
	}
	wg.Wait()

	if ctx.Err() != nil {
		slog.InfoContext(ctx, "supervisor: fleet stopped (context cancelled)")
		// A ctx cancel (Ctrl-C / SIGTERM) is a clean fleet shutdown, not
		// an error — peers drained on their own loops. Deliberately
		// return nil rather than ctx.Err().
		return nil //nolint:nilerr // ctx-cancel is a clean stop, not a failure
	}
	return s.aggregateFailures()
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
// state, in the fleet's configured order. Safe to call concurrently
// with the supervise loops.
func (s *Supervisor) Snapshot() []SyncStatusSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SyncStatusSnapshot, 0, len(s.syncs))
	for _, sy := range s.syncs {
		st := s.states[sy.ID]
		if st == nil {
			continue
		}
		snap := SyncStatusSnapshot{
			ID:                  sy.ID,
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
	for _, sy := range s.syncs {
		st := s.states[sy.ID]
		if st != nil && st.state == SyncFailed && st.lastErr != nil {
			errs = append(errs, fmt.Errorf("sync %q: %w", sy.ID, st.lastErr))
		}
	}
	return errors.Join(errs...)
}
