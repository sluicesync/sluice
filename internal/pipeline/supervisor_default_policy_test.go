// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// What the SHIPPED default actually does (audit 2026-08-04).
//
// TestSupervisor_SingleSyncFailureSurfaces pins that a permanently-failed
// single-sync fleet returns its error — under fastTestPolicy(2). The shipped
// default is MaxConsecutiveFailures 0 (RestartConfig in cmd/sluice/sync_run.go
// carries no default tag, so the Go zero value ships), and the terminal branch
// is guarded on `> 0`. So the existing gate proves a property under a policy
// no deployment takes, which is item 104's self-referential-fixture shape
// pointed at the loud-failure boundary rather than at a hash.
//
// This is the companion that runs at the real default. It does not assert the
// behaviour is GOOD — restart-forever is a defensible supervisor default and
// is not changed here — only that it is what ships, so the pair together say
// "under a positive limit it exits; under the default it does not", which is
// the true statement neither test made alone.

package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestSupervisor_DefaultPolicyRestartsForever pins the unreachable-terminal
// property at the shipped default.
func TestSupervisor_DefaultPolicyRestartsForever(t *testing.T) {
	var attempts int
	bad := SupervisedSync{
		ID: "never-starts",
		Runner: runnerFunc(func(_ context.Context) error {
			attempts++
			return errors.New("permanent: target unreachable")
		}),
	}

	// The shipped default for the fields an operator does not set. Only the
	// backoff is shortened, so the test finishes — the failure LIMIT is left
	// at its real zero value, which is the whole point.
	policy := RestartPolicy{
		BackoffBase:         time.Millisecond,
		BackoffCap:          2 * time.Millisecond,
		HealthyRunThreshold: time.Hour,
		// MaxConsecutiveFailures deliberately left at 0 — the shipped default.
	}
	if policy.MaxConsecutiveFailures != 0 {
		t.Fatal("this test is meaningless unless MaxConsecutiveFailures is the zero value")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	sup := NewSupervisor([]SupervisedSync{bad}, policy)
	err := sup.Run(ctx)
	// Run returns nil on ctx cancel. The sync never became terminal, so the
	// aggregated-failure path was never reached.
	if err != nil {
		t.Fatalf("Supervisor.Run = %v, want nil.\n\n"+
			"If this now returns an error, the default failure policy changed — which may well be an "+
			"improvement, but it is a BEHAVIOUR CHANGE for every existing `sluice sync run` deployment "+
			"and needs release notes. Update this test and Run's doc together.", err)
	}

	// Non-vacuity: the runner must actually have been retried, or this test
	// would pass against a supervisor that never ran anything at all.
	if attempts < 2 {
		t.Errorf("runner attempted %d time(s); the supervisor is not retrying, so this test proves nothing "+
			"about the restart-forever property it exists to pin", attempts)
	}
}

// TestSupervisor_PositiveLimitStillExits is the other half, kept adjacent so
// the pair reads as one statement. A positive limit must still surface the
// failure — otherwise the escape hatch named in Run's doc does not exist.
func TestSupervisor_PositiveLimitStillExits(t *testing.T) {
	sentinel := errors.New("permanent: schema mismatch")
	bad := SupervisedSync{
		ID:     "bad",
		Runner: runnerFunc(func(_ context.Context) error { return sentinel }),
	}
	err := NewSupervisor([]SupervisedSync{bad}, fastTestPolicy(2)).Run(context.Background())
	if err == nil {
		t.Fatal("with max-consecutive-failures=2 a permanently-failing single-sync fleet returned nil; the " +
			"documented escape hatch from restart-forever does not work")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error = %v; want it to wrap %v", err, sentinel)
	}
}
