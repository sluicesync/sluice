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
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
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

// terminalTestErr asserts [ir.TerminalError].
type terminalTestErr struct{ msg string }

func (e terminalTestErr) Error() string  { return e.msg }
func (e terminalTestErr) Terminal() bool { return true }

// TestSupervisor_UnforwardedRefusalIsNotRestarted pins the 2026-09-23 pre-tag
// review's F1: under the restart-forever default, an UNFORWARDED-SCHEMA-CHANGE
// refusal is run ONCE and marked failed — a restart could only refuse again,
// or, had the refusal's recording failed, re-baseline and accept the change.
// TestSupervisor_OtherTerminalFailuresAreStillRestarted is the control for
// the scope.
func TestSupervisor_UnforwardedRefusalIsNotRestarted(t *testing.T) {
	var attempts int
	refused := SupervisedSync{
		ID: "refused",
		Runner: runnerFunc(func(_ context.Context) error {
			attempts++
			return fmt.Errorf("pipeline: source cdc reader: %w", fmt.Errorf("%w on public.t: %w", ir.ErrUnforwardedSchemaChange, terminalTestErr{"refused"}))
		}),
	}
	policy := RestartPolicy{BackoffBase: time.Millisecond, BackoffCap: 2 * time.Millisecond, HealthyRunThreshold: time.Hour}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	sup := NewSupervisor([]SupervisedSync{refused}, policy)
	_ = sup.Run(ctx)

	if attempts != 1 {
		t.Errorf("a terminal failure was run %d times; want exactly 1 — a restart re-baselines the unforwarded-schema-change door and accepts the refused change", attempts)
	}
	snap := sup.Snapshot()
	if len(snap) != 1 || snap[0].State != SyncFailed {
		t.Errorf("snapshot = %+v; want the sync in state %q", snap, SyncFailed)
	}
}

// TestSupervisor_UnforwardedRefusalLogNamesTheRightRepair pins the fleet
// log's remedy per refusal kind. An interrupted added-column backfill wraps
// the same sentinel, but its column is already on the target and the repair
// is the SOURCE's values — "apply the change to the target" is the wrong
// instruction there (found by the v0.156.1 site-drift pass). The plain
// refusal is the control, so the branch cannot pass by always printing the
// backfill text.
func TestSupervisor_UnforwardedRefusalLogNamesTheRightRepair(t *testing.T) {
	cases := []struct {
		name, msg, want, notWant string
	}{
		{"plain", "on public.t: a CHECK changed", "apply the change to the target", "copy the added column"},
		{"backfill", addColumnBackfillIncompleteMarker + ": public.t (c): the backfill stopped early", "copy the added column's values from the source", "apply the change to the target"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prev := slog.Default()
			t.Cleanup(func() { slog.SetDefault(prev) })
			var logBuf logcapture.Buffer
			slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))

			sy := SupervisedSync{
				ID: "refused",
				Runner: runnerFunc(func(_ context.Context) error {
					return fmt.Errorf("%w: %s", ir.ErrUnforwardedSchemaChange, tc.msg)
				}),
			}
			policy := RestartPolicy{BackoffBase: time.Millisecond, BackoffCap: 2 * time.Millisecond, HealthyRunThreshold: time.Hour}
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			_ = NewSupervisor([]SupervisedSync{sy}, policy).Run(ctx)

			var line string
			for _, l := range strings.Split(logBuf.String(), "\n") {
				if strings.Contains(l, "not restarting") {
					line = l
				}
			}
			if line == "" {
				t.Fatalf("no supervisor refusal line logged; log:\n%s", logBuf.String())
			}
			if !strings.Contains(line, tc.want) || strings.Contains(line, tc.notWant) {
				t.Errorf("refusal line = %q; want it to contain %q and not %q", line, tc.want, tc.notWant)
			}
		})
	}
}

// TestSupervisor_OtherTerminalFailuresAreStillRestarted pins the SCOPE of
// the no-restart rule. A generic [ir.TerminalError] is terminal only to the
// in-process retry; several (a dead snapshot-pinned copy connection, an
// in-doubt raw-copy commit) are recovered by a fresh run, which is what the
// supervisor provides. Widening the rule to every terminal error — the first
// cut of F1 — silently took that recovery away from fleet legs.
func TestSupervisor_OtherTerminalFailuresAreStillRestarted(t *testing.T) {
	var attempts int
	sy := SupervisedSync{
		ID: "dead-snapshot",
		Runner: runnerFunc(func(_ context.Context) error {
			attempts++
			return fmt.Errorf("pipeline: copy: %w", terminalTestErr{"snapshot-pinned connection died; re-run the copy"})
		}),
	}
	policy := RestartPolicy{BackoffBase: time.Millisecond, BackoffCap: 2 * time.Millisecond, HealthyRunThreshold: time.Hour}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_ = NewSupervisor([]SupervisedSync{sy}, policy).Run(ctx)
	if attempts < 2 {
		t.Errorf("a generic terminal failure was run %d time(s); want it restarted — the no-restart rule is scoped to UNFORWARDED-SCHEMA-CHANGE", attempts)
	}
}
