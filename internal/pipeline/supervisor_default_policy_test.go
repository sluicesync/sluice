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

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
	"sluicesync.dev/sluice/internal/sluicecode"
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
		{"plain", "on public.t: a CHECK changed", syncUnforwardedRefusalRemedy, backfillIncompleteRepair},
		{"backfill", addColumnBackfillIncompleteMarker + ": public.t (c): the backfill stopped early", backfillIncompleteRepair, syncUnforwardedRefusalRemedy},
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
		t.Errorf("a generic terminal failure was run %d time(s); want it restarted — the no-restart rule is scoped to "+
			"UNFORWARDED-SCHEMA-CHANGE and refusalsARestartRepeats", attempts)
	}
}

// TestSupervisor_RefusalsARestartRepeatsAreNotRestarted pins, one sentinel at
// a time and at the shipped restart-forever default, that a leg ending on a
// refusal a restart would repeat is run ONCE, marked failed, and logged with
// its marker. Each case wraps the sentinel the way its engine does (inside a
// terminal error, under a pipeline frame); APPLY-MARK-MISMATCH uses the real
// applymarks.RefusalError, which matches through its Is method rather than a
// %w chain, and KEY-SCOPED-WRITE-MATCHED-MULTIPLE-ROWS the real coded refusal
// from appliershared. The roster floor keeps a new list entry from going
// unpinned; TestSupervisor_RefusalsARestartCanClearAreStillRestarted is the
// other direction.
func TestSupervisor_RefusalsARestartRepeatsAreNotRestarted(t *testing.T) {
	cases := map[string]error{
		"SLOT-ACKED-PAST-TARGET-POSITION": fmt.Errorf("postgres: %w: replication slot \"s\": %w", ir.ErrSlotAckedPastTargetPosition, terminalTestErr{"refused"}),
		"SHARDED-TARGET-VINDEX-UPDATE":    fmt.Errorf("%w: remedy: %w", ir.ErrShardedTargetVindexUpdate, terminalTestErr{"VT12001"}),
		"APPLY-MARK-MISMATCH":             &applymarks.RefusalError{},
		"CHARSET-NOT-DECODABLE":           fmt.Errorf("%w: table \"t\" column \"c\"", ir.ErrCharsetNotDecodable),
		"DSN-TIME-ZONE-NOT-UTC":           fmt.Errorf("mysql: %w: refusing the DSN parameter time_zone=x", ir.ErrDSNTimeZoneNotUTC),
		"CHANGE-LOG-WATERMARK-STALLED":    fmt.Errorf("pgtrigger: %w: the change-log poll read rows up to id 10003 with no gap", ir.ErrChangeLogWatermarkStalled),
		"HEARTBEAT-TABLE-NOT-SLUICES":     fmt.Errorf("pipeline: source heartbeat: mysql: %w: table `orders` already exists", ir.ErrHeartbeatTableNotSluices),
		// Built by the real constructor the first-boundary witness calls.
		"RESUME-SCHEMA-DIVERGENCE": resumeDivergenceRefusal("public.t",
			witnessVerdict{kind: witnessRefuse, diffs: []witnessColumnDiff{{"a", "(absent)", "Int32"}}}, "recovery: drained model"),
		// Built by the real judgement the unforwarded-stream check settles.
		"SCHEMA-CHANGE-REFUSED": unforwardedJudgement{refused: []witnessColumnDiff{{"ts", "DateTime(6)", "DateTime(0)"}}}.
			settle(context.Background(), "public.t", "--schema-changes=refuse"),
		// Coded (SLUICE-E-CDC-KEY-MATCHED-MULTIPLE-ROWS): built by the real
		// shared constructor every applier calls, so the alias between the
		// appliershared and ir sentinels is part of what is pinned.
		"KEY-SCOPED-WRITE-MATCHED-MULTIPLE-ROWS": appliershared.RefuseKeyScopedMultiMatch("postgres", "delete", "public", "t", ir.Row{"id": int64(2)}, 2),
	}

	if len(cases) != len(refusalsARestartRepeats) {
		t.Fatalf("%d cases for %d refusalsARestartRepeats entries; pin every entry", len(cases), len(refusalsARestartRepeats))
	}
	for _, sentinel := range refusalsARestartRepeats {
		if _, ok := cases[sentinel.Error()]; !ok {
			t.Errorf("refusalsARestartRepeats entry %q has no case", sentinel.Error())
		}
	}

	for marker, refusal := range cases {
		t.Run(marker, func(t *testing.T) {
			prev := slog.Default()
			t.Cleanup(func() { slog.SetDefault(prev) })
			var logBuf logcapture.Buffer
			slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))

			var attempts int
			sy := SupervisedSync{
				ID: "refused",
				Runner: runnerFunc(func(_ context.Context) error {
					attempts++
					return fmt.Errorf("pipeline: sync: %w", refusal)
				}),
			}
			policy := RestartPolicy{BackoffBase: time.Millisecond, BackoffCap: 2 * time.Millisecond, HealthyRunThreshold: time.Hour}
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			sup := NewSupervisor([]SupervisedSync{sy}, policy)
			_ = sup.Run(ctx)

			if attempts != 1 {
				t.Errorf("%s was run %d times; want exactly 1 — every restart refuses again", marker, attempts)
			}
			if snap := sup.Snapshot(); len(snap) != 1 || snap[0].State != SyncFailed {
				t.Errorf("snapshot = %+v; want the sync in state %q", snap, SyncFailed)
			}
			if !strings.Contains(logBuf.String(), "not restarting") || !strings.Contains(logBuf.String(), "marker="+marker) {
				t.Errorf("no not-restarting line naming marker=%s; log:\n%s", marker, logBuf.String())
			}
		})
	}
}

// TestSupervisor_RefusalsARestartCanClearAreStillRestarted pins the other half
// of the list's decisions: a coded refusal that clears without a change to the
// leg, and the Postgres DEFERRED-KEY-CHECK-FAILED-AT-COMMIT refusal (whose
// batch-loop cause depends on arrival timing), are restarted under the cap
// like any other failure. Each wraps the same way its raise site does.
func TestSupervisor_RefusalsARestartCanClearAreStillRestarted(t *testing.T) {
	cases := map[string]error{
		"SLUICE-E-CDC-REPLICATION-HEADROOM": sluicecode.Wrap(sluicecode.CodeCDCReplicationHeadroom, "free a slot",
			terminalTestErr{"max_replication_slots exhausted"}),
		"DEFERRED-KEY-CHECK-FAILED-AT-COMMIT": fmt.Errorf("postgres: applier: commit: %w",
			fmt.Errorf("DEFERRED-KEY-CHECK-FAILED-AT-COMMIT: a DEFERRABLE constraint's commit-time check refused the target transaction (t_pk): %w",
				terminalTestErr{"ERROR: duplicate key value violates unique constraint \"t_pk\" (SQLSTATE 23505)"})),
	}
	for name, refusal := range cases {
		t.Run(name, func(t *testing.T) {
			var attempts int
			sy := SupervisedSync{
				ID: "clears",
				Runner: runnerFunc(func(_ context.Context) error {
					attempts++
					return fmt.Errorf("pipeline: sync: %w", refusal)
				}),
			}
			policy := RestartPolicy{BackoffBase: time.Millisecond, BackoffCap: 2 * time.Millisecond, HealthyRunThreshold: time.Hour}
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			_ = NewSupervisor([]SupervisedSync{sy}, policy).Run(ctx)
			if attempts < 2 {
				t.Errorf("%s was run %d time(s); want it restarted — a restart can clear it", name, attempts)
			}
		})
	}
}
