// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// cancellingProber stops the run while its probe is in flight and then
// fails the way the driver does in that window — the shapes measured against
// a real Postgres (see [ErrOrCancel]).
type cancellingProber struct {
	stop  func()
	err   error
	empty bool // answer IsTableEmpty instead of failing it
}

func (*cancellingProber) WriteRows(context.Context, *ir.Table, <-chan ir.Row) error { return nil }

func (p *cancellingProber) IsTableEmpty(context.Context, *ir.Table) (bool, error) {
	if p.empty {
		return false, nil
	}
	p.stop()
	return false, p.err
}

func (p *cancellingProber) ProbeReplayKey(context.Context, *ir.Table) (exists, keyed bool, err error) {
	p.stop()
	return false, false, p.err
}

// errPGXWriteTimeout is pgx's write-path error for a socket whose deadline the
// context watcher moved into the past: pgproto3's writeError around a
// *net.OpError i/o timeout. Neither unwraps to a context error.
var errPGXWriteTimeout = fmt.Errorf("existence: write failed: %w", &net.OpError{
	Op: "write", Net: "tcp", Err: os.ErrDeadlineExceeded,
})

// TestFindReplayKeylessTables_ProbeFailureUnderCancelIsTheCancel pins the
// v0.157.0 CI failure (TestBroker_CancelStorm_Postgres): a run cancelled
// while the keyless door probes the target must report the CANCEL — which the
// broker turns into its documented clean stop — not the driver's
// network-shaped failure. Both probes, both driver shapes, both ways a
// context ends.
func TestFindReplayKeylessTables_ProbeFailureUnderCancelIsTheCancel(t *testing.T) {
	shapes := map[string]error{
		"pgx write-path i/o timeout":  errPGXWriteTimeout,
		"database/sql bad connection": fmt.Errorf("generated columns: %w", driver.ErrBadConn),
	}
	ends := map[string]func() (context.Context, func()){
		"cancel": func() (context.Context, func()) {
			ctx, cancel := context.WithCancel(context.Background())
			return ctx, cancel
		},
		"deadline": func() (context.Context, func()) {
			// Already past: the probe below ignores ctx, as a driver
			// mid-write does, and fails after the deadline.
			return context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
		},
	}
	for shapeName, shape := range shapes {
		for _, probe := range []string{"key", "rows"} {
			for endName, end := range ends {
				t.Run(shapeName+"/"+probe+"/"+endName, func(t *testing.T) {
					ctx, stop := end()
					defer stop()
					p := &cancellingProber{stop: stop, err: shape, empty: probe == "key"}
					_, err := FindReplayKeylessTables(ctx, p, []*ir.Table{replayTable("su", true, nil)},
						ReplayJudgeOptions{ProbeTarget: true, OnlyNonEmpty: true})
					if err == nil {
						t.Fatal("FindReplayKeylessTables = nil; want the cancellation")
					}
					if !errors.Is(err, ctx.Err()) {
						t.Fatalf("a probe failure while the run is cancelled = %v; want it to unwrap to %v, "+
							"which the broker reports as a clean stop", err, ctx.Err())
					}
					if !strings.Contains(err.Error(), shape.Error()) {
						t.Errorf("the probe's own failure is missing from the message: %v", err)
					}
				})
			}
		}
	}
}

// deadlinePassedTimerPending is a context whose deadline has passed but whose
// timer has not fired yet: Err() still reads nil. A driver dial bounded by the
// same deadline can fail in exactly that instant.
type deadlinePassedTimerPending struct{ context.Context }

func (deadlinePassedTimerPending) Deadline() (time.Time, bool) {
	return time.Now().Add(-time.Millisecond), true
}

// TestErrOrCancel_DeadlinePassedBeforeItsTimerFired pins [ctxEnded]: a
// dial timeout that beats the context's own timer is still the deadline.
func TestErrOrCancel_DeadlinePassedBeforeItsTimerFired(t *testing.T) {
	ctx := deadlinePassedTimerPending{context.Background()}
	dial := errors.New("failed to connect: dial error: timeout: dial tcp 127.0.0.1:5432: i/o timeout")
	if err := ErrOrCancel(ctx, dial); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ErrOrCancel = %v; want DeadlineExceeded: the deadline had passed", err)
	}
}

// TestFindReplayKeylessTables_ProbeFailureWithoutCancelStaysAFailure is the
// other direction: the same driver errors on a LIVE context are genuine probe
// failures and must not be laundered into a cancellation (a clean exit).
func TestFindReplayKeylessTables_ProbeFailureWithoutCancelStaysAFailure(t *testing.T) {
	for _, shape := range []error{errPGXWriteTimeout, driver.ErrBadConn} {
		p := &cancellingProber{stop: func() {}, err: shape}
		_, err := FindReplayKeylessTables(context.Background(), p, []*ir.Table{replayTable("su", true, nil)},
			ReplayJudgeOptions{ProbeTarget: true})
		if !errors.Is(err, shape) {
			t.Fatalf("probe failure on a live context = %v; want it to carry %v", err, shape)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("probe failure on a live context reads as a cancellation: %v", err)
		}
	}
}
