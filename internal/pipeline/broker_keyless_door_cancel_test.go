// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// cancelMidKeyProbe stops the run while the key probe is in flight and fails
// the way pgx's write path does then: an i/o timeout that does not unwrap to
// the context error.
type cancelMidKeyProbe struct{ stop func() }

func (cancelMidKeyProbe) WriteRows(context.Context, *ir.Table, <-chan ir.Row) error { return nil }

func (w cancelMidKeyProbe) ProbeReplayKey(context.Context, *ir.Table) (exists, keyed bool, err error) {
	w.stop()
	return false, false, fmt.Errorf("existence: write failed: %w", &net.OpError{Op: "write", Net: "tcp", Err: os.ErrDeadlineExceeded})
}

// cancelMidCoverageProbe stops the run while the apply-mark coverage probe is
// in flight and fails as database/sql does on a connection pgx closed on an
// earlier cancel.
type cancelMidCoverageProbe struct {
	coveringApplier
	stop func()
}

func (a *cancelMidCoverageProbe) MarksCoverReason(context.Context, *ir.Table) (string, error) {
	a.stop()
	return "", driver.ErrBadConn
}

// cancelMidOpenRowWriter stops the run while the door opens its target row
// writer, which fails the way a dial bounded by the run's deadline does.
type cancelMidOpenRowWriter struct {
	replayTargetEngine
	stop func()
}

func (e cancelMidOpenRowWriter) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	e.stop()
	return nil, errors.New("failed to connect: dial error: timeout: dial tcp 127.0.0.1:5432: i/o timeout")
}

// TestBrokerKeylessDoor_CancelMidProbeIsTheCancel pins the v0.157.0 CI
// failure (TestBroker_CancelStorm_Postgres, "existence: write failed: ...
// i/o timeout"): a broker cancelled while its keyless door is probing the
// target reports the CANCEL, not the driver's network-shaped failure — so a
// stop between incrementals, where the door runs, reads as the stop it is.
// Every target probe the door makes: the key probe and the per-incremental
// coverage probe on a warm resume, and the --reset-target-data door's
// coverage probe before its drop.
func TestBrokerKeylessDoor_CancelMidProbeIsTheCancel(t *testing.T) {
	for _, probe := range []string{"open row writer", "key probe", "coverage probe", "reset coverage probe"} {
		t.Run(probe, func(t *testing.T) {
			store, fullID := doorFixture(t, doorRows(true, false, true, false), true)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			runCtx, runCancel := context.WithCancel(ctx)
			defer runCancel()
			app := &cancelMidCoverageProbe{stop: runCancel}
			var rw ir.RowWriter = replayKeyWriter{keyed: true}
			dropped := 0
			switch probe {
			case "open row writer":
				app.stop = func() {}
			case "key probe":
				app.stop = func() {}
				rw = cancelMidKeyProbe{stop: runCancel}
			case "reset coverage probe":
				rw = resetDoorWriter{replayKeyWriter: replayKeyWriter{keyed: true}, dropped: &dropped}
			}
			if probe != "reset coverage probe" {
				p := encodeBrokerPosition("test://fe1", fullID)
				app.resume = &p
			}
			b := newReplayBroker(store, &app.replayApplier, true)
			b.Target = replayTargetEngine{applier: app, rw: rw}
			b.ResetTargetData = probe == "reset coverage probe"
			if probe == "open row writer" {
				b.Target = cancelMidOpenRowWriter{replayTargetEngine: replayTargetEngine{applier: app, rw: rw}, stop: runCancel}
			}
			err := b.Run(runCtx)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v; want the cancellation the door's probe was interrupted by", err)
			}
			if ce, ok := sluicecode.FromError(err); ok {
				t.Errorf("a cancelled probe surfaced a coded refusal %s: %v", ce.Code, err)
			}
			if dropped != 0 {
				t.Errorf("the reset dropped %d table(s) after its door was interrupted", dropped)
			}
			if len(app.received) != 0 || len(app.written) != 0 {
				t.Errorf("the interrupted door let the incremental reach the target: %d changes, %d position writes", len(app.received), len(app.written))
			}
		})
	}
}
