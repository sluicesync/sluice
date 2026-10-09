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
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// cancelMidPositionWrite stops the run while the position write is in flight
// and fails with the error the driver produces in that window.
type cancelMidPositionWrite struct {
	replayApplier
	stop func()
	err  error
}

func (a *cancelMidPositionWrite) WritePosition(context.Context, string, ir.Position) error {
	a.stop()
	return a.err
}

// TestSyncFromBackup_CancelMidPositionWriteIsTheCancel pins the v0.157.0 tag
// CI failure (run 37981028723, TestBroker_CancelStorm_Postgres): "broker:
// record at-chain-id position: broker: write position: postgres: write
// position: driver: bad connection". A cancel that lands while the broker's
// --at-chain-id position write is in flight must reach the caller as the
// cancellation — the clean stop — whatever shape the driver gave it. This is
// a statement the per-site fix did not reach; the rule now lives on Run's
// single return path, so the pin grades Run, not the site.
func TestSyncFromBackup_CancelMidPositionWriteIsTheCancel(t *testing.T) {
	shapes := map[string]error{
		"database/sql bad connection": fmt.Errorf("postgres: write position: %w", driver.ErrBadConn),
		"pgx write-path i/o timeout": fmt.Errorf("postgres: write position: write failed: %w",
			&net.OpError{Op: "write", Net: "tcp", Err: os.ErrDeadlineExceeded}),
		"runtime-class phase hint": sluicecode.Wrap(sluicecode.CodeConnectRefused, "check the host",
			errors.New("postgres: write position: dial tcp: connection refused")),
	}
	for name, shape := range shapes {
		t.Run(name, func(t *testing.T) {
			store, fullID, _ := brokerReplayFixture(t, true)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			runCtx, runCancel := context.WithCancel(ctx)
			defer runCancel()
			app := &cancelMidPositionWrite{stop: runCancel, err: shape}
			b := newReplayBroker(store, &app.replayApplier, true)
			b.Target = replayTargetEngine{applier: app, rw: replayKeyWriter{keyed: true}}
			b.AtChainID = fullID
			err := b.Run(runCtx)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v; want the cancellation that interrupted the position write", err)
			}
			if !strings.Contains(err.Error(), "write position") {
				t.Errorf("the interrupted statement's own failure is missing from the message: %v", err)
			}
		})
	}
}

// TestSyncFromBackup_CodedRefusalUnderCancelStaysCoded is the other
// direction: a refusal-class coded error that lands beside a cancel is a
// verdict, and the exit attribution must not mask it as a clean stop.
func TestSyncFromBackup_CodedRefusalUnderCancelStaysCoded(t *testing.T) {
	store, fullID, _ := brokerReplayFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	refusal := sluicecode.Wrap(sluicecode.CodeBrokerKeylessTable, "give the table a key", errors.New("refused"))
	app := &cancelMidPositionWrite{stop: runCancel, err: refusal}
	b := newReplayBroker(store, &app.replayApplier, true)
	b.Target = replayTargetEngine{applier: app, rw: replayKeyWriter{keyed: true}}
	b.AtChainID = fullID
	err := b.Run(runCtx)
	if runCtx.Err() == nil {
		t.Fatal("the cancel never fired; the arm was not exercised")
	}
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBrokerKeylessTable {
		t.Fatalf("Run = %v; want the coded refusal %s to survive the concurrent cancel", err, sluicecode.CodeBrokerKeylessTable)
	}
	if errors.Is(err, context.Canceled) {
		t.Errorf("the refusal reads as a clean stop: %v", err)
	}
}

// TestSyncFromBackup_RunIsTheOnlyEntryPoint keeps the exit attribution a
// single choke point: Run is the one exported method that drives target IO,
// and it routes every failure through [migcore.ErrOrCancel]. A new exported
// entry point must route through it too, then join this list.
func TestSyncFromBackup_RunIsTheOnlyEntryPoint(t *testing.T) {
	typ := reflect.TypeOf(&SyncFromBackup{})
	var got []string
	for i := 0; i < typ.NumMethod(); i++ {
		got = append(got, typ.Method(i).Name)
	}
	if want := []string{"Run"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("*SyncFromBackup exports %v; want %v. Route a new entry point through migcore.ErrOrCancel "+
			"(a mid-call cancel arrives as a driver error, not a context error) and add it here", got, want)
	}
}

// TestErrOrCancel_Matrix grades the attribution rule itself, both
// directions: what it re-attributes under an ended context, and what it
// never touches.
func TestErrOrCancel_Matrix(t *testing.T) {
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	driverErr := fmt.Errorf("write: %w", driver.ErrBadConn)
	runtimeCoded := sluicecode.Wrap(sluicecode.CodeConnectRefused, "h", errors.New("connection refused"))
	refusal := sluicecode.Wrap(sluicecode.CodeBrokerKeylessTable, "h", errors.New("refused"))
	verdict := &brokerIncrementalPartialError{backupID: "b", resumeFrom: "a", cause: context.Canceled}
	for _, c := range []struct {
		name       string
		ctx        context.Context
		err        error
		wantCancel bool
	}{
		{"live ctx, driver error stays", context.Background(), driverErr, false},
		{"live ctx, refusal stays", context.Background(), refusal, false},
		{"ended ctx, driver error is the cancel", ended, driverErr, true},
		{"ended ctx, runtime-class code is the cancel", ended, runtimeCoded, true},
		{"ended ctx, refusal stays", ended, refusal, false},
		{"ended ctx, partial verdict stays", ended, verdict, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := migcore.ErrOrCancel(c.ctx, c.err)
			if errors.Is(got, context.Canceled) != c.wantCancel {
				t.Fatalf("ErrOrCancel(%v) = %v; want cancellation=%v", c.err, got, c.wantCancel)
			}
			if !c.wantCancel && !errors.Is(got, c.err) {
				t.Fatalf("ErrOrCancel changed an error it must pass through: %v", got)
			}
		})
	}
}
