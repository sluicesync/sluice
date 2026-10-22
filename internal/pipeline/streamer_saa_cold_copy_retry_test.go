// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// readFailApplier is a side-channel position reader whose every read fails,
// as when the target (which is also the position store) is down during the
// very outage that faulted the copy.
type readFailApplier struct{ posControlApplier }

func (readFailApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{}, false, errors.New("dial tcp: connection refused")
}

type readFailTargetEngine struct{ posControlTargetEngine }

func (readFailTargetEngine) OpenChangeApplier(context.Context, string) (ir.ChangeApplier, error) {
	return readFailApplier{}, nil
}

// probeFailTarget is a target whose emptiness probe fails.
type probeFailTarget struct{ emptyCheckingDropper }

func (probeFailTarget) IsTableEmpty(context.Context, *ir.Table) (bool, error) {
	return false, errors.New("probe: connection reset")
}

// saaRetryRun drives runWithRetry through a mid-copy fault and, on the
// retry, through the real cold-start gate as the dispatch in
// phaseOpenChangeStream would call it: freshCopyOperatorRestart when the
// retry latched RestartFromScratch, freshCopyNone when it did not. It
// returns what the retry attempt saw and Run's error.
func saaRetryRun(t *testing.T, target ir.Engine, rw ir.RowWriter) (latched bool, err error) {
	t.Helper()
	captureSlog(t)
	s := &Streamer{
		StreamID:              "test-stream",
		Source:                &copyResumeEngine{name: "planetscale"},
		Target:                target,
		TargetDSN:             "tgt",
		ApplyRetryAttempts:    8,
		ApplyRetryBackoffBase: 1,
		ApplyRetryBackoffCap:  1,
		SchemaAlreadyApplied:  true,
	}
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	calls := 0
	s.runOnceFn = func(ctx context.Context) error {
		calls++
		if calls == 1 {
			return coldCopyDropErr()
		}
		latched = s.RestartFromScratch
		fresh := freshCopyNone
		if s.RestartFromScratch {
			fresh = freshCopyOperatorRestart
		}
		_, gerr := s.coldStartGatePreflight(ctx, schema, nil, rw, gateStream(idempotentReader{}),
			&stubChangeApplier{}, "test-stream", false, fresh)
		return gerr
	}
	err = s.Run(context.Background())
	if calls != 2 {
		t.Fatalf("runOnce ran %d times; want the faulted attempt and one retry", calls)
	}
	return latched, err
}

// TestSAAColdCopyRetry_RefusesAPartialCopy pins SAA-COLD-COPY-RETRY-UNCLEARED.
// A first cold start under --schema-already-applied on an upsert reader
// (VStream) faults mid-copy. Since VSTREAM-COPY-RECONNECT-STALE-ROWS every
// such fault reaches runWithRetry. Whether or not the retry could read the
// target's position (D0-4: a failed read leaves RestartFromScratch off, so
// the retry arrives as a plain cold start), the re-copy cannot clear the
// tables, and an upsert over the interrupted copy keeps every row the source
// deleted meanwhile. It must refuse while a target table holds rows, refuse
// when the probe fails, and name the fault, not a flag the operator never
// passed.
func TestSAAColdCopyRetry_RefusesAPartialCopy(t *testing.T) {
	cases := []struct {
		name       string
		target     ir.Engine
		wantLatch  bool
		rw         ir.RowWriter
		wantRefuse bool
	}{
		{"position read failed (D0-4, latch off), populated", readFailTargetEngine{}, false, &populatedTarget{}, true},
		{"position read failed (D0-4, latch off), probe fails", readFailTargetEngine{}, false, &probeFailTarget{}, true},
		{"position read failed (D0-4, latch off), emptied", readFailTargetEngine{}, false, &emptyCheckingDropper{}, false},
		{"no cdc-state row (latch on), populated", posControlTargetEngine{foundPos: false}, true, &populatedTarget{}, true},
		{"no cdc-state row (latch on), emptied", posControlTargetEngine{foundPos: false}, true, &emptyCheckingDropper{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			latched, err := saaRetryRun(t, c.target, c.rw)
			if latched != c.wantLatch {
				t.Fatalf("RIG: the retry saw RestartFromScratch=%v, want %v; this case does not exercise its path", latched, c.wantLatch)
			}
			if !c.wantRefuse {
				if err != nil {
					t.Fatalf("an emptied target was refused: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), restartUnclearedMarker) {
				t.Fatalf("the retried copy was let through onto a target it cannot clear: err = %v; want the %s refusal", err, restartUnclearedMarker)
			}
			if !strings.Contains(err.Error(), "a fault during the copy made this run restart the copy") {
				t.Errorf("the refusal does not name the fault that restarted the copy: %v", err)
			}
			if strings.Contains(err.Error(), "--restart-from-scratch with --schema-already-applied") {
				t.Errorf("the refusal blames --restart-from-scratch, which the operator never passed: %v", err)
			}
			if p, ok := c.rw.(*populatedTarget); ok && len(p.dropped) != 0 {
				t.Errorf("the refusal dropped %v; --schema-already-applied tables must never be dropped", p.dropped)
			}
		})
	}
}

// TestSAAColdStart_NoRetryIsUnchanged pins the other side: a first cold
// start under --schema-already-applied that never faulted keeps the
// operator's promise of empty tables and is not probed.
func TestSAAColdStart_NoRetryIsUnchanged(t *testing.T) {
	captureSlog(t)
	s := &Streamer{Source: &copyResumeEngine{name: "planetscale"}, Target: newRecordingEngine("planetscale"), SchemaAlreadyApplied: true}
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
	if _, err := s.coldStartGatePreflight(context.Background(), schema, nil, &populatedTarget{}, gateStream(idempotentReader{}),
		&stubChangeApplier{}, "stream-1", false, freshCopyNone); err != nil && strings.Contains(err.Error(), restartUnclearedMarker) {
		t.Fatalf("a cold start with no fault was probed and refused: %v", err)
	}
}
