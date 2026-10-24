// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
)

// livenessFakeApplier adds the liveness record to fakeApplier.
type livenessFakeApplier struct {
	*fakeApplier
	liveness ir.StreamLiveness
	err      error
}

var _ ir.StreamLivenessStore = livenessFakeApplier{}

func (a livenessFakeApplier) ReadStreamLiveness(context.Context, string) (ir.StreamLiveness, error) {
	return a.liveness, a.err
}

func (livenessFakeApplier) StampStreamAlive(context.Context, string) (ir.StreamStamp, error) {
	panic("sync stop --wait must not stamp")
}

func (livenessFakeApplier) RecordStreamStopped(context.Context, string) error {
	panic("sync stop --wait must not record a stop")
}

func (livenessFakeApplier) RestoreStreamStopped(context.Context, string, ir.StreamStamp) (bool, error) {
	panic("sync stop --wait must not restore a stop")
}

// TestWaitForStopComplete_RequiresTheRecordedStop is the fix-aimed review of
// c421369a (MEDIUM, loud): a cleared stop request is not proof the stream
// stopped, because every start clears it. The case that motivated it: a
// stop's drain is hard-cancelled (exit 4), systemd Restart=on-failure starts
// the process again, the restart clears the flag, and `sync stop --wait`
// used to report success while the stream ran. Success now needs the
// stopped marker a graceful drain writes before it clears the flag.
func TestWaitForStopComplete_RequiresTheRecordedStop(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name     string
		liveness ir.StreamLiveness
		readErr  error
		wantErr  []string // nil = success
	}{
		{name: "graceful drain: the stop is recorded", liveness: ir.StreamLiveness{Found: true, Recorded: true, Stopped: true, Now: now}},
		{
			name:     "hard-cancelled, then restarted: the flag cleared and the stream runs",
			liveness: ir.StreamLiveness{Found: true, Recorded: true, AliveAt: now, Now: now},
			wantErr:  []string{"did not record a stop", pipeline.StopDrainHardCancelledMarker, "RestartPreventExitStatus=4"},
		},
		{name: "a target without the liveness column cannot prove it either way", liveness: ir.StreamLiveness{Found: true, Now: now}},
		{name: "the record cannot be read", readErr: errors.New("connection reset"), wantErr: []string{"could not be read"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := &fakeApplier{}
			base.stopRequested.Store(true)
			go func() {
				time.Sleep(30 * time.Millisecond)
				base.stopRequested.Store(false) // a graceful drain, or a restart's start
			}()
			err := waitForStopComplete(context.Background(), livenessFakeApplier{base, tc.liveness, tc.readErr}, "s1", 5*time.Second)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("want success, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("want a failure, got success")
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error lacks %q: %v", want, err)
				}
			}
		})
	}
}
