// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestOrchestrator_LowVolumeMarkerlessStream_IdleCheckpointAdvances is the
// Bug-159 unit pin. A MARKER-LESS stream (no TxBegin/TxCommit — e.g. a
// postgres-trigger source, whose every change carries a distinct, monotone
// `{"last_id":N}` token) applying FAR fewer than checkpointEveryChanges (2000)
// changes, with the channel STILL OPEN (a live, quiet sync), must persist its
// resume position via the time-based idle checkpoint — not stay frozen at the
// cold-start anchor until 2000 changes accrue.
//
// Pre-fix the orchestrator only checkpointed at the 2000-change count cadence,
// at a barrier, or at end-of-stream (channel close). A low-volume trigger sync
// therefore applied every change correctly yet left source_position frozen at
// {"last_id":0} indefinitely (Bug 159) — the capture log was never reclaimable
// and every warm-resume re-read the whole log from id 0. The idle-checkpoint
// ticker (checkpointIdlePeriod) fixes it, mirroring the serial path's item-18
// idle flush. This test asserts the position advances WHILE the channel is open
// (the count cadence is never reached and end-of-stream never fires).
func TestOrchestrator_LowVolumeMarkerlessStream_IdleCheckpointAdvances(t *testing.T) {
	seam := &recordingSeam{}
	orch := NewOrchestrator(Config{Lanes: 2, MaxBatchSize: 4}, seam)

	pos := func(tok string) ir.Position { return ir.Position{Engine: "postgres-trigger", Token: tok} }

	// midStream captures the seam's last checkpoint observed WHILE the channel
	// is still open (before close), so the assertion isolates the idle-tick
	// path: the end-of-stream checkpoint (which exists with or without the fix)
	// must NOT be what satisfies this test. Without the idle ticker, no
	// checkpoint is recorded until close, so midStream stays empty and the test
	// fails — the genuine behavioral regression catch.
	midStreamCh := make(chan string, 1)

	// A long-lived channel: feed a handful of marker-less row changes, each with
	// a DISTINCT token (like the pgtrigger change-log id), then KEEP THE CHANNEL
	// OPEN so neither the 2000-change count cadence nor end-of-stream fires —
	// only the idle-checkpoint ticker can advance the position.
	changes := make(chan ir.Change)
	go func() {
		for i := 1; i <= 3; i++ {
			changes <- ir.Insert{
				Position: pos(`{"last_id":` + itoa(i) + `}`),
				Schema:   "public", Table: "t",
				Row: ir.Row{"id": int64(i)},
			}
		}
		// Hold the channel open past a few idle-checkpoint ticks so the
		// time-based flush has a chance to persist, then SNAPSHOT the seam's
		// last checkpoint BEFORE closing. A fixed duration (independent of
		// checkpointIdlePeriod) keeps this a genuine behavioral pin.
		time.Sleep(3500 * time.Millisecond)
		tok, _ := seam.lastCheckpoint()
		midStreamCh <- tok
		close(changes)
	}()

	if err := orch.Run(context.Background(), changes); err != nil {
		t.Fatalf("Run: %v", err)
	}

	midStream := <-midStreamCh
	// The MID-STREAM checkpoint (recorded by the idle tick, before channel
	// close) must be the trailing change's token {"last_id":3}: the idle tick
	// recorded the trailing marker-less boundary and persisted the durable
	// frontier WITHOUT waiting for 2000 changes or end-of-stream. An empty
	// mid-stream checkpoint is the Bug-159 signature (position frozen until a
	// barrier / count cadence / channel close).
	want := `{"last_id":3}`
	if midStream == "" {
		t.Fatal("Bug 159: no checkpoint persisted MID-STREAM on a low-volume marker-less stream — " +
			"the resume position stays frozen at the cold-start anchor until 2000 changes accrue or " +
			"the stream ends (the idle-checkpoint tick is what must advance it)")
	}
	if midStream != want {
		t.Errorf("mid-stream persisted position = %q; want %q (the trailing applied change's watermark)", midStream, want)
	}
}

// itoa is a tiny strconv.Itoa shim kept local so the test file's imports stay
// minimal and the token strings read inline.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
