//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"testing"
)

// TestStreamer_SchemaAlreadyAppliedStillStampsLiveness_Binlog runs the
// --schema-already-applied start path (which skips EnsureControlTable) end
// to end: stopped with `sync stop --wait` (the record holds the stopped
// marker), the stream is started again with SchemaAlreadyApplied and applies
// a change, and the record must say running, so a drained add refuses it.
//
// WHAT IT DOES NOT PIN, stated so it is not read as broader: the review's
// MEDIUM (c421369a) was a stamp that FAILS under this flag. With a working
// stamp the applier learns the column from the stamp itself, so this test
// passes with or without StampStreamAlive's detect-first change. The
// failing-stamp half is pinned by TestStampAttemptStartFailsClosedOverAStop
// (fail-closed over a stop); the detect-first call itself has no
// discriminating pin — reproducing a stamp that fails after the detect needs
// a fault seam in the engine, which this change does not add.
func TestStreamer_SchemaAlreadyAppliedStillStampsLiveness_Binlog(t *testing.T) {
	srcDSN, tgtDSN, eng, filter, cleanup := drainHBRig(t)
	defer cleanup()
	const streamID = "saa-stamps"
	mk := func(saa bool) *Streamer {
		return &Streamer{
			Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, Filter: filter,
			SchemaAlreadyApplied: saa,
		}
	}
	stream := startTWFBRun(mk(false))
	(twfbCell{tgt: twfbDB{engine: "mysql", dsn: tgtDSN}, streamID: streamID}).waitStreaming(t, stream)
	syncStopWait(t, eng, tgtDSN, streamID, stream)
	if got := drainHBControl(t, tgtDSN, streamID, "stream_alive_ms"); got != "0" {
		t.Fatalf("RIG: after `sync stop --wait` the record is %q; want the stopped marker", got)
	}

	stream = startTWFBRun(mk(true))
	defer func() { _ = stream.stop(t) }()
	applyDDLMySQL(t, srcDSN, "INSERT INTO users VALUES (2, 'u2')")
	drainHBWaitUser(t, tgtDSN, 2, stream)
	if got := drainHBControl(t, tgtDSN, streamID, "stream_alive_ms"); got == "0" || got == "" {
		t.Fatalf("a --schema-already-applied stream that applied a change left the record %q; want a running stamp", got)
	}
	err := (&AddTable{Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, TableName: "orders"}).
		Run(context.Background())
	if !errors.Is(err, errStreamNotStopped) {
		t.Fatalf("a drained add against the running --schema-already-applied stream: %v; want the %s refusal",
			err, DrainedAddTableStreamNotStoppedMarker)
	}
}
