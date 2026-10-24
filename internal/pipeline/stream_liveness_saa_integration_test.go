//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"testing"
)

// TestStreamer_SchemaAlreadyAppliedStillStampsLiveness_Binlog is the
// fix-aimed review of c421369a, MEDIUM: under --schema-already-applied a
// start skips EnsureControlTable, whose probe was the only other place an
// applier learned the liveness column exists. Stopped with `sync stop --wait`
// (the record holds the stopped marker), the stream is started again with
// SchemaAlreadyApplied and applies a change: the record must say running, so
// a drained add refuses it, exactly as without the flag (which the other
// tests in this package run).
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
