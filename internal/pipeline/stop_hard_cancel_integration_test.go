//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// TestStreamer_HardCancelledStopIsNotGraceful_Binlog drives STOP-DRAIN-HARD-
// CANCELLED end to end on a MySQL binlog source writing to MySQL: a target
// session holds a row lock the stream's next apply needs, so the apply wedges;
// `sync stop` is requested, the drain watchdog (shortened to 3s) cancels the
// apply, and the run must then fail with ErrStopDrainHardCancelled, leave
// stop_requested_at set and record no stop. A drained add-table refuses on
// the flag, naming the hard-cancel remedy; the remedy (start once, then `sync
// stop --wait`) is then driven and the add passes.
func TestStreamer_HardCancelledStopIsNotGraceful_Binlog(t *testing.T) {
	srcDSN, tgtDSN, eng, filter, cleanup := drainHBRig(t)
	defer cleanup()
	setDrainTimeoutForTest(t, 3*time.Second)
	setPollIntervalForTest(t, 250*time.Millisecond)
	const streamID = "hard-cancel"
	mk := func() *Streamer {
		return &Streamer{Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, Filter: filter}
	}
	stream := startTWFBRun(mk())
	defer func() { _ = stream.stop(t) }()
	(twfbCell{tgt: twfbDB{engine: "mysql", dsn: tgtDSN}, streamID: streamID}).waitStreaming(t, stream)
	drainHBWaitStamped(t, tgtDSN, streamID)

	// Wedge the next apply: hold users row 1 on the target.
	ctx := context.Background()
	db, err := sql.Open("mysql", tgtDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	lock, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err := lock.ExecContext(ctx, "SELECT id FROM users WHERE id = 1 FOR UPDATE"); err != nil {
		t.Fatalf("RIG: lock users row 1 on the target: %v", err)
	}
	applyDDLMySQL(t, srcDSN, "UPDATE users SET payload = 'wedged' WHERE id = 1")
	deadline := time.Now().Add(60 * time.Second)
	for {
		var waiting int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_state = 'LOCK WAIT'").Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if stream.exited() || time.Now().After(deadline) {
			t.Fatalf("RIG: the stream's apply never waited on the held lock (exited=%v err=%v)", stream.exited(), stream.err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// `sync stop`: the drain cannot finish, and the watchdog cancels it.
	applier, err := eng.OpenChangeApplier(ctx, tgtDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = applier.(io.Closer).Close() }()
	if err := applier.RequestStop(ctx, streamID); err != nil {
		t.Fatalf("RIG: request stop: %v", err)
	}
	deadline = time.Now().Add(60 * time.Second)
	for !stream.exited() {
		if time.Now().After(deadline) {
			t.Fatal("the wedged stream did not exit after its stop's drain timeout")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !errors.Is(stream.err, ErrStopDrainHardCancelled) {
		t.Fatalf("the hard-cancelled stop's Run = %v; want %s (not a graceful nil)", stream.err, StopDrainHardCancelledMarker)
	}
	if pending, err := applier.(stopFlagReader).ReadStopRequested(ctx, streamID); err != nil || !pending {
		t.Fatalf("stop_requested_at after a hard-cancelled drain: pending=%v err=%v; want it kept set", pending, err)
	}
	if got := drainHBControl(t, tgtDSN, streamID, "stream_alive_ms"); got == "0" || got == "" {
		t.Fatalf("stream_alive_ms after a hard-cancelled drain = %q; want the last running stamp, not a recorded stop", got)
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}

	add := func() error {
		return (&AddTable{Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, TableName: "orders"}).
			Run(ctx)
	}
	err = add()
	if err == nil || !strings.Contains(err.Error(), "in-flight stop request") || !strings.Contains(err.Error(), StopDrainHardCancelledMarker) {
		t.Fatalf("a drained add after a hard-cancelled stop: %v; want the in-flight-stop refusal naming %s", err, StopDrainHardCancelledMarker)
	}
	if rows := drainHBRows(t, tgtDSN, "orders"); rows != nil {
		t.Errorf("the refused add left orders on the target (%d rows)", len(rows))
	}

	// The remedy the refusal names: start once, then `sync stop --wait`.
	stream = startTWFBRun(mk())
	deadline = time.Now().Add(120 * time.Second)
	for drainHBRows(t, tgtDSN, "users")[1] != "wedged" {
		if stream.exited() || time.Now().After(deadline) {
			t.Fatalf("the restarted stream never applied the wedged update (exited=%v err=%v)", stream.exited(), stream.err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	syncStopWait(t, eng, tgtDSN, streamID, stream)
	if got := drainHBControl(t, tgtDSN, streamID, "stream_alive_ms"); got != "0" {
		t.Fatalf("after start then `sync stop --wait`, the record is %q; want the stopped marker", got)
	}
	if err := add(); err != nil {
		t.Fatalf("the drained add after the remedy: %v", err)
	}
}
