//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// TestStreamer_AddTableDrainedAgainstIdleWarmResume_Binlog: a stream stopped
// with `sync stop --wait` and started again warm, on a source that writes
// NOTHING, must record that it runs, so a drained add refuses it. An idle
// stream makes no position write, so the attempt-start stamp is the only
// thing that can replace the recorded stop — and on a warm resume that stamp
// is deferred until the source's position check passes (the fourth
// fix-aimed review of the drained-add-table heartbeat, G1). A restart that
// skipped both deferred stamps would look stopped for as long as the source
// stayed quiet.
//
// Source and target are separate servers, so the only writes the source sees
// are the test's own: on a shared server sluice's control-row writes are
// source binlog events, which the running stream would pick up and stamp
// from, masking a missing attempt stamp.
func TestStreamer_AddTableDrainedAgainstIdleWarmResume_Binlog(t *testing.T) {
	srcDSN, _, srcCleanup := startMySQLBinlog(t)
	defer srcCleanup()
	_, tgtDSN, tgtCleanup := startMySQLBinlog(t)
	defer tgtCleanup()
	applyDDLMySQL(t, srcDSN, `
		CREATE TABLE users (id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
		INSERT INTO users VALUES (1, 'u1');
		CREATE TABLE orders (id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
		INSERT INTO orders VALUES (1, 'v1'), (2, 'v1'), (3, 'v1');`)
	eng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	filter, err := migcore.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	const streamID = "idle-warm-resume"
	mk := func() *Streamer {
		return &Streamer{Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, Filter: filter}
	}
	stream := startTWFBRun(mk())
	(twfbCell{tgt: twfbDB{engine: "mysql", dsn: tgtDSN}, streamID: streamID}).waitStreaming(t, stream)
	syncStopWait(t, eng, tgtDSN, streamID, stream)
	if got := drainHBControl(t, tgtDSN, streamID, "stream_alive_ms"); got != "0" {
		t.Fatalf("RIG: after `sync stop --wait` the record is %q; want the stopped marker", got)
	}
	positionAtStop := drainHBControl(t, tgtDSN, streamID, "source_position")
	waitBinlogDumpThreads(t, srcDSN, 0)

	// Warm restart, and the source writes nothing. The independent signal
	// that it is streaming is the source's own: its binlog dump thread.
	stream = startTWFBRun(mk())
	defer func() { _ = stream.stop(t) }()
	waitBinlogDumpThreads(t, srcDSN, 1)
	if stream.exited() {
		t.Fatalf("the warm restart exited: %v", stream.err)
	}
	if got := drainHBControl(t, tgtDSN, streamID, "source_position"); got != positionAtStop {
		t.Fatalf("RIG: the idle restart moved the position (%q -> %q); a position write stamps the record, so "+
			"this cell would not measure the attempt-start stamp", positionAtStop, got)
	}

	err = (&AddTable{Source: eng, Target: eng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID, TableName: "orders"}).
		Run(context.Background())
	if !errors.Is(err, errStreamNotStopped) || !strings.Contains(err.Error(), "has not recorded that it stopped") {
		t.Fatalf("a drained add against an idle warm-resumed stream: %v; want the %s refusal at its first check — "+
			"the restart never replaced the recorded stop", err, DrainedAddTableStreamNotStoppedMarker)
	}
	if rows := drainHBRows(t, tgtDSN, "orders"); rows != nil {
		t.Errorf("the refused add left orders on the target (%d rows)", len(rows))
	}
}

// waitBinlogDumpThreads waits until the source server runs exactly want
// binlog dump threads (one per streaming binlog reader).
func waitBinlogDumpThreads(t *testing.T, dsn string, want int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	deadline := time.Now().Add(90 * time.Second)
	var got int
	for time.Now().Before(deadline) {
		if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'").Scan(&got); err != nil {
			t.Fatalf("read the source's processlist: %v", err)
		}
		if got == want {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("RIG: the source runs %d binlog dump threads; want %d", got, want)
}
