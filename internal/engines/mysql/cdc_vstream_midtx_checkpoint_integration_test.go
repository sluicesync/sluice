//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVStream_MidTxCheckpointReplaysNotSkips binds the load-bearing wire-
// ordering PREMISE that laneapply.flushPendingBoundary's mid-run-checkpoint
// safety rests on (audit backlog C-1, the premise-naming rule). The premise,
// as stated in that function's doc: a VStream ROW event carries the VGTID that
// EXCLUDES its own transaction, because the reader advances r.currentVgtid to a
// transaction's VGTID only on the TRAILING VGTID event (cdc_vstream.go's
// dispatch VGTID arm), so every row is stamped — via dispatchRow's
// r.positionFor() — with the position established BEFORE its transaction. The
// consequence the concurrent applier depends on: a resume from a mid-transaction
// checkpoint (a row's position) REPLAYS the partially-applied transaction
// idempotently rather than skipping its tail.
//
// Until this test there was no pin. flushPendingBoundary's doc named the premise
// UNVERIFIED in those words; the concurrent laneapply path engages by default on
// PlanetScale (ADR-0106), and if a reader refactor ever stamped rows from the
// TRAILING VGTID, a crash after an idle-tick checkpoint would resume PAST a
// partly-applied transaction and silently drop its tail — silent loss at exit 0,
// the worst class. This test fails the moment that regresses.
//
// It binds the premise two ways, and the two are mutually reinforcing (so the
// pin cannot pass on empty/degenerate tokens):
//
//	(1) DIRECTLY: every row of one multi-row transaction carries the SAME
//	    token (the VGTID is stable within a source tx), and that token DIFFERS
//	    from a LATER single-row transaction's token — which is only true if a
//	    row's position excludes its own transaction. If rows were stamped with
//	    the trailing VGTID, the multi-row tx's rows would carry the same token
//	    the later tx sees as its start, and (1) would fail.
//	(2) OPERATIONALLY: reopening the stream FROM a mid-transaction row's
//	    position re-delivers the WHOLE transaction (replay), not nothing (skip).
//	    If rows carried the trailing VGTID, the resume would start past the tx
//	    and drop all of it — exactly the silent tail-drop (2) exists to catch.
func TestVStream_MidTxCheckpointReplaysNotSkips(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	applyVTTestSQL(t, mysqlDSN, `
		CREATE TABLE t (
			id BIGINT       NOT NULL AUTO_INCREMENT,
			v  VARCHAR(64)  NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	// --- Reader 1: observe a 3-row transaction followed by a 1-row sentinel tx.
	rdr1, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader r1: %v", err)
	}
	changes1, err := rdr1.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges r1: %v", err)
	}
	// Settle window: vtgate's "current" stream takes a moment to register;
	// events generated too quickly can land just before the boundary and drop.
	time.Sleep(2 * time.Second)

	// One explicit multi-row transaction: its three rows commit atomically as a
	// single source binlog transaction, so they share one trailing VGTID.
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", `
		START TRANSACTION;
		INSERT INTO t (v) VALUES ('r1');
		INSERT INTO t (v) VALUES ('r2');
		INSERT INTO t (v) VALUES ('r3');
		COMMIT;
	`)
	// A later, separate transaction. Its own start position is AFTER the 3-row
	// tx committed, so its row's token is the 3-row tx's POST-commit position.
	applyVTTestSQL(t, mysqlDSN, `INSERT INTO t (v) VALUES ('sentinel')`)

	got := drainVTTestChanges(t, ctx, changes1, 4, 90*time.Second)
	if len(got) != 4 {
		if streamErr := rdr1.(*vstreamCDCReader).Err(); streamErr != nil {
			t.Fatalf("r1: got %d changes; want 4 (stream error: %v)", len(got), streamErr)
		}
		t.Fatalf("r1: got %d changes; want 4 (tx r1/r2/r3 + sentinel)", len(got))
	}
	for i, want := range []string{"r1", "r2", "r3", "sentinel"} {
		ins, ok := got[i].(ir.Insert)
		if !ok {
			t.Fatalf("r1 got[%d] = %T; want ir.Insert", i, got[i])
		}
		if v, _ := ins.Row["v"].(string); v != want {
			t.Fatalf("r1 got[%d].v = %q; want %q", i, v, want)
		}
	}

	txToken := got[0].Pos().Token
	if txToken == "" {
		t.Fatalf("r1: the 3-row tx carries an empty position token")
	}
	// (1a) The VGTID is STABLE within the transaction: all three rows share it.
	for i := 1; i < 3; i++ {
		if got[i].Pos().Token != txToken {
			t.Fatalf("r1 row %d token %q != row0 token %q — the VGTID is not stable within the transaction, "+
				"which the premise's mid-tx-checkpoint safety depends on", i, got[i].Pos().Token, txToken)
		}
	}
	// (1b) The row's position EXCLUDES its own transaction: the later sentinel
	// transaction — which starts after the 3-row tx committed — carries a
	// DIFFERENT token. Equal tokens here would mean the rows were stamped with
	// the trailing (post-commit) VGTID, the exact regression this pins against.
	sentinelToken := got[3].Pos().Token
	if sentinelToken == txToken {
		t.Fatalf("r1: the 3-row tx's rows carry the same token %q as the LATER sentinel tx — a row's position "+
			"is NOT excluding its own transaction (premise C-1 violated); a mid-tx checkpoint resume would skip "+
			"the tx tail and silently drop rows", txToken)
	}
	if c, ok := rdr1.(interface{ Close() error }); ok {
		_ = c.Close()
	}

	// --- Reader 2: resume FROM a mid-transaction row's position (the checkpoint
	// case). The premise guarantees this replays the WHOLE 3-row tx.
	rdr2, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader r2: %v", err)
	}
	defer func() {
		if c, ok := rdr2.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	changes2, err := rdr2.StreamChanges(ctx, got[0].Pos())
	if err != nil {
		t.Fatalf("StreamChanges r2 (resume from mid-tx row position): %v", err)
	}

	replayed := drainVTTestChanges(t, ctx, changes2, 4, 90*time.Second)
	if len(replayed) != 4 {
		if streamErr := rdr2.(*vstreamCDCReader).Err(); streamErr != nil {
			t.Fatalf("r2 resume: got %d replayed changes; want 4 (stream error: %v)", len(replayed), streamErr)
		}
		t.Fatalf("r2 resume from a mid-tx row position replayed %d rows; want 4 (r1/r2/r3 + sentinel). "+
			"Fewer means the resume SKIPPED the partially-applied transaction's tail — silent loss, premise C-1 broken",
			len(replayed))
	}
	for i, want := range []string{"r1", "r2", "r3", "sentinel"} {
		ins, ok := replayed[i].(ir.Insert)
		if !ok {
			t.Fatalf("r2 replayed[%d] = %T; want ir.Insert", i, replayed[i])
		}
		if v, _ := ins.Row["v"].(string); v != want {
			t.Fatalf("r2 replayed[%d].v = %q; want %q — the resumed stream did not replay the transaction faithfully", i, v, want)
		}
	}
}
