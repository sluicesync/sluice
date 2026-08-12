//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestCDCReader_XA is the CDCPOS-1 real-server pin (audit 2026-08-11), on a
// real gtid_mode=ON binlog. Two arms in stream order on one reader:
//
//   - OUT-OF-SCOPE: a whole XA lifecycle (START/END/PREPARE/COMMIT) on a
//     DIFFERENT database streams past without disturbing the reader, and an
//     ordinary in-scope transaction that follows still satisfies the
//     item-132 staging contract via the server's own GTID_SUBSET — proving
//     the XA body's GTID folded (late, never early) rather than being
//     dropped or double-counted.
//   - IN-SCOPE with XA ROLLBACK: the stream must refuse loudly with
//     SLUICE-E-CDC-XA-UNSUPPORTED at the body's first row — the rollback arm
//     is exactly the one where pre-fix behaviour FABRICATED rows on the
//     target (sluice applied at read; the source rolled back and never
//     showed them).
func TestCDCReader_XA(t *testing.T) {
	dsn, cleanup := startMySQLGTIDForCDC(t)
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE ledger (
			id   BIGINT       NOT NULL AUTO_INCREMENT,
			memo VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)
	applyMySQL(t, dsn, `CREATE DATABASE xa_other;`)
	applyMySQL(t, dsn, `CREATE TABLE xa_other.t (id BIGINT PRIMARY KEY, v VARCHAR(32));`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// --- Arm 1: out-of-scope XA streams past; the staging contract holds after.
	applyMySQL(t, dsn, `
		XA START 'oos1';
		INSERT INTO xa_other.t (id, v) VALUES (1, 'a');
		XA END 'oos1';
		XA PREPARE 'oos1';
		XA COMMIT 'oos1';
	`)
	before := gtidExecuted(t, dsn) // includes the out-of-scope XA groups
	applyMySQL(t, dsn, `
		START TRANSACTION;
		INSERT INTO ledger (memo) VALUES ('a');
		INSERT INTO ledger (memo) VALUES ('b');
		COMMIT;
	`)
	after := gtidExecuted(t, dsn)
	if before == after {
		t.Fatalf("test premise broken: gtid_executed unchanged across the in-scope transaction (%q)", before)
	}

	got := drainChangesWithBoundaries(t, ctx, changes, 4, 60*time.Second)
	if len(got) != 4 { // TxBegin, 2 Inserts, TxCommit — the XA arm must emit NOTHING
		t.Fatalf("drained %d changes, want 4 (TxBegin, 2 Inserts, TxCommit) — an out-of-scope XA body must "+
			"emit nothing and must not disturb the stream: %#v", len(got), got)
	}
	for i, c := range got[:3] {
		set := mustDecodeGTIDPos(t, c.Pos(), "post-XA change").GTIDSet
		if !serverGTIDSubset(t, dsn, set, before) {
			t.Errorf("change %d (%T) carries %q, not a subset of the pre-transaction executed set %q — either "+
				"the in-scope transaction leaked into its own rows (item 132) or the out-of-scope XA body "+
				"folded somewhere illegal (CDCPOS-1)", i, c, set, before)
		}
	}
	commitSet := mustDecodeGTIDPos(t, got[3].Pos(), "post-XA TxCommit").GTIDSet
	if !serverGTIDSubset(t, dsn, after, commitSet) {
		t.Errorf("TxCommit carries %q, which does not cover the post-transaction executed set %q — the "+
			"out-of-scope XA GTIDs or the transaction's own were dropped from the fold (loss on resume)", commitSet, after)
	}

	// --- Arm 2: in-scope XA refuses loudly at the first body row (rollback arm).
	applyMySQL(t, dsn, `
		XA START 'ins1';
		INSERT INTO ledger (memo) VALUES ('phantom');
		XA END 'ins1';
		XA PREPARE 'ins1';
		XA ROLLBACK 'ins1';
	`)

	// The pump refuses at the row and closes the channel; drain to close.
	deadline := time.After(60 * time.Second)
	for {
		select {
		case c, ok := <-changes:
			if !ok {
				goto closed
			}
			t.Fatalf("stream delivered %T after an in-scope XA body row; want the loud refusal and nothing "+
				"delivered — pre-fix this arm FABRICATED the rolled-back row on the target", c)
		case <-deadline:
			t.Fatal("stream did not halt within 60s of the in-scope XA body row; the refusal never fired")
		}
	}
closed:
	errer, ok := rdr.(interface{ Err() error })
	if !ok {
		t.Fatal("reader exposes no Err(); cannot grade the refusal")
	}
	streamErr := errer.Err()
	if streamErr == nil {
		t.Fatal("stream closed with nil Err after an in-scope XA body row; want SLUICE-E-CDC-XA-UNSUPPORTED")
	}
	ce, isCoded := sluicecode.FromError(streamErr)
	if !isCoded || ce.Code != sluicecode.CodeCDCXAUnsupported {
		t.Fatalf("stream error should carry %s structurally; got: %v", sluicecode.CodeCDCXAUnsupported, streamErr)
	}
}
