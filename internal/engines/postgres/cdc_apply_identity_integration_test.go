//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0190 phase 1 pin against a real server: the pgoutput reader stamps
// every row change with an identity that a RE-DELIVERY of the same
// transaction reproduces exactly — the property the apply-mark skip rule
// stands on.
//
// The transaction is read once, a fresh reader resumes from the commit
// position of the transaction BEFORE it (what an applier persists), reads it
// again, and the two identity sequences must be identical. The expected
// per-table ordinals come from the test's own statement list; the identity's
// system id from the server (pg_control_system()), and its commit LSN must be
// the one every row of the transaction carries as its Position.

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

type pgIdentityStep struct {
	table string
	kind  string
	id    ir.ApplyID
}

func pgIdentitySteps(got []ir.Change) []pgIdentityStep {
	var out []pgIdentityStep
	for _, c := range got {
		switch v := c.(type) {
		case ir.Insert:
			out = append(out, pgIdentityStep{v.Table, "I", v.ApplyID})
		case ir.Update:
			out = append(out, pgIdentityStep{v.Table, "U", v.ApplyID})
		case ir.Delete:
			out = append(out, pgIdentityStep{v.Table, "D", v.ApplyID})
		}
	}
	return out
}

// untilRowsThenCommit stops a collection at the first TxCommit that follows
// at least rows row changes.
func untilRowsThenCommit(rows int) func(got []ir.Change) bool {
	return func(got []ir.Change) bool {
		_, isCommit := lastChange(got).(ir.TxCommit)
		return isCommit && len(pgIdentitySteps(got)) >= rows
	}
}

func TestPGCDC_ApplyIdentity_StableAcrossRedelivery(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()
	applyPGSQL(t, dsn, `
		CREATE TABLE ta (id INT PRIMARY KEY, v TEXT NOT NULL);
		CREATE TABLE tb (id INT PRIMARY KEY, v TEXT NOT NULL);`)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// ---- Delivery 1: a warm-up transaction W, then the transaction T. ----
	rdr1 := openCDC(t, ctx, dsn, true)
	// Keep the slot's confirmed_flush where delivery 2 must resume from.
	rdr1.HoldSlotAckAtCommitted()
	ch1, err := rdr1.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("delivery 1 StreamChanges: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	applyPGSQL(t, dsn, `INSERT INTO ta (id, v) VALUES (100, 'w');`)
	warm, _ := collectChanges(t, ctx, ch1, 30*time.Second, untilRowsThenCommit(1))
	commitW := lastTxCommit(t, warm)
	warmSteps := pgIdentitySteps(warm)

	applyPGSQL(t, dsn, `
		BEGIN;
		INSERT INTO ta (id, v) VALUES (1, 'a'), (2, 'b');
		INSERT INTO tb (id, v) VALUES (10, 'x');
		UPDATE ta SET v = 'a2' WHERE id = 1;
		DELETE FROM tb WHERE id = 10;
		INSERT INTO ta (id, v) VALUES (3, 'c');
		COMMIT;`)
	gotT, _ := collectChanges(t, ctx, ch1, 30*time.Second, untilRowsThenCommit(6))
	if err := rdr1.Close(); err != nil {
		t.Fatalf("delivery 1 Close: %v", err)
	}
	first := pgIdentitySteps(gotT)
	var firstRow ir.Change
	for _, c := range gotT {
		if ir.IsRowDMLChange(c) {
			firstRow = c
			break
		}
	}

	// ---- Delivery 2: resume from W's commit; T is re-delivered. ----
	rdr2 := openCDC(t, ctx, dsn, true)
	ch2, err := rdr2.StreamChanges(ctx, commitW)
	if err != nil {
		t.Fatalf("delivery 2 StreamChanges: %v", err)
	}
	gotT2, _ := collectChanges(t, ctx, ch2, 30*time.Second, untilRowsThenCommit(6))
	if err := rdr2.Close(); err != nil {
		t.Fatalf("delivery 2 Close: %v", err)
	}
	second := pgIdentitySteps(gotT2)

	want := []struct {
		table, kind string
		seq         uint64
	}{{"ta", "I", 1}, {"ta", "I", 2}, {"tb", "I", 1}, {"ta", "U", 3}, {"tb", "D", 2}, {"ta", "I", 4}}
	if len(first) != len(want) {
		t.Fatalf("delivery 1 produced %d row changes for T; want %d: %+v", len(first), len(want), first)
	}
	for i, w := range want {
		got := first[i]
		if got.table != w.table || got.kind != w.kind || got.id.Seq != w.seq {
			t.Errorf("change %d = %s %s #%d; want %s %s #%d (the per-table ordinal)", i, got.table, got.kind, got.id.Seq, w.table, w.kind, w.seq)
		}
		if got.id.TxID == "" || got.id.TxID != first[0].id.TxID {
			t.Errorf("change %d carries transaction %q; every change of T must carry %q", i, got.id.TxID, first[0].id.TxID)
		}
	}
	if len(warmSteps) != 1 || warmSteps[0].id.IsZero() || warmSteps[0].id.TxID == first[0].id.TxID {
		t.Errorf("the warm-up transaction's identity %+v must be present and differ from T's %q", warmSteps, first[0].id.TxID)
	}

	// The independent value: the server's system identifier, and the commit
	// LSN every row of T carries as its position.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var sysid string
	if err := db.QueryRow(`SELECT system_identifier::text FROM pg_control_system()`).Scan(&sysid); err != nil {
		t.Fatalf("system identifier: %v", err)
	}
	wantTx := fmt.Sprintf("pg:%s:", sysid)
	if len(first[0].id.TxID) <= len(wantTx) || first[0].id.TxID[:len(wantTx)] != wantTx {
		t.Errorf("transaction identity %q does not name the server's system id %s", first[0].id.TxID, sysid)
	}
	if lsn := positionLSN(t, firstRow.Pos()).String(); first[0].id.TxID[len(first[0].id.TxID)-len(lsn):] != lsn {
		t.Errorf("transaction identity %q does not end in the transaction's commit LSN %s", first[0].id.TxID, lsn)
	}

	if fmt.Sprint(second) != fmt.Sprint(first) {
		t.Errorf("a re-delivery of the same transaction numbered it differently — an apply mark would skip the wrong change:\n  first  %+v\n  second %+v", first, second)
	}
}
