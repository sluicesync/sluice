//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0190 phase 1 pins against real servers: the binlog reader stamps every
// row change with an identity that a RE-DELIVERY of the same transaction
// reproduces exactly — the one property the apply-mark skip rule stands on.
//
// Each cell reads a transaction once, reopens a fresh reader from the commit
// position of the transaction BEFORE it (the resume point an applier
// persists), reads it again, and requires the two identity sequences to be
// identical. The expected values are independent of sluice: the per-table
// ordinals come from the test's own statement list, and the transaction
// identity from the SERVER (GTID mode: GTID_SUBTRACT of @@gtid_executed
// across the transaction; MariaDB: @@gtid_binlog_pos after it; file/pos: the
// server's own @@server_uuid and binlog file).
//
// The re-delivery from a REPLICA after a failover (row-image identity across
// servers, the ADR-0190 §1 premise) is its own cell:
// cdc_apply_identity_replica_integration_test.go.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// applyIdentityTxn is the transaction under test: two tables interleaved, so
// a per-TRANSACTION ordinal and a per-TABLE one disagree, and every DML verb.
const applyIdentityTxn = `
	START TRANSACTION;
	INSERT INTO ta (id, v) VALUES (1, 'a'), (2, 'b');
	INSERT INTO tb (id, v) VALUES (10, 'x');
	UPDATE ta SET v = 'a2' WHERE id = 1;
	DELETE FROM tb WHERE id = 10;
	INSERT INTO ta (id, v) VALUES (3, 'c');
	COMMIT;`

// identityStep is one delivered row change as the gate compares it.
type identityStep struct {
	table string
	kind  string
	id    ir.ApplyID
}

// applyIdentityWant is the per-table ordinal sequence applyIdentityTxn must
// produce — derived from the statement list, not from sluice.
var applyIdentityWant = []struct {
	table, kind string
	seq         uint64
}{
	{"ta", "I", 1}, {"ta", "I", 2}, {"tb", "I", 1}, {"ta", "U", 3}, {"tb", "D", 2}, {"ta", "I", 4},
}

func identitySteps(got []ir.Change) []identityStep {
	var out []identityStep
	for _, c := range got {
		var s identityStep
		switch v := c.(type) {
		case ir.Insert:
			s = identityStep{table: v.Table, kind: "I", id: v.ApplyID}
		case ir.Update:
			s = identityStep{table: v.Table, kind: "U", id: v.ApplyID}
		case ir.Delete:
			s = identityStep{table: v.Table, kind: "D", id: v.ApplyID}
		default:
			continue
		}
		out = append(out, s)
	}
	return out
}

// assertApplyIdentityStableAcrossRedelivery runs the cell. serverState reads
// the independent before/after server state; wantTxID derives the expected
// transaction identity from it (a prefix match when exact is false).
func assertApplyIdentityStableAcrossRedelivery(
	t *testing.T,
	eng Engine,
	dsn string,
	serverState func(t *testing.T) string,
	wantTxID func(t *testing.T, before, after string) (want string, exact bool),
) {
	t.Helper()
	applyMySQL(t, dsn, `
		CREATE TABLE ta (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) NOT NULL) ENGINE=InnoDB;
		CREATE TABLE tb (id INT NOT NULL PRIMARY KEY, v VARCHAR(16) NOT NULL) ENGINE=InnoDB;`)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	open := func(from ir.Position) (ir.CDCReader, <-chan ir.Change) {
		rdr, err := eng.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		ch, err := rdr.StreamChanges(ctx, from)
		if err != nil {
			t.Fatalf("StreamChanges(%v): %v", from, err)
		}
		return rdr, ch
	}
	closeReader := func(rdr ir.CDCReader) {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}

	// ---- Delivery 1: a warm-up transaction W, then the transaction T. ----
	rdr1, ch1 := open(ir.Position{})
	time.Sleep(500 * time.Millisecond)
	applyMySQL(t, dsn, `INSERT INTO ta (id, v) VALUES (100, 'w');`)
	warm := drainChangesWithBoundaries(t, ctx, ch1, 3, 60*time.Second) // TxBegin, Insert, TxCommit
	commitW, ok := warm[len(warm)-1].(ir.TxCommit)
	if len(warm) != 3 || !ok {
		t.Fatalf("warm-up transaction delivered %#v; want TxBegin, Insert, TxCommit", warm)
	}
	warmID := identitySteps(warm)[0].id

	before := serverState(t)
	applyMySQL(t, dsn, applyIdentityTxn)
	after := serverState(t)
	first := identitySteps(drainChangesWithBoundaries(t, ctx, ch1, 8, 60*time.Second))
	closeReader(rdr1)

	// ---- Delivery 2: a fresh reader resumed from W's commit re-reads T. ----
	rdr2, ch2 := open(commitW.Position)
	second := identitySteps(drainChangesWithBoundaries(t, ctx, ch2, 8, 60*time.Second))
	closeReader(rdr2)

	if len(first) != len(applyIdentityWant) {
		t.Fatalf("first delivery produced %d row changes; want %d: %+v", len(first), len(applyIdentityWant), first)
	}
	for i, w := range applyIdentityWant {
		got := first[i]
		if got.table != w.table || got.kind != w.kind || got.id.Seq != w.seq {
			t.Errorf("change %d = %s %s #%d; want %s %s #%d (the per-table ordinal)", i, got.table, got.kind, got.id.Seq, w.table, w.kind, w.seq)
		}
		if got.id.TxID == "" || got.id.TxID != first[0].id.TxID {
			t.Errorf("change %d carries transaction %q; every change of the transaction must carry %q", i, got.id.TxID, first[0].id.TxID)
		}
	}
	if warmID.IsZero() || warmID.TxID == first[0].id.TxID {
		t.Errorf("the warm-up transaction's identity %+v must be present and differ from T's %q", warmID, first[0].id.TxID)
	}
	want, exact := wantTxID(t, before, after)
	if exact && first[0].id.TxID != want {
		t.Errorf("transaction identity %q; the server names this transaction %q", first[0].id.TxID, want)
	}
	if !exact && !strings.HasPrefix(first[0].id.TxID, want) {
		t.Errorf("transaction identity %q does not start with the server-derived %q", first[0].id.TxID, want)
	}
	if fmt.Sprint(second) != fmt.Sprint(first) {
		t.Errorf("a re-delivery of the same transaction numbered it differently — an apply mark would skip the wrong change:\n  first  %+v\n  second %+v", first, second)
	}
}

func queryString(t *testing.T, dsn, q string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var s string
	if err := db.QueryRow(q).Scan(&s); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return s
}

func TestCDCReader_ApplyIdentity_GTID_StableAcrossRedelivery(t *testing.T) {
	dsn, cleanup := startMySQLGTIDForCDC(t)
	defer cleanup()
	assertApplyIdentityStableAcrossRedelivery(t, Engine{Flavor: FlavorVanilla}, dsn,
		func(t *testing.T) string { return gtidExecuted(t, dsn) },
		func(t *testing.T, before, after string) (string, bool) {
			return queryString(t, dsn, fmt.Sprintf("SELECT GTID_SUBTRACT('%s', '%s')", after, before)), true
		})
}

func TestCDCReader_ApplyIdentity_FilePos_StableAcrossRedelivery(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()
	assertApplyIdentityStableAcrossRedelivery(t, Engine{Flavor: FlavorVanilla}, dsn,
		func(*testing.T) string { return "" },
		func(t *testing.T, _, _ string) (string, bool) {
			return "filepos:" + queryString(t, dsn, "SELECT @@server_uuid") + ":", false
		})
}

func TestCDCReader_ApplyIdentity_MariaDB_StableAcrossRedelivery(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, "mariadb:11.4")
	defer cleanup()
	assertApplyIdentityStableAcrossRedelivery(t, Engine{Flavor: FlavorMariaDB}, dsn,
		func(t *testing.T) string { return queryString(t, dsn, "SELECT @@gtid_binlog_pos") },
		func(_ *testing.T, _, after string) (string, bool) { return after, true })
}
