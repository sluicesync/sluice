//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The item-132 pin against a REAL gtid_mode=ON MySQL, with the SERVER as the
// independent oracle.
//
// The unit matrix (cdc_reader_gtid_staging_test.go) drives synthetic events
// and asks go-mysql's set algebra whether a position covers a GTID the TEST
// chose. This one drives a real multi-row transaction through a real binlog
// and asks the SOURCE — via its own GTID_SUBSET function, against the GTID the
// SERVER assigned — the same question. Neither the expected value nor the
// predicate comes from sluice: the transaction's GTID is the delta between
// @@global.gtid_executed before and after the transaction, and the containment
// test is the server's.
//
// What it proves that the unit test cannot: that a REAL binlog's GTID_EVENT →
// BEGIN → ROWS → XID ordering, decoded by the real go-mysql parser, produces
// row positions excluding their own transaction. The unit test constructs the
// event order it expects; this one observes it.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// gtidExecuted reads @@global.gtid_executed — the independent before/after
// measurement the expected value is derived from.
func gtidExecuted(t *testing.T, dsn string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var set string
	if err := db.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&set); err != nil {
		t.Fatalf("SELECT @@global.gtid_executed: %v", err)
	}
	return set
}

// serverGTIDSubset asks the SOURCE whether sub is a subset of super. This is
// the independent predicate: sluice's own set handling takes no part in it.
func serverGTIDSubset(t *testing.T, dsn, sub, super string) bool {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var isSubset int
	if err := db.QueryRowContext(ctx, "SELECT GTID_SUBSET(?, ?)", sub, super).Scan(&isSubset); err != nil {
		t.Fatalf("SELECT GTID_SUBSET(%q, %q): %v", sub, super, err)
	}
	return isSubset == 1
}

// drainChangesWithBoundaries is drainChanges without its boundary filter: this
// test's whole subject is the RELATIONSHIP between the row positions and the
// TxCommit position, so the shared helper's "TxBegin/TxCommit are applier-
// internal, skip them" convention would drop half the evidence.
func drainChangesWithBoundaries(
	t *testing.T,
	ctx context.Context,
	changes <-chan ir.Change,
	want int,
	timeout time.Duration,
) []ir.Change {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	got := make([]ir.Change, 0, want)
	for len(got) < want {
		select {
		case c, ok := <-changes:
			if !ok {
				return got
			}
			if _, isSnap := c.(ir.SchemaSnapshot); isSnap {
				// ADR-0049 schema-history boundary — orthogonal infra.
				continue
			}
			got = append(got, c)
		case <-deadline.C:
			t.Logf("timed out after %v with %d/%d changes", timeout, len(got), want)
			return got
		case <-ctx.Done():
			return got
		}
	}
	return got
}

// mustDecodeGTIDPos decodes a position and asserts it is a GTID-mode one
// carrying a non-empty set — the shared "is the GTID branch even being
// exercised" guard for the item-132 resume pins.
func mustDecodeGTIDPos(t *testing.T, pos ir.Position, what string) binlogPos {
	t.Helper()
	decoded, ok, err := decodeBinlogPos(pos)
	if err != nil || !ok {
		t.Fatalf("decode %s position: ok=%v err=%v", what, ok, err)
	}
	if decoded.Mode != positionModeGTID {
		t.Fatalf("%s position mode = %q; want %q", what, decoded.Mode, positionModeGTID)
	}
	if decoded.GTIDSet == "" {
		t.Fatalf("%s position has an empty gtid_set", what)
	}
	return decoded
}

// TestCDCReader_GTIDStaging_RowPositionExcludesOwnTransaction is the item-132
// real-server pin.
//
//  1. Boot gtid_mode=ON MySQL, seed a table, open the reader at "current".
//  2. Record @@global.gtid_executed (BEFORE).
//  3. Run ONE explicit transaction inserting three rows.
//  4. Record @@global.gtid_executed (AFTER). AFTER ⊇ BEFORE ∪ {this tx}.
//  5. Drain the emitted changes and assert, using the server's GTID_SUBSET:
//     - every ROW position ⊆ BEFORE  → it excludes its own transaction;
//     - AFTER ⊆ the TxCommit position → the commit includes it.
//
// The honest-test guard is step 4's premise check: if BEFORE and AFTER are
// equal the transaction produced no GTID and the whole assertion would pass
// vacuously.
func TestCDCReader_GTIDStaging_RowPositionExcludesOwnTransaction(t *testing.T) {
	dsn, cleanup := startMySQLGTIDForCDC(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE ledger (
			id     BIGINT       NOT NULL AUTO_INCREMENT,
			memo   VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	applyMySQL(t, dsn, seedDDL)

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

	before := gtidExecuted(t, dsn)
	applyMySQL(t, dsn, `
		START TRANSACTION;
		INSERT INTO ledger (memo) VALUES ('a');
		INSERT INTO ledger (memo) VALUES ('b');
		INSERT INTO ledger (memo) VALUES ('c');
		COMMIT;
	`)
	after := gtidExecuted(t, dsn)

	// Honest-test guard: the transaction must actually have produced a new
	// GTID, or "the row position excludes its own transaction" is vacuous.
	if before == after {
		t.Fatalf("test premise broken: @@global.gtid_executed unchanged across the transaction (%q) — "+
			"the source assigned no GTID, so this test would pass for the wrong reason", before)
	}
	t.Logf("gtid_executed before = %q", before)
	t.Logf("gtid_executed after  = %q", after)

	// TxBegin + 3 Inserts + TxCommit.
	got := drainChangesWithBoundaries(t, ctx, changes, 5, 60*time.Second)
	if len(got) != 5 {
		t.Fatalf("drained %d changes, want 5 (TxBegin, 3 Inserts, TxCommit): %#v", len(got), got)
	}
	if _, ok := got[4].(ir.TxCommit); !ok {
		t.Fatalf("last change is %T, want ir.TxCommit", got[4])
	}

	setFor := func(c ir.Change) string {
		decoded, ok, derr := decodeBinlogPos(c.Pos())
		if derr != nil || !ok {
			t.Fatalf("decode position of %T: ok=%v err=%v", c, ok, derr)
		}
		if decoded.Mode != positionModeGTID {
			t.Fatalf("position of %T is mode %q, want %q — the GTID branch is not being exercised",
				c, decoded.Mode, positionModeGTID)
		}
		return decoded.GTIDSet
	}

	// Rows (and the TxBegin) must resume from BEFORE the transaction.
	for i, c := range got[:4] {
		set := setFor(c)
		if !serverGTIDSubset(t, dsn, set, before) {
			t.Errorf("change %d (%T) carries GTID set %q, which is NOT a subset of the pre-transaction "+
				"gtid_executed %q — it contains its own in-flight transaction, and StartSyncGTID with "+
				"such a set SKIPS that transaction entirely (item 132)", i, c, set, before)
		}
	}

	// The commit must resume from AFTER it.
	commitSet := setFor(got[4])
	if !serverGTIDSubset(t, dsn, after, commitSet) {
		t.Errorf("TxCommit carries GTID set %q, which does not cover the post-transaction gtid_executed "+
			"%q — a resume from the commit boundary would re-read a transaction already durably applied "+
			"(item 132: the fold must happen BEFORE the commit position is computed)", commitSet, after)
	}
}
